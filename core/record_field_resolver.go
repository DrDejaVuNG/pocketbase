package core

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"ganigeorgiev.com/pocketbase/dbx"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/inflector"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/search"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/security"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/types"
	"github.com/spf13/cast"
)

// filter modifiers
const (
	eachModifier    string = "each"
	issetModifier   string = "isset"
	lengthModifier  string = "length"
	lowerModifier   string = "lower"
	changedModifier string = "changed"
)

type ruleJoin struct {
	collection *Collection
	tableAlias string
}

// ensure that `search.FieldResolver` interface is implemented
var _ search.FieldResolver = (*RecordFieldResolver)(nil)

// RecordFieldResolver defines a custom search resolver struct for
// managing Record model search fields.
//
// Usually used together with `search.Provider`.
// Example:
//
//	resolver := resolvers.NewRecordFieldResolver(
//	    app,
//	    myCollection,
//	    &models.RequestInfo{...},
//	    true,
//	)
//	provider := search.NewProvider(resolver)
//	...
type RecordFieldResolver struct {
	app               App
	baseCollection    *Collection
	requestInfo       *RequestInfo
	staticRequestInfo map[string]any
	allowedFields     []string
	joins             []*search.Join
	toOneJoins        map[string]struct{} // PostgreSQL only: aliases of the joins that match at most one row
	allowHiddenFields bool
	// ---
	listRuleJoins       []ruleJoin
	joinAliasSuffix     string // used for uniqueness in the flatten collection list rule join
	baseCollectionAlias string
}

// AllowedFields returns a copy of the resolver's allowed fields.
func (r *RecordFieldResolver) AllowedFields() []string {
	return slices.Clone(r.allowedFields)
}

// SetAllowedFields replaces the resolver's allowed fields with the new ones.
func (r *RecordFieldResolver) SetAllowedFields(newAllowedFields []string) {
	r.allowedFields = slices.Clone(newAllowedFields)
}

// AllowHiddenFields returns whether the current resolver allows filtering hidden fields.
func (r *RecordFieldResolver) AllowHiddenFields() bool {
	return r.allowHiddenFields
}

// SetAllowHiddenFields enables or disables hidden fields filtering.
func (r *RecordFieldResolver) SetAllowHiddenFields(allowHiddenFields bool) {
	r.allowHiddenFields = allowHiddenFields
}

// NewRecordFieldResolver creates and initializes a new `RecordFieldResolver`.
func NewRecordFieldResolver(
	app App,
	baseCollection *Collection,
	requestInfo *RequestInfo,
	allowHiddenFields bool,
) *RecordFieldResolver {
	r := &RecordFieldResolver{
		app:               app,
		baseCollection:    baseCollection,
		requestInfo:       requestInfo,
		allowHiddenFields: allowHiddenFields, // note: it is not based only on the requestInfo.auth since it could be used by a non-request internal method
		joins:             []*search.Join{},
		allowedFields: []string{
			`^\w+[\w\.\:]*$`,
			`^\@request\.context$`,
			`^\@request\.method$`,
			`^\@request\.auth\.[\w\.\:]*\w+$`,
			`^\@request\.body\.[\w\.\:]*\w+$`,
			`^\@request\.query\.[\w\.\:]*\w+$`,
			`^\@request\.headers\.[\w\.\:]*\w+$`,
			`^\@collection\.\w+(\:\w+)?\.[\w\.\:]*\w+$`,
		},
	}

	r.staticRequestInfo = map[string]any{}
	if r.requestInfo != nil {
		r.staticRequestInfo["context"] = r.requestInfo.Context
		r.staticRequestInfo["method"] = r.requestInfo.Method
		r.staticRequestInfo["query"] = r.requestInfo.Query
		r.staticRequestInfo["headers"] = r.requestInfo.Headers
		r.staticRequestInfo["body"] = r.requestInfo.Body
		r.staticRequestInfo["auth"] = nil
		if r.requestInfo.Auth != nil {
			authClone := r.requestInfo.Auth.Clone()
			r.staticRequestInfo["auth"] = authClone.
				Unhide(authClone.Collection().Fields.FieldNames()...).
				IgnoreEmailVisibility(true).
				PublicExport()
		}
	}

	return r
}

// @todo think of a better a way how to call it automatically after BuildExpr
//
// UpdateQuery implements `search.FieldResolver` interface.
//
// Conditionally updates the provided search query based on the
// resolved fields (eg. dynamically joining relations).
func (r *RecordFieldResolver) UpdateQuery(query *dbx.SelectQuery) error {
	// PostgreSQL:
	// the deduplication constraint is applied once at the end because
	// it needs to know all join aliases (see updateQueryWithDeduplicateConstraint)
	joinAliases := make([]string, 0, len(r.joins))

	// PostgreSQL: whether some of the joins could match more than one row
	// (aka. could duplicate the main query rows)
	multiRowJoins := false

	for _, join := range r.joins {
		query.LeftJoin(
			(join.TableName + " " + join.TableAlias),
			join.On,
		)
		joinAliases = append(joinAliases, join.TableAlias)
		if _, ok := r.toOneJoins[join.TableAlias]; !ok {
			multiRowJoins = true
		}
	}

	// note: for now the joins are not applied for multi-match conditions to avoid excessive checks
	for _, join := range r.listRuleJoins {
		ruleJoinAliases, ruleMultiRowJoins, err := r.updateQueryWithCollectionListRule(join.collection, join.tableAlias, query)
		if err != nil {
			return err
		}
		joinAliases = append(joinAliases, ruleJoinAliases...)
		multiRowJoins = multiRowJoins || ruleMultiRowJoins
	}

	if multiRowJoins {
		r.updateQueryWithDeduplicateConstraint(query, joinAliases)
	} else if len(joinAliases) > 0 {
		// PostgreSQL:
		// the to-one joins can't duplicate the main query rows so the GROUP BY
		// deduplication is skipped (it prevents PostgreSQL from stopping early
		// with an ORDER BY index and LIMIT), only the joined columns are excluded
		r.updateQueryWithBaseSelect(query)
	}

	return nil
}

// updateQueryWithBaseSelect excludes the joined tables columns
// from a "SELECT *" query (aka. selects only the main table columns).
func (r *RecordFieldResolver) updateQueryWithBaseSelect(query *dbx.SelectQuery) {
	baseAlias := r.baseCollection.Name
	if r.baseCollectionAlias != "" {
		baseAlias = r.baseCollectionAlias
	}

	if selectAllType(query.Info().Selects, baseAlias) == selectAllJoined {
		query.Select("{{" + baseAlias + "}}.*")
	}
}

// updateQueryWithCollectionListRule applies the collection ListRule constraint
// to the query and returns the aliases of the joins added by the rule (if any)
// and whether some of them could match more than one row.
func (r *RecordFieldResolver) updateQueryWithCollectionListRule(c *Collection, tableAlias string, query *dbx.SelectQuery) ([]string, bool, error) {
	if r.allowHiddenFields || c == nil || c.ListRule == nil || *c.ListRule == "" {
		return nil, false, nil
	}

	cloneR := *r
	cloneR.joins = []*search.Join{}
	cloneR.toOneJoins = nil
	cloneR.baseCollection = c
	cloneR.baseCollectionAlias = tableAlias
	cloneR.allowHiddenFields = true
	cloneR.joinAliasSuffix = security.PseudorandomString(8)

	// The extra "id='' || (\nRULE\n)" concatenated part on its own
	// doesn't make much sense because all records are required to have an id,
	// but it is necessary to properly resolve client-side filters when
	// referencing missing relations (the "\n" is for leading and trailing comments).
	//
	// Consider the client-side filter: "a.name != '' || b.name != ''",
	// where both "a" and "b" ref collections have non-empty ListRule.
	// Without the empty check the query will always evaluate to FALSE
	// when one of the "a" or "b" relation fields are empty,
	// even if for example "a.name != ''" is true.
	expr, err := search.FilterData("id='' || (\n" + *c.ListRule + "\n)").BuildExpr(&cloneR)
	if err != nil {
		return nil, false, fmt.Errorf("failed to build %q ListRule join subquery filter expression: %w", c.Name, err)
	}

	// Bind the extra rule expression at the top query level for performance and security reasons
	// (it is more strict and minimizes the risk of data disclosure from a side-channel attack).
	//
	// @todo Investigate with the refactoring if there is a way to group it
	// together with the client-side constraint that invoked it and benchmark
	// it with many (tag.name="1"||...) like statements to evaluate the impact of applying the check many times.
	// If not feasible - document it as caveat and maybe add --dev log.
	query.AndWhere(expr)

	joinAliases := make([]string, 0, len(cloneR.joins))
	multiRowJoins := false
	for _, j := range cloneR.joins {
		query.LeftJoin(
			(j.TableName + " " + j.TableAlias),
			j.On,
		)
		joinAliases = append(joinAliases, j.TableAlias)
		if _, ok := cloneR.toOneJoins[j.TableAlias]; !ok {
			multiRowJoins = true
		}
	}

	return joinAliases, multiRowJoins, nil
}

func (r *RecordFieldResolver) updateQueryWithDeduplicateConstraint(query *dbx.SelectQuery, joinAliases []string) {
	/* SQLite:
	query.Distinct(true)

	// @todo Research better options for generic rows deduplication.
	//
	// Disable the GROUP BY conditional checks for now since it prevents
	// proper utilization of ORDER BY indexes (and maybe others)
	// (https://ganigeorgiev.com/pocketbase/discussions/7461)

	// info := query.Info()
	// if info.Distinct {
	// 	return
	// }

	// // already has the group by registered
	// var groupByCol = r.baseCollection.Name
	// if r.baseCollectionAlias != "" {
	// 	groupByCol = r.baseCollectionAlias
	// }
	// groupByCol += ".id"
	// if len(info.GroupBy) > 0 && info.GroupBy[0] == groupByCol {
	// 	return
	// }

	// // when deemed safe (GROUP BY could have different execution order compared to DISTINCT),
	// // prefer GROUP BY to deduplicate only on the id field instead of all columns
	// // so that the size of a single row wouldn't matter that much
	// if preferGroupBy(info, groupByCol) {
	// 	query.GroupBy(groupByCol)
	// } else {
	// 	query.Distinct(true)
	// }
	*/

	// PostgreSQL:
	// "SELECT DISTINCT base.*" can't be used because PostgreSQL requires the ORDER BY
	// expressions to appear in the select list (eg. "sort=author.name", "sort=@random")
	// and some column types don't have an equality operator (eg. json columns in views).
	//
	// Instead the rows are deduplicated by grouping them by the base collection id:
	// - for tables the other base columns are functionally dependent on the "id" primary key
	// - for views (no primary key) the base row is selected with any_value()
	// - the ORDER BY expressions that are not functionally dependent on the
	//   base "id" are aggregated (aka. the min/max value for ASC/DESC)
	info := query.Info()

	// custom grouping
	if len(info.GroupBy) > 0 {
		query.Distinct(true)
		return
	}

	baseAlias := r.baseCollection.Name
	if r.baseCollectionAlias != "" {
		baseAlias = r.baseCollectionAlias
	}

	isView := r.baseCollection.IsView()

	query.GroupBy(baseAlias + ".id")

	if selectAll := selectAllType(info.Selects, baseAlias); selectAll != selectAllNone {
		if isView {
			query.Select("(any_value({{" + baseAlias + "}})).*")
		} else if selectAll == selectAllJoined {
			// exclude the joined tables columns
			query.Select("{{" + baseAlias + "}}.*")
		}
	}

	if len(info.OrderBy) > 0 {
		orderBy := make([]string, 0, len(info.OrderBy))
		for _, col := range info.OrderBy {
			for _, term := range splitTopLevelCommas(col) {
				orderBy = append(orderBy, groupedOrderByTerm(term, isView, joinAliases))
			}
		}
		query.OrderBy(orderBy...)
	}
}

const (
	selectAllNone   = 0 // specific columns/expressions
	selectAllBase   = 1 // "base.*"
	selectAllJoined = 2 // "*" (or no explicit select), aka. including the joined tables columns
)

// selectAllType reports whether the select columns are all columns
// of the base table (see the selectAll* constants).
func selectAllType(selects []string, baseAlias string) int {
	if len(selects) == 0 {
		return selectAllJoined
	}

	if len(selects) != 1 {
		return selectAllNone
	}

	switch identifierQuotesReplacer.Replace(strings.TrimSpace(selects[0])) {
	case "*":
		return selectAllJoined
	case baseAlias + ".*":
		return selectAllBase
	default:
		return selectAllNone
	}
}

var identifierQuotesReplacer = strings.NewReplacer(`"`, "", "`", "", "[[", "", "]]", "", "{{", "", "}}", "")

var orderByDirectionRegex = regexp.MustCompile(`(?i)\s+(ASC|DESC)$`)

// groupedOrderByTerm returns the provided single ORDER BY term
// in a form compatible with the "GROUP BY base.id" deduplication.
func groupedOrderByTerm(term string, isView bool, joinAliases []string) string {
	expr := strings.TrimSpace(term)
	dir := "ASC"
	if match := orderByDirectionRegex.FindStringSubmatch(expr); len(match) > 0 {
		expr = strings.TrimSpace(expr[:len(expr)-len(match[0])])
		dir = strings.ToUpper(match[1])
	}

	// non-column expression or functionally dependent on the base table primary key
	if strings.EqualFold(expr, "RANDOM()") || (!isView && !referencesAnyAlias(expr, joinAliases)) {
		return strings.TrimSpace(term)
	}

	// aggregate the value of the duplicated rows (NULLS LAST is to pick the min/max non-null value)
	return fmt.Sprintf("(array_agg(%s ORDER BY %s %s NULLS LAST))[1] %s", expr, expr, dir, dir)
}

// referencesAnyAlias reports whether the SQL expression references
// a column of any of the provided table aliases (eg. "[[alias.column]]").
func referencesAnyAlias(expr string, aliases []string) bool {
	for _, alias := range aliases {
		if alias == "" {
			continue
		}

		for offset := 0; offset < len(expr); {
			idx := strings.Index(expr[offset:], alias)
			if idx < 0 {
				break
			}

			start := offset + idx
			end := start + len(alias)
			offset = end

			if start > 0 && isIdentifierChar(expr[start-1]) {
				continue
			}

			rest := expr[end:]
			if strings.HasPrefix(rest, ".") || strings.HasPrefix(rest, `".`) || strings.HasPrefix(rest, "`.") {
				return true
			}
		}
	}

	return false
}

func isIdentifierChar(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// splitTopLevelCommas splits the provided ORDER BY clause part
// by its top-level commas (aka. not inside parenthesis or quotes).
func splitTopLevelCommas(str string) []string {
	var parts []string
	var depth int
	var quote byte
	var start int

	for i := range len(str) {
		c := str[i]

		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			parts = append(parts, str[start:i])
			start = i + 1
		}
	}

	return append(parts, str[start:])
}

//nolint:unused
func preferGroupBy(info *dbx.QueryInfo, fullUnquotedGroupByCol string) bool {
	if len(info.GroupBy) != 0 {
		return false
	}

	if info.Having != nil {
		return false
	}

	// dbx fallbacks to * if not set
	if len(info.Selects) == 0 {
		return true
	}

	if len(info.Selects) != 1 {
		return false
	}

	identifier := info.Selects[0]

	if identifier == "*" || identifier == fullUnquotedGroupByCol {
		return true
	}

	// try again as direct col match in an unquoted column format
	identifier = inflector.Columnify(identifier)
	if identifier == fullUnquotedGroupByCol {
		return true
	}

	// remains table.* to check
	// (aliased columns for now are ignored as they could be represented by expressions)
	if !strings.HasSuffix(identifier, ".*") {
		return false
	}

	return strings.HasPrefix(fullUnquotedGroupByCol, strings.TrimSuffix(identifier, "*"))
}

// Resolve implements `search.FieldResolver` interface.
//
// Example of some resolvable fieldName formats:
//
//	id
//	someSelect.each
//	project.screen.status
//	screen.project_via_prototype.name
//	@request.context
//	@request.method
//	@request.query.filter
//	@request.headers.x_token
//	@request.auth.someRelation.name
//	@request.body.someRelation.name
//	@request.body.someField
//	@request.body.someSelect:each
//	@request.body.someField:isset
//	@collection.product.name
func (r *RecordFieldResolver) Resolve(fieldName string) (*search.ResolverResult, error) {
	return parseAndRun(fieldName, r)
}

func (r *RecordFieldResolver) resolveStaticRequestField(path ...string) (*search.ResolverResult, error) {
	if len(path) == 0 {
		return nil, errors.New("at least one path key should be provided")
	}

	lastProp, modifier, err := splitModifier(path[len(path)-1])
	if err != nil {
		return nil, err
	}

	path[len(path)-1] = lastProp

	// extract value
	resultVal, err := extractNestedVal(r.staticRequestInfo, path...)
	if err != nil {
		r.app.Logger().Debug("resolveStaticRequestField graceful fallback", "error", err.Error())
	}

	if modifier == issetModifier {
		if err != nil {
			return &search.ResolverResult{Identifier: "FALSE"}, nil
		}
		return &search.ResolverResult{Identifier: "TRUE"}, nil
	}

	// note: we are ignoring the error because requestInfo is dynamic
	// and some of the lookup keys may not be defined for the request

	switch v := resultVal.(type) {
	case nil:
		// no further processing is needed...
	case string:
		// check if it is a number field and explicitly try to cast to
		// float in case of a numeric string value was used
		// (this usually the case when the data is from a multipart/form-data request)
		field := r.baseCollection.Fields.GetByName(path[len(path)-1])
		if field != nil && field.Type() == FieldTypeNumber {
			if nv, err := strconv.ParseFloat(v, 64); err == nil {
				resultVal = nv
			}
		}
		// otherwise - no further processing is needed...
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		// no further processing is needed...
	default:
		// non-plain value
		// try casting to string (in case for example fmt.Stringer is implemented)
		val, castErr := cast.ToStringE(v)

		// if that doesn't work, try encoding it
		if castErr != nil {
			encoded, jsonErr := json.Marshal(v, json.Deterministic(true))
			if jsonErr == nil {
				val = string(encoded)
			}
		}

		resultVal = val
	}

	// unsupported modifier
	// @todo consider deprecating with the introduction of filter functions
	if modifier != "" && modifier != lowerModifier {
		return nil, fmt.Errorf("invalid modifier sequence %s:%s", lastProp, modifier)
	}

	// no need to wrap as placeholder if we already know that it is null
	if resultVal == nil {
		return &search.ResolverResult{Identifier: "NULL"}, nil
	}

	// PostgreSQL Only:
	// Expanded special empty string filter in advance
	if resultVal == "" && (modifier == "" || modifier == lowerModifier) {
		// note: LOWER('') is still ''
		return &search.ResolverResult{Identifier: `''`}, nil
	}

	placeholder := "f" + security.PseudorandomString(10)

	// @todo consider deprecating with the introduction of filter functions
	if modifier == lowerModifier {
		return &search.ResolverResult{
			Identifier: "LOWER({:" + placeholder + "})",
			/* SQLite:
			Params:     dbx.Params{placeholder: resultVal},
			*/
			// PostgreSQL:
			// `SELECT LOWER(1)` is valid in SQLite, but not in PostgreSQL
			// so we need to explicitly cast the value to text
			Params: dbx.Params{placeholder: cast.ToString(resultVal)},
		}, nil
	}

	/* SQLite:
	return &search.ResolverResult{
		Identifier: "{:" + placeholder + "}",
		Params:     dbx.Params{placeholder: resultVal},
	}, nil
	*/
	// PostgreSQL:
	// Explicitly type the numeric and boolean params because otherwise PostgreSQL
	// infers their type from the other operand and pgx silently truncates
	// the value (eg. "@request.body.amount >= 0" with -0.5 is compared as int4 0)
	// or fails to encode it (eg. a bool compared with an integer).
	identifier := "{:" + placeholder + "}"
	switch resultVal.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		identifier += "::numeric"
	case bool:
		identifier += "::boolean"
	}

	return &search.ResolverResult{
		Identifier: identifier,
		Params:     dbx.Params{placeholder: resultVal},
	}, nil
}

func (r *RecordFieldResolver) loadCollection(collectionNameOrId string) (*Collection, error) {
	if collectionNameOrId == r.baseCollection.Name || collectionNameOrId == r.baseCollection.Id {
		return r.baseCollection, nil
	}

	return getCollectionByModelOrIdentifier(r.app, collectionNameOrId)
}

func (r *RecordFieldResolver) registerJoin(tableName string, tableAlias string, on dbx.Expression) error {
	// PostgreSQL only:
	if on == nil {
		// Note: SQLite allows join without `on` clause but PostgreSQL does not.
		// Join without `on` clause is equivalent to `ON 1=1` clause.
		on = dbx.NewExp("1=1")
	}

	newJoin := &search.Join{
		TableName:  tableName,
		TableAlias: tableAlias,
		On:         on,
	}

	// (see updateQueryWithCollectionListRule)
	if !r.allowHiddenFields {
		c, _ := r.loadCollection(tableName)

		// ignore non-collections since the table name could be an expression (e.g. json) or some other subquery
		if c != nil {
			// treat all fields as if they are hidden
			if c.ListRule == nil {
				return fmt.Errorf("%q fields can be accessed only when allowHiddenFields is enabled or by superusers", c.Name)
			}

			r.registerRuleJoin(c, newJoin.TableAlias)
		}
	}

	// replace existing join
	for i, j := range r.joins {
		if j.TableAlias == newJoin.TableAlias {
			r.joins[i] = newJoin
			return nil
		}
	}

	// register new join
	r.joins = append(r.joins, newJoin)
	return nil
}

// registerToOneJoin registers a join that matches at most one row
// for each row of the joined table (eg. a single relation by its id),
// aka. a join that can't duplicate the main query rows.
func (r *RecordFieldResolver) registerToOneJoin(tableName string, tableAlias string, on dbx.Expression) error {
	if err := r.registerJoin(tableName, tableAlias, on); err != nil {
		return err
	}

	if r.toOneJoins == nil {
		r.toOneJoins = map[string]struct{}{}
	}
	r.toOneJoins[tableAlias] = struct{}{}

	return nil
}

func (r *RecordFieldResolver) registerRuleJoin(collection *Collection, tableAlias string) {
	// replace existing
	for i, j := range r.listRuleJoins {
		if j.tableAlias == tableAlias {
			r.listRuleJoins[i].collection = collection
			return
		}
	}

	// register new
	r.listRuleJoins = append(r.listRuleJoins, ruleJoin{collection, tableAlias})
}

type mapExtractor interface {
	AsMap() map[string]any
}

func extractNestedVal(rawData any, keys ...string) (any, error) {
	if len(keys) == 0 {
		return nil, errors.New("at least one key should be provided")
	}

	switch m := rawData.(type) {
	// maps
	case map[string]any:
		return mapVal(m, keys...)
	case map[string]string:
		return mapVal(m, keys...)
	case map[string]bool:
		return mapVal(m, keys...)
	case map[string]float32:
		return mapVal(m, keys...)
	case map[string]float64:
		return mapVal(m, keys...)
	case map[string]int:
		return mapVal(m, keys...)
	case map[string]int8:
		return mapVal(m, keys...)
	case map[string]int16:
		return mapVal(m, keys...)
	case map[string]int32:
		return mapVal(m, keys...)
	case map[string]int64:
		return mapVal(m, keys...)
	case map[string]uint:
		return mapVal(m, keys...)
	case map[string]uint8:
		return mapVal(m, keys...)
	case map[string]uint16:
		return mapVal(m, keys...)
	case map[string]uint32:
		return mapVal(m, keys...)
	case map[string]uint64:
		return mapVal(m, keys...)
	case mapExtractor:
		return mapVal(m.AsMap(), keys...)
	case types.JSONRaw:
		var raw any
		err := json.Unmarshal(m, &raw)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal raw JSON in order extract nested value from: %w", err)
		}
		return extractNestedVal(raw, keys...)

	// slices
	case []string:
		return arrVal(m, keys...)
	case []bool:
		return arrVal(m, keys...)
	case []float32:
		return arrVal(m, keys...)
	case []float64:
		return arrVal(m, keys...)
	case []int:
		return arrVal(m, keys...)
	case []int8:
		return arrVal(m, keys...)
	case []int16:
		return arrVal(m, keys...)
	case []int32:
		return arrVal(m, keys...)
	case []int64:
		return arrVal(m, keys...)
	case []uint:
		return arrVal(m, keys...)
	case []uint8:
		return arrVal(m, keys...)
	case []uint16:
		return arrVal(m, keys...)
	case []uint32:
		return arrVal(m, keys...)
	case []uint64:
		return arrVal(m, keys...)
	case []mapExtractor:
		extracted := make([]any, len(m))
		for i, v := range m {
			extracted[i] = v.AsMap()
		}
		return arrVal(extracted, keys...)
	case []any:
		return arrVal(m, keys...)
	case []types.JSONRaw:
		return arrVal(m, keys...)
	default:
		return nil, fmt.Errorf("expected map or array, got %#v", rawData)
	}
}

func mapVal[T any](m map[string]T, keys ...string) (any, error) {
	result, ok := m[keys[0]]
	if !ok {
		return nil, fmt.Errorf("invalid key path - missing key %q", keys[0])
	}

	// end key reached
	if len(keys) == 1 {
		return result, nil
	}

	return extractNestedVal(result, keys[1:]...)
}

func arrVal[T any](m []T, keys ...string) (any, error) {
	idx, err := strconv.Atoi(keys[0])
	if err != nil || idx < 0 || idx >= len(m) {
		return nil, fmt.Errorf("invalid key path - invalid or missing array index %q", keys[0])
	}

	result := m[idx]

	// end key reached
	if len(keys) == 1 {
		return result, nil
	}

	return extractNestedVal(result, keys[1:]...)
}

func splitModifier(combined string) (string, string, error) {
	parts := strings.Split(combined, ":")

	if len(parts) != 2 {
		return combined, "", nil
	}

	// validate modifier
	switch parts[1] {
	case issetModifier,
		eachModifier,
		lengthModifier,
		lowerModifier,
		changedModifier:
		return parts[0], parts[1], nil
	}

	return "", "", fmt.Errorf("unknown modifier in %q", combined)
}
