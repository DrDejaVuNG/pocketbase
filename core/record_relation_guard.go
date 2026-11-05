package core

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pocketbase/dbx"
	validation "github.com/pocketbase/ozzo-validation/v4"
	"github.com/pocketbase/pocketbase/tools/list"
)

// PostgreSQL only:
//
// The relation field validation checks that the related records exist before
// the db write, so a concurrent delete of a related record (eg. a cascade
// delete on another node) could commit in between and the write would store
// a reference to a missing record (there are no foreign keys).
//
// guardRelationValues closes that window for validated record writes: every
// new or changed relation value is written through a subquery that returns the
// value only when all related records exist and locks them FOR KEY SHARE until
// the write commits (the same lock as a foreign key check). A concurrent delete
// then either waits for the write (and its cascade finds the new reference) or
// it commits first and the subquery returns NULL, which violates the NOT NULL
// relation column and is reported as a relation validation error (see
// relationGuardError).

const pgCodeNotNullViolation = "23502"

// guardRelationValues replaces the new or changed non-empty relation values
// of the record db export data with guarded subquery expressions.
func guardRelationValues(app App, model Model, data map[string]any) {
	record := modelRecord(model)
	if record == nil {
		return
	}

	for _, field := range record.Collection().Fields {
		relField, ok := field.(*RelationField)
		if !ok {
			continue
		}

		value, ok := data[relField.Name]
		if !ok {
			continue // not part of the write
		}

		ids := relationIds(relField, record)
		if len(ids) == 0 {
			continue
		}

		// unchanged relations are already visible to a concurrent cascade delete
		if !record.IsNew() && slices.Equal(ids, relationIds(relField, record.Original())) {
			continue
		}

		relCollection, err := app.FindCachedCollectionByNameOrId(relField.CollectionId)
		if err != nil || relCollection.IsView() {
			continue
		}

		data[relField.Name] = relationGuardExpr(relField, relCollection.Name, value, ids)
	}
}

// relationIds returns the related record ids that the relation field db value holds.
func relationIds(field *RelationField, record *Record) []string {
	ids := list.ToUniqueStringSlice(record.GetRaw(field.Name))
	if !field.IsMultiple() && len(ids) > 1 {
		ids = ids[len(ids)-1:] // the last selected (see RelationField.DriverValue)
	}
	return ids
}

func relationGuardExpr(field *RelationField, relTable string, value any, ids []string) dbx.Expression {
	params := make(dbx.Params, len(ids)+1)

	valueParam := "__pbrg_" + field.Name
	params[valueParam] = value

	placeholders := make([]string, len(ids))
	for i, id := range ids {
		name := fmt.Sprintf("%s_%d", valueParam, i)
		params[name] = id
		placeholders[i] = "{:" + name + "}"
	}

	valueType := "text"
	if field.IsMultiple() {
		valueType = "jsonb"
	}

	return dbx.NewExp(fmt.Sprintf(
		"(SELECT {:%s}::%s WHERE (SELECT COUNT(*) FROM (SELECT 1 FROM {{%s}} WHERE [[id]] IN (%s) FOR KEY SHARE) {{__pbrg}}) = %d)",
		valueParam,
		valueType,
		relTable,
		strings.Join(placeholders, ", "),
		len(ids),
	), params)
}

// relationGuardError converts the NOT NULL violation of a guarded relation
// column into the relation field validation error.
func relationGuardError(model Model, err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != pgCodeNotNullViolation {
		return err
	}

	record := modelRecord(model)
	if record == nil {
		return err
	}

	field, ok := record.Collection().Fields.GetByName(pgErr.ColumnName).(*RelationField)
	if !ok {
		return err
	}

	return validation.Errors{
		field.Name: validation.NewError("validation_missing_rel_records", "Failed to find all relation records with the provided ids"),
	}
}

func modelRecord(model Model) *Record {
	switch m := model.(type) {
	case *Record:
		return m
	case RecordProxy:
		return m.ProxyRecord()
	default:
		return nil
	}
}
