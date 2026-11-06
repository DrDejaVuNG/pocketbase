package core

import (
	"strings"
	"sync/atomic"
)

// PostgreSQL only:
// The pgx caching query exec modes (cache_describe, the default, and
// cache_statement) keep statement descriptions per pooled connection and
// assume that the schema doesn't change. After a table change, a statement
// that a connection cached before the change fails once on that connection
// ("bind message has N result formats but query has M columns" or "cached
// plan must not change result type"). The connections are kept consistent by:
//   - clearing the cache of a connection right after it runs a schema change
//     (CREATE/ALTER/DROP) and when such a change is rolled back to a savepoint;
//   - bumping statementCacheGeneration when a schema change ends (commit,
//     rollback or autocommit), when the realtime bridge reports a collection
//     change from another instance, and when a stale statement error is seen;
//     every pooled connection compares its generation before it is reused and
//     clears its cache when it is behind;
//   - running a statement that failed with a stale statement error once more
//     when it ran outside a transaction (the failed statement had no effect and
//     pgx already dropped it from the connection's cache).
//
// A transaction that reads a table changed by another process (eg. psql, or
// another instance without the realtime bridge) can still fail once on each
// connection that cached the statement before the change.
var statementCacheGeneration atomic.Uint64

// ResetStatementCaches makes every pooled PostgreSQL connection of this
// process clear its cached statement descriptions before it is used again.
//
// Schema changes made through this process and collection changes reported
// by the realtime bridge from other instances call it automatically. Call it
// after changing tables from outside PocketBase (eg. psql or another tool)
// so that transactions don't fail on the first stale statement.
func ResetStatementCaches() {
	statementCacheGeneration.Add(1)
}

// isSchemaChange reports whether one of the ";" separated statements of query
// starts with CREATE, ALTER or DROP (after whitespace and SQL comments).
//
// A ";" inside a string literal could split a statement too early; that only
// clears a cache that didn't need it.
func isSchemaChange(query string) bool {
	for stmt := range strings.SplitSeq(query, ";") {
		stmt = skipSpaceAndComments(stmt)
		if hasKeywordPrefix(stmt, "CREATE") || hasKeywordPrefix(stmt, "ALTER") || hasKeywordPrefix(stmt, "DROP") {
			return true
		}
	}
	return false
}

// isRollbackStatement reports whether query starts with ROLLBACK
// (eg. "ROLLBACK TO SAVEPOINT name").
func isRollbackStatement(query string) bool {
	return hasKeywordPrefix(skipSpaceAndComments(query), "ROLLBACK")
}

// skipSpaceAndComments returns s without its leading whitespace,
// "--" line comments and "/* */" block comments.
func skipSpaceAndComments(s string) string {
	for {
		s = strings.TrimLeft(s, " \t\r\n")
		switch {
		case strings.HasPrefix(s, "--"):
			i := strings.IndexByte(s, '\n')
			if i < 0 {
				return ""
			}
			s = s[i+1:]
		case strings.HasPrefix(s, "/*"):
			i := strings.Index(s, "*/")
			if i < 0 {
				return ""
			}
			s = s[i+2:]
		default:
			return s
		}
	}
}

// hasKeywordPrefix reports whether s starts with the keyword
// (case insensitive) followed by a non-identifier character or the end.
func hasKeywordPrefix(s, keyword string) bool {
	if len(s) < len(keyword) || !strings.EqualFold(s[:len(keyword)], keyword) {
		return false
	}
	if len(s) == len(keyword) {
		return true
	}
	c := s[len(keyword)]
	return !(c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z')
}
