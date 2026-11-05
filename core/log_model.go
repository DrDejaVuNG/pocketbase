package core

import (
	"bytes"
	"encoding/json/v2"
	"strings"
	"unicode/utf8"

	"ganigeorgiev.com/pocketbase/pocketbase/tools/types"
)

var (
	_ Model      = (*Log)(nil)
	_ DBExporter = (*Log)(nil)
)

const LogsTableName = "_logs"

const (
	defaultMaxLogDataSize    = 16 << 10 // ~16kb
	defaultMaxLogMessageSize = 8000
)

type Log struct {
	BaseModel

	Created types.DateTime     `db:"created" json:"created"`
	Data    types.JSONMap[any] `db:"data" json:"data"`
	Message string             `db:"message" json:"message"`
	Level   int                `db:"level" json:"level"`
}

func (l *Log) TableName() string {
	return LogsTableName
}

// DBExport prepares and exports the current log model for db persistence.
//
// It also truncates the log's message and data to ensure that it is
// under app.Settings().Logs.MaxDataSize.
func (l *Log) DBExport(app App) (map[string]any, error) {
	result := map[string]any{
		"id":      l.Id,
		"created": l.Created,
		"level":   l.Level,
	}

	/* SQLite:
	// truncate the raw message bytes
	// (this is expected to be very rare so it is ok even if multi-byte chars)
	if int64(len(l.Message)) > defaultMaxLogMessageSize {
		result["message"] = l.Message[:defaultMaxLogMessageSize]
	} else {
		result["message"] = l.Message
	}
	*/
	// PostgreSQL: TEXT rejects NUL bytes and invalid UTF-8 (SQLSTATE 22021),
	// so the message (e.g. an url-unescaped request uri like "/x?a=%00")
	// is sanitized and truncated on a rune boundary.
	result["message"] = sanitizeLogText(l.Message, defaultMaxLogMessageSize)

	// @todo once added in the standard library consider replacing with
	// WithByteLimit and WithDepthLimit as suggested in https://github.com/golang/go/issues/56733
	if len(l.Data) == 0 {
		result["data"] = l.Data
	} else {
		maxDataSize := app.Settings().Logs.MaxDataSize
		if maxDataSize == 0 {
			maxDataSize = defaultMaxLogDataSize
		}

		rawData, err := l.Data.MarshalJSON()
		if err != nil {
			return nil, err
		}

		if int64(len(rawData)) > maxDataSize {
			truncatedData := types.JSONMap[any]{}

			// ignore syntax errors in case of truncated incomplete json
			//
			// jsonv2 stream decodes and all "valid" attrs read up to the
			// invalid part will be populated in truncatedData
			_ = json.Unmarshal(rawData[:maxDataSize], &truncatedData)

			truncatedData["__pb_truncated__"] = true

			rawData, err = truncatedData.MarshalJSON()
			if err != nil {
				return nil, err
			}
		}

		// PostgreSQL: JSONB rejects the \u0000 escape (SQLSTATE 22P05)
		// (invalid UTF-8 is already replaced with U+FFFD by the JSONMap marshaler)
		result["data"] = types.JSONRaw(stripJSONNulEscapes(rawData))
	}

	return result, nil
}

// sanitizeLogText returns s as valid UTF-8 without NUL bytes and
// truncated to at most maxBytes without splitting a multi-byte character.
func sanitizeLogText(s string, maxBytes int) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "\uFFFD")
	}

	if strings.IndexByte(s, 0) >= 0 {
		s = strings.ReplaceAll(s, "\x00", "")
	}

	if len(s) > maxBytes {
		cut := maxBytes
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut]
	}

	return s
}

var jsonNulEscape = []byte(`\u0000`)

// stripJSONNulEscapes removes all \u0000 escape sequences from the raw JSON
// while keeping every other escape sequence (incl. an escaped backslash
// followed by a literal "u0000") untouched.
func stripJSONNulEscapes(raw []byte) []byte {
	if !bytes.Contains(raw, jsonNulEscape) {
		return raw
	}

	result := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			result = append(result, raw[i])
			continue
		}

		if bytes.HasPrefix(raw[i:], jsonNulEscape) {
			i += len(jsonNulEscape) - 1
			continue
		}

		// copy the escape sequence pair as it is
		result = append(result, raw[i])
		if i+1 < len(raw) {
			i++
			result = append(result, raw[i])
		}
	}

	return result
}
