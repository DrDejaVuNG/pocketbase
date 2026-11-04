package migrations

import (
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

// PostgreSQL: older versions created UUID typed primary key columns for
// the "<uuidv7>" id pattern, which breaks the relation joins and filters
// (relation fields store the referenced ids as TEXT and uuid = text doesn't exist).
//
// Convert the existing UUID typed record id columns to TEXT
// (the conversion itself is handled by the records table sync).
func init() {
	core.SystemMigrations.Register(func(txApp core.App) error {
		var uuidTables []string
		err := txApp.DB().NewQuery(`
			SELECT table_name
			FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND column_name = {:columnName}
			  AND data_type = 'uuid'
		`).Bind(dbx.Params{"columnName": core.FieldNameId}).Column(&uuidTables)
		if err != nil {
			return err
		}

		if len(uuidTables) == 0 {
			return nil
		}

		collections, err := txApp.FindAllCollections(core.CollectionTypeBase, core.CollectionTypeAuth)
		if err != nil {
			return err
		}

		for _, collection := range collections {
			for _, table := range uuidTables {
				if collection.Name != table {
					continue
				}

				if err := txApp.SyncRecordTableSchema(collection, collection); err != nil {
					return err
				}
			}
		}

		return nil
	}, nil)
}
