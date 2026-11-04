package migrations

import (
	"github.com/pocketbase/pocketbase/core"
)

// PostgreSQL: autodate columns used to be created as "TIMESTAMP DEFAULT NOW() NOT NULL",
// which makes every record create fail for autodate fields with onCreate=false
// (the zero DateTime is stored as explicit NULL).
//
// Drop the NOT NULL constraint of the existing autodate columns so that
// they match the current [core.AutodateField.ColumnType] definition.
func init() {
	core.SystemMigrations.Register(func(txApp core.App) error {
		collections, err := txApp.FindAllCollections()
		if err != nil {
			return err
		}

		for _, collection := range collections {
			if collection.IsView() {
				continue
			}

			for _, field := range collection.Fields {
				if field.Type() != core.FieldTypeAutodate {
					continue
				}

				_, err := txApp.DB().NewQuery(
					"ALTER TABLE {{" + collection.Name + "}} ALTER COLUMN [[" + field.GetName() + "]] DROP NOT NULL",
				).Execute()
				if err != nil {
					return err
				}
			}
		}

		return nil
	}, nil)
}
