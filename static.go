package main

type StaticMigration struct {
	insertor       Insertor
	columnMappings []ColumnMapping
}

func (c *StaticMigration) migrate() error {

	var values []interface{} = make([]interface{}, len(c.columnMappings))

	for i, cm := range c.columnMappings {
		switch v := cm.source.(type) {
		case string:
			if v == "" {
				values[i] = nil
			} else {
				values[i] = v
			}
		default: // any other types treated as static values
			values[i] = v
		}
	}

	if err := c.insertor.insert(values...); err != nil {
		return err
	}
	return nil
}
