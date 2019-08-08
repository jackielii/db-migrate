package main

import (
	"crypto/md5"
	"encoding/csv"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

type CSVMigration struct {
	sourceName     string
	insertor       Insertor
	r              *csv.Reader
	tableName      string
	columnMappings []ColumnMapping

	nameIndex map[string]Int
}

type Int struct {
	exists bool
	value  int
}

func (c *CSVMigration) migrate(bulk bool) error {

	var headerRead bool
	var header []string
	var row int64
	for {
		record, err := c.r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panicln(err)
		}

		if !headerRead {
			header = record
			c.nameIndex = make(map[string]Int)

			for i, v := range header {
				c.nameIndex[v] = Int{exists: true, value: i}
			}
			// spew.Dump(c.nameIndex)
			headerRead = true
			continue
		}
		row++

		if len(record) != len(header) {
			log.Error("wrong width at row", row)
			continue
		}

		var values = make([]interface{}, len(c.columnMappings))

		for i, cm := range c.columnMappings {
			switch v := cm.source.(type) {
			case string:
				if v[:1] == "$" { // column name
					var columnName = v[1:]
					var value string

					if strings.HasPrefix(columnName, "hash(") {
						columnName = columnName[len("hash(") : len(columnName)-1]

						var cs = strings.Split(columnName, ",")

						for _, cc := range cs {
							columnName = strings.TrimSpace(cc)
							var index = c.nameIndex[columnName]
							if !index.exists {
								log.Panicln("unable to find column", columnName)
							}
							value += record[index.value]
						}
						var hash = md5.Sum([]byte(value))
						value = fmt.Sprintf("%x", hash)[:20]
					} else if strings.HasPrefix(columnName, "replace") {
						columnName = columnName[len("replace(") : len(columnName)-1]

						var cs = strings.Split(columnName, ",")
						if len(cs) != 3 {
							log.Panicln("need 3 parameters for replace: column, old, new")
						}

						var old = cs[1]
						var new = cs[2]
						columnName = cs[0]
						columnName = strings.TrimSpace(columnName)
						var index = c.nameIndex[columnName]
						if !index.exists {
							log.Panicln("unable to find column", columnName)
						}
						value = record[index.value]
						re := regexp.MustCompile(old)
						value = re.ReplaceAllString(value, new)
					} else if strings.HasPrefix(columnName, "concat") {
						columnName = columnName[len("concat(") : len(columnName)-1]

						var cs = strings.Split(columnName, ",")
						for _, cc := range cs {
							columnName = strings.TrimSpace(cc)
							if strings.HasPrefix(columnName, "'") && strings.HasSuffix(columnName, "'") {
								value += columnName[1 : len(columnName)-1]
							} else {
								var index = c.nameIndex[columnName]
								if !index.exists {
									log.Panicln("unable to find column", columnName)
								}
								value += record[index.value]
							}
						}
					} else {
						var index = c.nameIndex[columnName]
						if !index.exists {
							log.Panicln("unable to find column", columnName)
						}
						value = record[index.value]

						if *max20 {
							if (columnName == "RECALL_LOG_ID" || columnName == "RECALL_TRACE_ID") && len(value) > 20 {
								value = value[len(value)-20:]
							}
						}
					}

					value = strings.TrimSpace(value)

					if value == "" {
						values[i] = nil
					} else {
						values[i] = value

						// sql server doesn't accept scientific number as string
						if strings.Contains(value, "E") {
							if v, err := strconv.ParseFloat(value, 64); err == nil {
								values[i] = v
							}
						}
					}
				} else { // static string
					values[i] = v
				}
			default: // any other types treated as static values
				values[i] = v
			}
		}

		if bulk {
			bi, ok := c.insertor.(BulkInsertor)
			if !ok {
				log.Fatalln("bulk insert is not possible")
			}
			if err := bi.bulkInsert(values...); err != nil {
				log.Infof("File %s, At row %d", c.sourceName, row)
				return err
			}
			// if row == 4 {
			// 	break
			// }
		} else if err := c.insertor.insert(values...); err != nil {
			log.Infof("File %s, At row %d", c.sourceName, row)
			return err
		}
	}
	return nil
}

var _ Migrator = &CSVMigration{}
