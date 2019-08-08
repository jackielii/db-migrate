package main

import (
	"database/sql"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"

	"time"

	mssql "github.com/denisenkom/go-mssqldb"
)

type MSInsert struct {
	db  *sql.DB
	tnx *sql.Tx

	tableName      string
	columnMappings []ColumnMapping

	stmt        *sql.Stmt // cache the statement
	sql         string    // cache sql formatter
	columnNames []string

	bulkInsertVals []string
	bulkCount      int64
}

func (m *MSInsert) bulkInsert(values ...interface{}) error {
	if m.columnNames == nil {
		m.columnNames = make([]string, len(m.columnMappings))
		for i, cm := range m.columnMappings {
			m.columnNames[i] = cm.target
		}
	}
	var err error
	if m.stmt == nil {
		options := mssql.MssqlBulkOptions{
			FireTriggers: true,
			// RowsPerBatch: 10000,
		}
		if m.tnx, err = m.db.Begin(); err != nil {
			return err
		}
		m.stmt, err = m.db.Prepare(mssql.CopyIn(m.tableName, options, m.columnNames...))
		if err != nil {
			return err
		}
	}
	m.bulkCount++
	// convert date fields
	for i, v := range values {
		if v == nil {
			continue
		}
		if len(*dateFields) != 0 {
			dateColumns := strings.Split(*dateFields, ",")
			for _, dateColumn := range dateColumns {
				if strings.TrimSpace(dateColumn) == m.columnNames[i] {
					if values[i], err = time.Parse("2006-01-02", v.(string)); err != nil {
						return err
					}
					break
				}
			}
		} else {
			if strings.Contains(m.columnNames[i], "date") {
				if values[i], err = time.Parse("2006-01-02", v.(string)); err != nil {
					return err
				}
			}
		}
	}
	if _, err = m.stmt.Exec(values...); err != nil {
		return err
	}

	if m.bulkCount >= *bulkLimit {
		if _, err = m.stmt.Exec(); err != nil {
			return err
		}
		if err = m.stmt.Close(); err != nil {
			return err
		}
		if err = m.tnx.Commit(); err != nil {
			return err
		}
		m.bulkCount = 0
		m.tnx = nil
		m.stmt = nil
	}

	return nil
}

func (m *MSInsert) bulkCommit() error {
	if _, err := m.stmt.Exec(); err != nil {
		return err
	}
	return nil
}

func (m *MSInsert) insert(values ...interface{}) error {

	if m.columnNames == nil {
		m.columnNames = make([]string, len(m.columnMappings))
		for i, cm := range m.columnMappings {
			m.columnNames[i] = cm.target
		}
	}

	if m.stmt == nil {
		var vh = strings.Repeat("?,", len(m.columnNames))
		vh = vh[:len(vh)-1]
		m.sql = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			m.tableName,
			strings.Join(m.columnNames, ","),
			vh,
		)

		var err error
		m.stmt, err = m.db.Prepare(m.sql)
		if err != nil {
			return err
		}
	}

	log.Debug(repl(m.sql, values...))

	_, err := m.stmt.Exec(values...)
	if err != nil {
		log.Info(repl(m.sql, values...))
		if *exitOnError {
			return err
		}
		log.Errorln(err)
		return nil
	}

	return nil
}

func (m *MSInsert) close() error {
	if m.stmt != nil {
		if m.bulkCount != 0 {
			if err := m.bulkCommit(); err != nil {
				return err
			}
		}
		if err := m.stmt.Close(); err != nil {
			return err
		}
	}
	if m.tnx != nil {
		return m.tnx.Commit()
	}
	return nil
}

func repl(sql string, values ...interface{}) string {
	sql = strings.Replace(sql, "?", "%q", -1)
	sql = fmt.Sprintf(sql, values...)
	sql = strings.Replace(sql, "\"", "'", -1)
	sql = strings.Replace(sql, "%!q(<nil>)", "NULL", -1)
	return sql
}

func val(s string) string {
	if strings.EqualFold(s, "NULL") {
		return "NULL"
	}
	return "'" + s + "'"
}
