package main

import (
	"database/sql"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	// _ "github.com/mattn/go-oci8" // TODO oci8.pc
)

type PetrobankMigration struct {
	insertor Insertor
	pbk      *sql.DB
}

func NewPetrobankMigration(insertor Insertor) *PetrobankMigration {
	var conn string
	pb_sqlconnect := os.Getenv("PB_SQLCONNECT")
	two_task := os.Getenv("TWO_TASK")
	if pb_sqlconnect != "" && two_task != "" {
		conn = pb_sqlconnect + "@" + two_task
	} else {
		log.Panicln("check environment variable TWO_TASK and PB_SQLCONNECT")
	}
	db, err := sql.Open("oci8", conn)
	if err != nil {
		log.Panicf("can't connect to mds: %s", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Panicf("close db error: %s", err)
		}
	}()
	if err = test_conn(db); err != nil {
		log.Panicf("can't connect: %s: %s", conn, err)
	}
	if err = role_pbk(db); err != nil {
		log.Panicf("can't set role to petrobank: %s", err)
	}

	return &PetrobankMigration{pbk: db, insertor: insertor}

	// rows, err := db.Query(query, *req_no)
	// if err != nil {
	// 	log.Panicf("%s:", err)
	// }
	// cols, err := rows.Columns()
	// if err != nil {
	// 	log.Panicf("%s:", err)
	// }
	// if len(cols) != 1 {
	// 	log.Panicf("should return 1 column but returned: %d", cols)
	// }
	// var row_cnt int
	// var result string
	// for rows.Next() {
	// 	if err := rows.Scan(&result); err != nil {
	// 		log.Panicf("%s:", err)
	// 	}
	// 	row_cnt++
	// }
	// if err = rows.Err(); err != nil {
	// 	log.Panicf("%s:", err)
	// }
	// if err = rows.Close(); err != nil {
	// 	log.Panicf("%s:", err)
	// }
	// if row_cnt != 1 {
	// 	log.Panicf("expecting 1 row returned but got: %d", row_cnt)
	// }
	// fmt.Printf("%s", result)
}

func test_conn(db *sql.DB) (err error) {
	query := "select * from dual"
	_, err = db.Query(query)
	return err
}

func role_pbk(db *sql.DB) (err error) {
	rolex := os.Getenv("PB_ROLEX")
	if len(rolex) != 0 {
		_, err = db.Exec(fmt.Sprintf("set role petrobank identified by %s", rolex))
		if err != nil {
			_, err = db.Exec("set role petrobank")
		}
	} else {
		_, err = db.Exec("set role petrobank")
		if err != nil {
			_, err = db.Exec("set role petrobank identified by us1test")
		}
	}
	return err
}

func (p *PetrobankMigration) migrate() error {
	panic("not implemented")
}
