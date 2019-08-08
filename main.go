package main

import (
	"archive/zip"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"

	_ "github.com/denisenkom/go-mssqldb"
)

var (
	configFn    = flag.String("config", "./migration.yaml", "migration config")
	debugFlag   = flag.Bool("debug", false, "enable debug")
	debugMssql  = flag.Bool("debug-sql", false, "print mssql database debug")
	exitOnError = flag.Bool("exit-on-error", false, "exit on error")
	max20       = flag.Bool("max20", false, "trim specific columns to 20 chars")
	section     = flag.String("section", "", "specify sections to import")
	extractOnly = flag.Bool("extractOnly", false, "only extract related item in AO")
	migrateOnly = flag.Bool("migrateOnly", true, "only run migration")
	bulkInsert  = flag.Bool("bulk", false, "use bulk insert")
	bulkLimit   = flag.Int64("bulk-limit", 100000, "number of rows to batch bulk insert")
	connTimeout = flag.Int("conn-timeout", 300, "connection time in seconds")
	dateFields  = flag.String("date-columns", "", "bulk: convert date columns seperated by comma, if none specified, any column with word date")
)

type Database struct {
	Username string
	Password string
	Server   string
	Database string
	Port     string
}

type Config struct {
	Petrobank map[string]string
	Database  Database
	Migration []yaml.MapItem
	Extract   map[string]RelatedItem
}

type ColumnMapping struct {
	source interface{}
	target string
}

type Migrator interface {
	migrate(bulk bool) error
}

type Insertor interface {
	insert(...interface{}) error
}

type BulkInsertor interface {
	bulkInsert(...interface{}) error
}

type closer interface {
	close() error
}

func init() {
	flag.Parse()

	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
}

func main() {
	if *debugFlag {
		log.SetLevel(log.DebugLevel)
	}

	fp, err := os.Open(*configFn)
	if err != nil {
		log.Fatal(err)
	}

	buf, err := ioutil.ReadAll(fp)
	if err != nil {
		log.Fatal(err)
	}

	var config Config
	err = yaml.Unmarshal(buf, &config)
	if err != nil {
		log.Fatal(err)
	}

	var dbConfig = config.Database
	if len(dbConfig.Port) == 0 {
		dbConfig.Port = "1433"
	}
	dsn := "server=" + dbConfig.Server +
		";user id=" + dbConfig.Username +
		";password=" + dbConfig.Password +
		";database=" + dbConfig.Database +
		";port=" + dbConfig.Port +
		";connection timeout=" + strconv.Itoa(*connTimeout)
	if *debugMssql {
		dsn += ";log=63"
	}

	log.Debugln("Opening MS Sqlserver connection...")
	db, err := sql.Open("mssql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	defer func() {
		if err := recover(); err != nil {
			if *debugFlag {
				panic(err)
			} else {
				log.Error(err)
			}
			os.Exit(1)
		}
	}()
	if !*extractOnly {
		migrate(db, config)
	}
	if !*migrateOnly {
		extract(db, config)
	}
}

func migrate(db *sql.DB, config Config) {
	// specifed sections
	var sections []string
	if len(*section) != 0 {
		sections = strings.Split(*section, ",")
	}

	contains := func(v string) bool {
		if len(sections) == 0 {
			return true
		}
		for _, sec := range sections {
			if strings.TrimSpace(sec) == v {
				return true
			}
		}
		return false
	}

	// migration
	for _, v := range config.Migration {
		var dataType = v.Key.(string)

		if !contains(dataType) {
			continue
		}

		var source string
		var target string
		var columnMappings []ColumnMapping

		var mc = v.Value.([]yaml.MapItem)
		for _, v := range mc {
			var key = v.Key.(string)
			if v.Value == nil {
				continue
			}
			switch key {
			case "source":
				source = v.Value.(string)
			case "target":
				target = v.Value.(string)
			case "columns":
				var value = v.Value.([]yaml.MapItem)
				columnMappings = make([]ColumnMapping, len(value))
				for i, m := range value {
					columnMappings[i].source = m.Key
					columnMappings[i].target = m.Value.(string)
				}
			default:
				log.Error("unknow key:", key)
			}
		}
		if source == "" {
			log.Errorf(`In %q: %q source field is empty`, *configFn, dataType)
			continue
		}
		if target == "" {
			log.Errorf(`In %q: %q target field is empty`, *configFn, dataType)
			continue
		}

		if len(columnMappings) == 0 {
			log.Errorf(`In %q: %q column field is empty`, *configFn, dataType)
			continue
		}
		var insertor = &MSInsert{
			db:             db,
			tableName:      target,
			columnMappings: columnMappings,
		}

		defer func() {
			if err := insertor.close(); err != nil {
				fmt.Printf("%#v\n", err)
				log.Fatal(err)
			}
		}()

		log.Infof("Migrating %q", dataType)

		if source == "static" {
			migration := &StaticMigration{
				insertor:       insertor,
				columnMappings: columnMappings,
			}
			if err := migration.migrate(); err != nil {
				if *exitOnError {
					log.Panicln(err)
				} else {
					log.Errorln(err)
				}
			} else {
				log.Infof("Migrating %q successful, commiting...", dataType)
			}
		} else if validPath(source) {
			var sources = strings.Split(source, ",")
			var readers = make([]io.Reader, 0)
			var names = make([]string, 0)

			for _, filename := range sources {
				if strings.HasSuffix(filename, ".zip") {
					zr, err := zip.OpenReader(filename)
					if err != nil {
						log.Fatalln(err)
					}
					defer zr.Close()
					for _, cr := range zr.File {
						r, err := cr.Open()
						if err != nil {
							log.Fatalln(err)
						}
						defer r.Close()
						readers = append(readers, r)
						names = append(names, filename+"!"+cr.Name)
					}
				} else {
					fp, err := os.Open(filename)
					if err != nil {
						log.Fatal(err)
					}
					defer fp.Close()
					readers = append(readers, fp)
					names = append(names, filename)
				}
			}

			for i, reader := range readers {
				migration := &CSVMigration{
					sourceName:     names[i],
					insertor:       insertor,
					r:              csv.NewReader(reader),
					tableName:      target,
					columnMappings: columnMappings,
				}
				if err := migration.migrate(*bulkInsert); err != nil {
					if *exitOnError {
						log.Panicln(err)
					} else {
						log.Errorln(err)
					}
				} else {
					log.Infof("Migrating %q from source %s successful, commiting...", dataType, names[i])
				}
			}
		}
	}
}

type RelatedItem struct {
	TargetTable  string
	TargetColumn string
	Key          string
	Index        int
	Sequence     string
	Constants    string
}

func extract(db *sql.DB, config Config) {
	// related item extraction
	relatedItemSql := "select information_item_id, access_condition from rm_information_item"
	var relatedItemKeys = make(map[string]bool)

	rows, err := db.Query(relatedItemSql)
	if err != nil {
		log.Fatalln(err)
	}
	defer rows.Close()
ROWS_LOOP:
	for rows.Next() {
		var documentId string
		var accessCondition sql.NullString
		if err := rows.Scan(&documentId, &accessCondition); err != nil {
			log.Fatal(err)
		}

		if !accessCondition.Valid {
			continue
		}

		parts := strings.Split(accessCondition.String, ";")
		var params = make(map[string]string)

		for _, v := range parts {
			if strings.TrimSpace(v) == "..." {
				continue
			}
			pair := strings.Split(v, "||")
			if len(pair) != 2 {
				log.Fatalf("related param split by || should contain 2 values: %s", v)
			}
			key := strings.TrimSpace(pair[0])
			params[key] = strings.TrimSpace(pair[1])
			relatedItemKeys[key] = true
		}

		for section, conf := range config.Extract {
			log.Debug("extracting for ", section)
			if params[conf.Key] == "" {
				continue ROWS_LOOP
			}

			idParts := stripAndSplit(params[conf.Key])
			id := idParts[conf.Index]
			var insertSql string
			if len(conf.Constants) != 0 {
				constants := strings.Split(conf.Constants, ",")
				var constCols []string
				var constVals []string
				for _, v := range constants {
					parts := strings.Split(strings.TrimSpace(v), "=")
					constCols = append(constCols, strings.TrimSpace(parts[0]))
					constVals = append(constVals, strings.TrimSpace(parts[1]))
				}

				insertSql = "insert into %s (%s, " +
					strings.Join(constCols, ",") +
					", component_obs_no, information_item_id, info_item_subtype)" +
					" values (?, '" +
					strings.Join(constVals, ",") +
					"', next value for %s, ?, 'RM_DOCUMENT')"
			} else {
				insertSql = "insert into %s (%s, component_obs_no, information_item_id, info_item_subtype)" +
					"values (?, next value for %s, ?, 'RM_DOCUMENT')"
			}
			insert := fmt.Sprintf(insertSql,
				conf.TargetTable, conf.TargetColumn, conf.Sequence)
			log.Debugln(repl(insert, id, documentId))
			res, err := db.Exec(insert, id, documentId)
			if err != nil {
				if *exitOnError {
					log.Info(repl(insert, id, documentId))
					log.Fatalln(err)
				} else {
					log.Errorln(err)
				}
			}
			log.Debug(res)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("distinct keys:")
	for key, _ := range relatedItemKeys {
		fmt.Println(key)
	}
}

func validPath(s string) bool {
	return strings.Contains(s, ".") || strings.Contains(s, "/")
}

var stripId = regexp.MustCompile(`(.*?) (\[\d+\])`)

func stripAndSplit(s string) []string {
	match := stripId.FindStringSubmatch(s)
	if len(match) != 3 {
		log.Fatalln("match id pattern should return 2 groups: %v", match)
	}
	return strings.Split(match[1], "\\")
}
