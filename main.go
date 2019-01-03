package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/alash3al/go-color"

	"gopkg.in/cheggaaa/pb.v1"

	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

var (
	flagMYSQLUser     = flag.String("mysql-user", "root", "source (mysql) settings")
	flagMYSQLPassword = flag.String("mysql-password", "root", "source (mysql) settings")
	flagMYSQLHost     = flag.String("mysql-host", "127.0.0.1:3306", "source (mysql) settings")
	flagMYSQLDBName   = flag.String("mysql-db", "mysql", "source (mysql) settings")
	flagSQLITE        = flag.String("sqlite", "./database.sqlite", "target (sqlite) database")
	flagSkip          = flag.String("skip", "", "tables to skip")
)

var (
	mysqlConn  *sqlx.DB
	sqliteConn *sqlx.DB
	tables     []string
	mainBar    *pb.ProgressBar
)

func init() {
	mydsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?sql_mode=''", *flagMYSQLUser, *flagMYSQLPassword, *flagMYSQLHost, *flagMYSQLDBName)
	mydb, err := sqlx.Open("mysql", mydsn)
	if err != nil {
		log.Fatal(err.Error())
	}

	mysqlConn = mydb

	litedb, err := sqlx.Open("sqlite3", *flagSQLITE+"?cache=shared&_journal=memory")
	if err != nil {
		log.Fatal(err.Error())
	}

	sqliteConn = litedb
}

func main() {
	defer mysqlConn.Close()
	defer sqliteConn.Close()

	tables, err := getMYSQLTables()
	if err != nil {
		log.Fatal(err.Error())
	}

	mainBar = pb.StartNew(len(tables)).Prefix(color.CyanString("⇨ Overall: "))
	wg := &sync.WaitGroup{}

	for _, tb := range tables {
		wg.Add(1)
		go (func(tb string) {
			defer wg.Done()
			if err := moveTable(tb); err != nil {
				log.Println(err.Error())
			}
		})(tb)
	}

	wg.Wait()
}

func getMYSQLTables() ([]string, error) {
	rows, err := mysqlConn.Query("SELECT table_name FROM information_schema.tables where table_schema=?", *flagMYSQLDBName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	skips := map[string]bool{}

	for _, x := range strings.Split(*flagSkip, ",") {
		x = strings.TrimSpace(x)
		if "" != x {
			skips[x] = true
		}
	}

	cols := []string{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err == nil {
			if skips[v] {
				continue
			}
			cols = append(cols, v)
		}
	}

	return cols, nil
}

func moveTable(tb string) error {
	var count uint64
	mysqlConn.QueryRow("SELECT COUNT(*) FROM " + tb).Scan(&count)

	rows, err := mysqlConn.Queryx("SELECT * FROM " + tb)
	if err != nil {
		return err
	}

	defer rows.Close()

	mainBar.Increment()

	if count < 1 {
		return nil
	}

	subBar := pb.StartNew(int(count)).Prefix(color.GreenString("⇨ " + tb + ": "))

	for rows.Next() {
		subBar.Increment()
		data := make(map[string]interface{})
		if err := rows.MapScan(data); err != nil {
			continue
		}

		cols, vals, names := (func() (cols []string, vals []interface{}, names []string) {
			for k, v := range data {
				cols = append(cols, k)
				vals = append(vals, v)
				names = append(names, "?")
			}

			return cols, vals, names
		})()

		sql := "INSERT INTO `" + (tb) + "`(`" + (strings.Join(cols, "`, `")) + "`) VALUES(" + (strings.Join(names, ",")) + ")"
		sqliteConn.Exec(sql, vals...)
	}

	return nil
}
