package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"strings"
	"time"
)

func getDateForRowID(db *sql.DB, id int) (date string) {
	stmtOut, err := db.Prepare("SELECT logtime FROM aprs_uk WHERE id = ?")
	if err != nil {
		panic(err.Error())
	}
	var logtime string
	err = stmtOut.QueryRow(id).Scan(&logtime)

	if err != nil {
		panic(err.Error())
	}

	date = strings.Split(logtime, " ")[0]

	defer stmtOut.Close()
	return
}

func getEndNumber(db *sql.DB, operation string) (id int) {
	if operation == "ASC" {
		stmtOut, err := db.Prepare("SELECT id FROM aprs_uk ORDER BY id ASC LIMIT 1")
		if err != nil {
			panic(err.Error())
		}
		err = stmtOut.QueryRow().Scan(&id)
	} else {
		stmtOut, err := db.Prepare("SELECT id FROM aprs_uk ORDER BY id DESC LIMIT 1")
		if err != nil {
			panic(err.Error())
		}
		err = stmtOut.QueryRow().Scan(&id)
	}
	return
}

func main() {
	CFG := GetCFG()
	DBHost := flag.String("host", CFG.DBHost, "<hostname>:<port>")
	DBName := flag.String("database", CFG.DBName, "<dbname>")
	DBUser := flag.String("user", CFG.DBUser, "<dbuser>")
	DBPass := flag.String("pass", CFG.DBPass, "<dbpass>")
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", *DBUser, *DBPass, *DBHost, *DBName))

	logger := log.New(os.Stderr, "[ShovelBackup] ", log.Ltime)
	logger.Println("Connecting to DB")

	if err != nil {
		logger.Fatalln("Unable to connect to the database, Aborting.")
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		logger.Fatalln("Unable to connect to the database, Aborting.")
	}

	var firstrow int
	firstrow = getEndNumber(db, "ASC")
	var lastrow int
	lastrow = getEndNumber(db, "DESC")

	logger.Printf("Processing: %dish rows\n", (lastrow - firstrow))

	var firstrow_date = getDateForRowID(db, firstrow)
	var lastrow_date = getDateForRowID(db, lastrow)

	logger.Println(firstrow_date)
	logger.Println(lastrow_date)

	layout := "2001-11-40"
	t, _ := time.Parse(time.RFC3339, firstrow_date)
	logger.Println(t)
	t1, _ := time.Parse(layout, lastrow_date)
	logger.Println(t1)
}
