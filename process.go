package main

import (
	"database/sql"
	"encoding/csv"
	"log"
	"os"
	"strconv"

	"github.com/jeremymorgan/hotcar/datalayer"
)

func main() {

	dbname := "test"
	newdb, err := datalayer.CreateDatabase(dbname)
	if err != nil {
		log.Fatal(err)
	}

	dbconn, err := sql.Open("sqlite3", dbname+".db")

	if err != nil {
		log.Fatal(err)
	}

	if err != nil {
		log.Fatal(err)
	}
	// if we have a database game on
	if newdb {
		log.Println("Database created or exists")
		// lets connect to it
		dbconn, err := sql.Open("sqlite3", "./"+dbname+".db")
		if err != nil {
			log.Fatal(err)
		}
		// let's create a table
		newtable, err := datalayer.CreateTable(dbconn)
		if err != nil {
			log.Fatal(err)
		}

		// if the table exists
		if newtable {
			log.Println("Table exists")
		}
	}

	records := readCsvFile("./csvs/OutsideTemperature.csv")

	for i := range records {
		timestamp := records[i][3][:len(records[i][3])-4]
		seconds, err := strconv.Atoi(string(timestamp[len(timestamp)-2:]))

		if err != nil {
			log.Printf("Error: %v", err)
		}

		if seconds >= 30 {
			timestamp = timestamp[:len(timestamp)-2] + "30"
		} else {
			timestamp = timestamp[:len(timestamp)-2] + "00"
		}

		ourValue, err := strconv.ParseFloat(records[i][1], 64)

		if err != nil {
			log.Println("Could not convert value!")
		}

		result, err := datalayer.InsertData(dbconn, "OutsideTemperature", timestamp, ourValue)

		if err != nil {
			log.Println("Could not insert")
		}

		if result {
			log.Printf("Inserted %v", ourValue)
		}
		/*
			result, err := datalayer.FirstInsert(dbconn, timestamp, ourValue)

			if err != nil {
				log.Println("Could not insert!")
			}

			if result {
				log.Println("Success")
		}*/

	}
}

func readCsvFile(filePath string) [][]string {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read input file "+filePath, err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+filePath, err)
	}
	return records
}
