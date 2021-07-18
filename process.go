package main

import (
	"database/sql"
	"encoding/csv"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/jeremymorgan/hotcar/datalayer"
)

func main() {

	dbname := "Readings.db"

	if _, err := os.Stat(dbname); os.IsNotExist(err) {
		log.Printf("Database does not exist. We'll create one!")

		// create a new database
		newdb, err := datalayer.CreateDatabase(dbname)
		if err != nil {
			log.Fatal(err)
		}
		// if the database now exists
		if newdb {

			// let's connect to it!
			dbconn, err := sql.Open("sqlite3", dbname)
			if err != nil {
				log.Fatal(err)
			}

			created, err := datalayer.CreateTable(dbconn)
			if err != nil {
				log.Fatal(err)
			}

			if created {
				// table was created!
				log.Println("Table was created! ")
				loadCSVIntoDB(dbconn, "./csvs/CarTemperature.csv", true, true)
				loadCSVIntoDB(dbconn, "./csvs/CarHumidity.csv", false, false)
				loadCSVIntoDB(dbconn, "./csvs/OutsideTemperature.csv", false, true)
				loadCSVIntoDB(dbconn, "./csvs/OutsideHumidity.csv", false, false)
			}

		} else {
			log.Println("Something went wrong")
		}

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

func loadCSVIntoDB(dbconn *sql.DB, csvfile string, firstInsert bool, temperature bool) {

	records := readCsvFile(csvfile)

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

		if firstInsert {
			// if this is the first insert, we will insert timestamps and data
			result, err := datalayer.FirstInsert(dbconn, timestamp, ourValue)

			if err != nil {
				log.Fatal(err)
			}
			// if it was good!
			if result {
				log.Println("Success")
			}

		} else {
			// first insert was done, let's populate those records
			tablename := strings.Replace(csvfile[:len(csvfile)-4], "./csvs/", "", len(csvfile))

			result, err := datalayer.UpdateData(dbconn, tablename, timestamp, ourValue, temperature)

			if err != nil {
				log.Fatal(err)
			}

			if result {
				log.Printf("Inserted %v", ourValue)
			}
		}
	}
}
