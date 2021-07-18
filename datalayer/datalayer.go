package datalayer

import (
	"database/sql"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

func CreateDatabase(dbname string) (bool, error) {
	if _, err := os.Stat(dbname); err == nil {
		log.Println("Database exists, skipping creation")
	} else if os.IsNotExist(err) {
		file, err := os.Create(dbname)
		if err != nil {
			return false, err
		}
		file.Close()
	}
	return true, nil
}

func CreateTable(db *sql.DB) (bool, error) {
	createReadingSql := `CREATE TABLE IF NOT EXISTS Reading ( 
		"TimeStamp" TEXT,		
		"CarHumidity" FLOAT,
		"CarTemperatureFahrenheit" FLOAT,
		"CarTemperatureCelcius" FLOAT,
		"OutsideHumidity" FLOAT,
		"OutsideTemperatureFahrenheit" FLOAT,
		"OutsideTemperatureCelcius" FLOAT);`

	statement, err := db.Prepare(createReadingSql)
	if err != nil {
		log.Println("Failed to create Database")
	}
	statement.Exec()
	return true, nil
}

func FirstInsert(db *sql.DB, timestamp string, value float64) (bool, error) {

	fahrenheit := value
	celcius := ((fahrenheit - 32) * 5 / 9)

	insertReadingSql := `INSERT INTO Reading (TimeStamp, CarTemperatureFahrenheit, CarTemperatureCelcius) VALUES (?,?,?)`

	statement, err := db.Prepare(insertReadingSql)

	if err != nil {
		log.Println("Failed to prepare SQL Statement")
		return false, err
	}

	_, err = statement.Exec(timestamp, fahrenheit, celcius)

	if err != nil {
		log.Println("Failed to insert data")
		log.Printf("Timestamp: %v \n Value: %v \n", timestamp, value)
		return false, err
	}

	return true, nil
}

func UpdateData(db *sql.DB, columnname string, timestamp string, value float64, temperature bool) (bool, error) {

	updateReadingSql := ""

	if temperature {
		// if its a temperature
		fahrenheit := value
		celcius := ((fahrenheit - 32) * 5 / 9)

		updateReadingSql = `UPDATE Reading SET ` + columnname + "fahrenheit" + ` =?, ` + columnname + "celcius" + ` =? WHERE TimeStamp = ?`

		statement, err := db.Prepare(updateReadingSql)

		if err != nil {
			log.Println(updateReadingSql)
			log.Println("Failed to prepare SQL Statement")
			return false, err
		}
		_, err = statement.Exec(fahrenheit, celcius, timestamp)

		if err != nil {
			log.Println("Failed to insert data")
			return false, err
		}
	} else {
		// if it's humidity
		updateReadingSql = `UPDATE Reading SET ` + columnname + ` =? WHERE TimeStamp = ?`
		statement, err := db.Prepare(updateReadingSql)

		if err != nil {
			log.Printf("Column is %v", columnname)
			log.Println(updateReadingSql)
			log.Println("Failed to prepare SQL Statement")
			return false, err
		}
		_, err = statement.Exec(value, timestamp)

		if err != nil {
			log.Println("Failed to insert data")
			return false, err
		}
	}

	return true, nil
}
