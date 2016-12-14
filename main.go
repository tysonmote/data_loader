package main

import (
	"database/sql"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

const (
	dbPath   = "./data.db"
	specPath = "./specs/"
	dataPath = "./data/"
)

func main() {
	specs, err := LoadAllSpecs(specPath)
	if err != nil {
		panic(err)
	}

	createTables(specs)
	if err != nil {
		panic(err)
	}

	dfs, err := AllDataFiles(specs, dataPath)
	if err != nil {
		panic(err)
	}

	err = loadDataFiles(dfs)
	if err != nil {
		panic(err)
	}
}

func createTables(specs map[string]Spec) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	// Create all tables
	for _, spec := range specs {
		log.Printf("Creating table %#v", spec.Name)
		_, err := db.Exec(CreateTableSQL(spec.Name, spec))
		if err != nil {
			return err
		}
	}

	return nil
}

func loadDataFiles(dfs []DataFile) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	for _, df := range dfs {
		stmt, err := db.Prepare(PreparedStatementSQL(df.Spec.Name, df))
		if err != nil {
			return err
		}

		c, err := df.AllRows()
		if err != nil {
			return err
		}
		for row := range c {
			_, err = stmt.Exec(row...)
			if err != nil {
				return err
			}
		}

		stmt.Close()
	}

	return nil
}
