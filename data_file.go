package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// DataFile contains metadata about a data file whose format is specified by
// a Spec.
type DataFile struct {
	Name      string
	Spec      Spec
	path      string
	namedSpec string
}

// AllDataFiles loads metadata for all data files in the given directory. If
// any data files use a format not contained in the given specs, a warning is
// logged and the data file is excluded from the returned results.
func AllDataFiles(specs map[string]Spec, dir string) (dfs []DataFile, err error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return dfs, err
	}

	dfs = []DataFile{}
	for _, file := range files {
		path := filepath.Join(dir, file.Name())
		if file.IsDir() || !strings.HasSuffix(path, ".txt") {
			log.Printf("Skipping unrecognized data file: %s", path)
			continue
		}
		log.Printf("Opening data file: %s", path)
		df, err := getDataFile(path)
		if err != nil {
			log.Printf("WARNING: Couldn't read %s: %s", path, err)
			continue
		}

		if spec, ok := specs[df.namedSpec]; ok {
			df.Spec = spec
			dfs = append(dfs, df)
		} else {
			log.Printf("WARNING: Unrecognized format: %#v", df.namedSpec)
		}
	}

	return dfs, nil
}

func getDataFile(path string) (df DataFile, err error) {
	df = DataFile{path: path}

	// FIXME: This is not a robust way to load the filename parts.
	name := filepath.Base(path)
	parts := strings.Split(name, "_")
	if len(parts) != 2 {
		return df, fmt.Errorf("Unrecognized data file name format: %s", name)
	}
	df.namedSpec = parts[0]
	subparts := strings.Split(parts[0], ".")
	df.Name = subparts[0]

	return df, nil
}

func (df *DataFile) AllRows() (c chan []interface{}, err error) {
	file, err := os.Open(df.path)
	if err != nil {
		return nil, err
	}

	c = make(chan []interface{})

	go func(file *os.File) {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			row := scanner.Text()
			c <- df.parseRow(row)
		}

		// TODO: Gracefully handle errors
		if err = scanner.Err(); err != nil {
			log.Fatal(err)
		}

		file.Close()
		close(c)
	}(file)

	return c, nil
}

func (df *DataFile) parseRow(row string) []interface{} {
	parts := []interface{}{}

	var start, end int
	for _, col := range df.Spec.Columns {
		end = start + col.Width
		part := strings.TrimSpace(row[start:end])

		switch col.Datatype {
		case IntType, BoolType:
			i, err := strconv.Atoi(part)
			if err != nil {
				// FIXME: Handle this better
				log.Printf("WARNING: expected int, got: %s", part)
			}
			parts = append(parts, i)
		default:
			parts = append(parts, part)
		}

		start = end
	}

	return parts
}
