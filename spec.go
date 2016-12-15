package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Datatype int

const (
	TextType Datatype = iota
	BoolType Datatype = iota
	IntType  Datatype = iota
)

// Spec contains information about a file format: the name, and definitions for
// each column in a data file.
type Spec struct {
	Name    string
	Columns []Column
}

// Column contains specifications for one column in a data file.
type Column struct {
	Name     string
	Width    int
	Datatype Datatype
}

// LoadAllSpecs finds all valid spec files in the given directory and returns
// them in a map that is keyed by the format name. Invalid spec files cause
// a warning message to be logged and are then skipped from the returned map.
func LoadAllSpecs(dir string) (specs map[string]Spec, err error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return specs, err
	}

	specs = map[string]Spec{}
	for _, file := range files {
		path := filepath.Join(dir, file.Name())
		if file.IsDir() || !strings.HasSuffix(path, ".csv") {
			log.Printf("Skipping unrecognized spec file: %s", path)
			continue
		}
		log.Printf("Loading spec: %s", path)
		spec, err := load(path)
		if err != nil {
			log.Printf("WARNING: Couldn't read %s: %s", path, err)
			continue
		}

		specs[spec.Name] = spec
	}

	return specs, nil
}

func load(path string) (spec Spec, err error) {
	spec = Spec{
		Name: stripExtension(filepath.Base(path)),
	}

	file, err := os.Open(path)
	if err != nil {
		return spec, err
	}
	defer file.Close()

	r := csv.NewReader(file)

	var headers []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return spec, err
		}

		if headers == nil {
			headers = record
		} else {
			column, err := loadColumn(headers, record)
			if err != nil {
				return spec, err
			}
			spec.Columns = append(spec.Columns, column)
		}
	}

	return spec, nil
}

func loadColumn(headers, record []string) (col Column, err error) {
	col = Column{}

	m, err := mapify(headers, record)
	if err != nil {
		return col, err
	}

	col.Name = m["column name"]

	col.Width, err = strconv.Atoi(m["width"])
	if err != nil {
		return col, err
	}

	switch m["datatype"] {
	case "TEXT":
		col.Datatype = TextType
	case "BOOLEAN":
		col.Datatype = BoolType
	case "INTEGER":
		col.Datatype = IntType
	default:
		return col, fmt.Errorf("unrecognized datatype: %s", m["datatype"])
	}

	return col, nil
}

// -- utils

func mapify(headers, values []string) (m map[string]string, err error) {
	if len(headers) != len(values) {
		return nil, fmt.Errorf("row is too short: %#v", values)
	}

	m = map[string]string{}
	for i, header := range headers {
		m[header] = values[i]
	}

	return m, nil
}

func stripExtension(name string) string {
	parts := strings.Split(name, ".")
	if len(parts) == 1 {
		return name
	}
	return strings.Join(parts[0:len(parts)-1], ".")
}
