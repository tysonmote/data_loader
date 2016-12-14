package spec

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

type Spec struct {
	Name    string
	Columns []Column
}

type Column struct {
	Name     string
	Width    int
	Datatype Datatype
}

func LoadAll(dir string) (specs []Spec, err error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return specs, err
	}

	specs = []Spec{}
	for _, file := range files {
		path := filepath.Join(dir, file.Name())
		if file.IsDir() || !strings.HasSuffix(path, ".csv") {
			log.Printf("Skipping %s", path)
			continue
		}
		spec, err := load(path)
		if err != nil {
			log.Printf("WARNING: Couldn't read %s: %s", path, err)
		}

		specs = append(specs, spec)
	}

	return specs, nil
}

func (s *Spec) CreateSQL(tablename string) string {
	rows := []string{}

	rows = append(rows, fmt.Sprintf("CREATE TABLE `%s` (", tablename))
	for _, col := range s.Columns {
		switch col.Datatype {
		case TextType:
			rows = append(rows, fmt.Sprintf("`%s` VARCHAR(%d) NOT NULL;", col.Name, col.Width))
		case BoolType, IntType:
			rows = append(rows, fmt.Sprintf("`%s` INTEGER NOT NULL;", col.Name))
		default:
			panic(fmt.Errorf("unknown Datatype: %#v", col.Datatype))
		}
	}
	rows = append(rows, ");")

	return strings.Join(rows, "\n")
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
