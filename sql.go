package main

import (
	"fmt"
	"strings"
)

func CreateTableSQL(tablename string, spec Spec) string {
	rows := []string{}

	rows = append(rows, fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (", tablename))
	for _, col := range spec.Columns {
		switch col.Datatype {
		case TextType:
			rows = append(rows, fmt.Sprintf("`%s` VARCHAR(%d) NOT NULL,", col.Name, col.Width))
		case BoolType, IntType:
			rows = append(rows, fmt.Sprintf("`%s` INTEGER NOT NULL,", col.Name))
		default:
			panic(fmt.Errorf("unknown Datatype: %#v", col.Datatype))
		}
	}
	stripLastComma(rows)
	rows = append(rows, ");")

	return strings.Join(rows, "\n")
}

func PreparedStatementSQL(tablename string, df DataFile) string {
	names := make([]string, len(df.Spec.Columns))
	for i, col := range df.Spec.Columns {
		names[i] = col.Name
	}

	cmd := []string{}
	cmd = append(cmd, fmt.Sprintf("INSERT INTO %s(", tablename))
	for _, name := range names {
		cmd = append(cmd, fmt.Sprintf("%s,", name))
	}
	stripLastComma(cmd)
	cmd = append(cmd, ") values (")
	for range names {
		cmd = append(cmd, "?,")
	}
	stripLastComma(cmd)
	cmd = append(cmd, ")")

	return strings.Join(cmd, " ")
}

func stripLastComma(ss []string) {
	ss[len(ss)-1] = strings.TrimRight(ss[len(ss)-1], ",")
}
