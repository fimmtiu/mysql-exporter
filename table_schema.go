package main

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type Column struct {
	Name string
	SqlType string
	Width int
	Scale int
	Signed bool
	Nullable bool
}

type TableSchema struct {
	Name string
	Columns []Column

	// goType reflect.Type
	// parquetSchema *parquet.Schema
}

func NewTableSchema(name string) TableSchema {
	return TableSchema{name, make([]Column, 0)}
}

func (ts *TableSchema) AddColumn(col Column) {
	ts.Columns = append(ts.Columns, col)
}

func ParseSchema(s string) *TableSchema {
	strings.TrimSpace(s)
	if !strings.HasPrefix(s, "CREATE TABLE `") {
		panic(fmt.Errorf("Doesn't look like a valid CREATE TABLE statement!\n%s\n", s))
	}

	splitStatement := strings.SplitN(s, "`", 3)
	tableName := splitStatement[1]

	schema := NewTableSchema(tableName)
	lines := strings.Split(splitStatement[2], "\n")
	for _, line := range lines {
		// Ignore the first and last lines of the CREATE TABLE statement, empty lines, and all index definitions.
		if strings.HasPrefix(line, " (") || strings.HasPrefix(line, ")") ||
		   strings.Contains(line, " KEY ") || len(line) == 0 {
			continue
		}
		splitLine := strings.SplitN(line, "`", 3)
		name := splitLine[1]
		typeInfo := strings.TrimSpace(splitLine[2])
		sqlType, width, scale := parseSqlType(strings.SplitN(typeInfo, " ", 2)[0])

		column := NewColumn(
			name, sqlType, width, scale,
			!strings.Contains(typeInfo, " unsigned"),
			!strings.Contains(typeInfo, " NOT NULL"),
		)
		schema.AddColumn(column)
	}

	return &schema
}

// Break down a SQL type string like "int", "varchar(11)" or "decimal(20,10)".
func parseSqlType(s string) (sqlType string, width, scale int) {
	if !strings.Contains(s, "(") {
		return s, 0, 0
	}
	splitString := strings.SplitN(s, "(", 2)
	splitString[1] = strings.TrimSuffix(splitString[1], ")")
	commaIndex := strings.Index(splitString[1], ",")
	if commaIndex >= 0 {
		widthString := splitString[1][0:commaIndex]
		scaleString := splitString[1][commaIndex+1:len(splitString[1])]
		return splitString[0], MustParseInt(widthString), MustParseInt(scaleString)
	} else {
		return splitString[0], MustParseInt(splitString[1]), 0
	}
}

func NewColumn(name, sqlType string, width, scale int, signed, nullable bool) Column {
	column := Column{name, sqlType, width, scale, signed, nullable}
	return column
}

func (c Column) MysqlLibraryType() uint8 {
	switch c.SqlType {
	case "tinyint": return mysql.MYSQL_TYPE_TINY
	case "smallint": return mysql.MYSQL_TYPE_SHORT
	case "mediumint": return mysql.MYSQL_TYPE_INT24
	case "int": return mysql.MYSQL_TYPE_LONG
	case "bigint": return mysql.MYSQL_TYPE_LONGLONG
	case "float": return mysql.MYSQL_TYPE_FLOAT
	case "double": return mysql.MYSQL_TYPE_DOUBLE
	case "decimal": return mysql.MYSQL_TYPE_NEWDECIMAL
	case "char": return mysql.MYSQL_TYPE_STRING
	case "varchar": return mysql.MYSQL_TYPE_VAR_STRING
	case "text": return mysql.MYSQL_TYPE_STRING
	case "mediumtext": return mysql.MYSQL_TYPE_STRING
	case "longtext": return mysql.MYSQL_TYPE_STRING
	case "binary": return mysql.MYSQL_TYPE_STRING
	case "varbinary": return mysql.MYSQL_TYPE_VAR_STRING
	case "tinyblob": return mysql.MYSQL_TYPE_BLOB
	case "blob": return mysql.MYSQL_TYPE_BLOB
	case "mediumblob": return mysql.MYSQL_TYPE_BLOB
	case "longblob": return mysql.MYSQL_TYPE_BLOB
	case "date": return mysql.MYSQL_TYPE_DATE
	case "datetime": return mysql.MYSQL_TYPE_DATETIME
	case "timestamp": return mysql.MYSQL_TYPE_TIMESTAMP
	default: panic(fmt.Errorf("Unknown SQL type for '%s': %s", c.Name, c.SqlType))
	}
}
