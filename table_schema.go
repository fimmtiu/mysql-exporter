package main

import (
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"
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
	goType reflect.Type
}

func NewTableSchema(name string) TableSchema {
	return TableSchema{name, make([]Column, 0), nil}
}

func (ts *TableSchema) AddColumn(col Column) {
	if ts.goType != nil {
		panic(fmt.Errorf("Can't add column '%s' to '%s' after we've already generated the Go type!", col.Name, ts.Name))
	}
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

func (c Column) ConvertToGoColumn() reflect.StructField {
	// The name has to be capitalized to make Go consider the field public.
	field := reflect.StructField{Name: UpperFirst(c.Name)}

	switch c.SqlType {
	case "tinyint":
		if c.Width == 1 {
			field.Type = reflect.TypeOf(false)
		} else {
			if c.Signed {
				field.Type = reflect.TypeOf(int8(0))
			} else {
				field.Type = reflect.TypeOf(uint8(0))
			}
		}
	case "int", "smallint", "mediumint":
		if c.Signed {
			field.Type = reflect.TypeOf(int32(0))
		} else {
			field.Type = reflect.TypeOf(uint32(0))
		}
	case "bigint":
		if c.Signed {
			field.Type = reflect.TypeOf(int64(0))
		} else {
			field.Type = reflect.TypeOf(uint64(0))
		}

	case "decimal":
		field.Type = reflect.TypeOf(big.Int{})

	// Floating-point numbers
	case "float":
		field.Type = reflect.TypeOf(float32(0))
	case "double":
		field.Type = reflect.TypeOf(float64(0))

	// Strings
	case "char", "varchar", "text", "mediumtext", "longtext":
		field.Type = reflect.TypeOf("")

	// Binary data
	case "binary":
		field.Type = reflect.ArrayOf(c.Width, reflect.TypeOf(byte(0)))
	case "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		field.Type = reflect.TypeOf([]byte{})

	// Times and dates.
	case "date":
		field.Type = reflect.TypeOf(int32(0))
	case "datetime", "timestamp":
		field.Type = reflect.TypeOf(time.Time{})

	default: panic(fmt.Errorf("Unsupported SQL type for '%s': %s", c.Name, c.SqlType))
	}
	return field
}

func (ts *TableSchema) GoType() reflect.Type {
	if ts.goType == nil {
		columns := []reflect.StructField{}
		schemaField := reflect.StructField{Name: "_tableSchema", Type: reflect.TypeOf(ts), PkgPath: "github.com/clio/mysql-exporter/anonymoustype"}
		nullField := reflect.StructField{Name: "_nullColumns", Type: reflect.TypeOf([]uint8{}), PkgPath: "github.com/clio/mysql-exporter/anonymoustype"}
		columns = append(columns, schemaField, nullField)
		for _, col := range ts.Columns {
			columns = append(columns, col.ConvertToGoColumn())
		}
		ts.goType = reflect.StructOf(columns)
	}
	return ts.goType
}

func (ts *TableSchema) NullColumnsLength() int {
	if len(ts.Columns) < 8 {
		return 1
	} else if len(ts.Columns) % 8 == 0 {
		return (len(ts.Columns) - 2) / 8
	} else {
		return (len(ts.Columns) - 2) / 8 + 1
	}
}

func GoRowSchema(gorow any) *TableSchema {
	return reflect.ValueOf(gorow).Field(1).Interface().(*TableSchema)
}

func GoRowColumnIsNull(gorow any, index int) bool {
	bitfield := reflect.ValueOf(gorow).Field(1).Interface().([]uint8)
	return bitfield[index / 8] & (1 << (index % 8)) != 0
}
