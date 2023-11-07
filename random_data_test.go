package main

import (
	"fmt"
	"math"
	"math/rand"
)

// This file is only compiled in test mode.

func PopulateTestData() {
	if !InTestMode() {   // just out of paranoia
		return
	}

	MustExecute("DROP DATABASE IF EXISTS `" + config.MysqlDatabase + "`")
	MustExecute("CREATE DATABASE `" + config.MysqlDatabase + "`")
	MustExecute("USE `" + config.MysqlDatabase + "`")

	tables := []string{"all_date_types", "all_string_types", "all_number_types"}
	for _, table := range tables {
		createTable := MustReadFile(fmt.Sprintf("test_schemas/%s.sql", table))
		MustExecute(createTable)

		schema := ParseSchema(createTable)
		insert := "INSERT INTO `" + table + "` VALUES "
		for i := 0; i < 1000; i++ {
			row := ""
			if i > 0 {
				row += ", "
			}
			row += "(NULL"
			for _, column := range schema.Columns[1:] {
				row += ","

				// 20% of all nullable columns are NULL.
				if column.Nullable && rand.Intn(100) < 20 {
					row += "NULL"
				} else {
					switch column.SqlType {
					case "date":
						row += fmt.Sprintf(`"%d-%02d-%02d"`, rand.Intn(200)+1900, rand.Intn(12)+1, rand.Intn(28)+1)
					case "datetime":
						row += fmt.Sprintf(`"%d-%02d-%02d %02d:%02d:%02d"`, rand.Intn(200)+1900, rand.Intn(12)+1, rand.Intn(28)+1, rand.Intn(24), rand.Intn(60), rand.Intn(60))
					case "timestamp":
						row += fmt.Sprintf(`"%d-%02d-%02d %02d:%02d:%02d"`, rand.Intn(67)+1970, rand.Intn(12)+1, rand.Intn(28)+1, rand.Intn(24), rand.Intn(60), rand.Intn(60))
					case "time":
						row += fmt.Sprintf(`"%02d:%02d:%02d"`, rand.Intn(24), rand.Intn(60), rand.Intn(60))

					case "tinyint":	  row += randomInt( 8, column.Signed)
					case "smallint":	row += randomInt(16, column.Signed)
					case "mediumint":	row += randomInt(24, column.Signed)
					case "int":	      row += randomInt(32, column.Signed)
					case "bigint":  	row += randomInt(64, column.Signed)
					case "float":     row += fmt.Sprintf("%f", rand.Float32() * math.MaxFloat32)
					case "double":    row += fmt.Sprintf("%f", rand.Float64() * math.MaxFloat64)
					case "decimal":
						// FIXME: Do this correctly later.
						row += "10.0"

					case "char", "varchar":
						row += randomString(rand.Intn(column.Width))
					case "text", "tinytext", "mediumtext", "longtext":
						row += randomString(rand.Intn(256))
					case "binary", "varbinary":
						row += randomEscapedByteArray(rand.Intn(column.Width))
					case "blob", "tinyblob", "mediumblob", "longblob":
						row += randomEscapedByteArray(rand.Intn(256))
					}
				}
			}
			insert += row + ")"
		}
		MustExecute(insert)
	}
}

func randomInt(bits int, signed bool) string {
	var max uint64 = 1 << bits
	if bits == 64 {
		if signed {
			return fmt.Sprintf("%d", int64(rand.Uint64()))
		} else {
			return fmt.Sprintf("%d", rand.Uint64())
		}
	} else {
		if signed {
			return fmt.Sprintf("%d", rand.Intn(int(max)) - (int(max) / 2))
		} else {
			return fmt.Sprintf("%d", rand.Intn(int(max)))
		}
	}
}

func randomString(length int) string {
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789               "
	result := `"`
	for i := 0; i < length; i++ {
		result += string(chars[rand.Intn(len(chars))])
	}
	return result + `"`
}

// FIXME: I can't be arsed to handle characters >127 correctly right now, since they'll be interpreted as
// UTF8 escapes and everything gets weird.
func randomEscapedByteArray(length int) string {
	result := `"`
	for i := 0; i < length; i++ {
		s := string(rune(rand.Intn(128)))
		// See https://dev.mysql.com/doc/refman/8.0/en/string-literals.html#character-escape-sequences
		switch s {
		case "\\", "\"", "'": result += "\\" + s
		case "\n":            result += "\\n"
		case "\r":            result += "\\r"
		case "\t":            result += "\\t"
		case "\b":            result += "\\b"
		case "\x00":          result += "\\0"
		case "\x1a":          result += "\\Z"
		default:
			if len(s) != 1 {
				panic("wtf")
			}
			result += s
		}
	}
	return result + `"`
}

func MustExecute(sql string) {
	_, err := pool.Execute(sql)
	if err != nil {
		panic(err)
	}
}
