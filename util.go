package main

import (
	"os"
	"strconv"
	"strings"
)

// FIXME: Go through these later, verify that we're using all of them.

// Parses an integer from a string, dying if anything goes wrong.
func MustParseInt(s string) int {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return int(n)
}

// Slurps the contents of a file from disk, dying if anything goes wrong.
func MustReadFile(filename string) string {
	data, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	return string(data)
}

// Returns a copy of a string with the first letter capitalized.
func UpperFirst(s string) string {
	return strings.ToUpper(s[0:1]) + s[1:]
}

// Returns true if the given string is in the slice.
func StringInList(s string, list []string) bool {
	for _, ls := range list {
		if ls == s {
			return true
		}
	}
	return false
}

// Returns true if a string looks true-ish.
func StringToBool(s string) bool {
	s = strings.TrimSpace(strings.ToLower(s))
	return s == "1" || s == "true" || s == "yes" || s == "y"
}

// This indexing is subtle enough that it deserves to be given a descriptive function name.
func DeleteFromSlice[T any](list []T, index int) []T {
	return append(list[:index], list[index+1:]...)
}

// Returns true if we're running tests.
func InTestMode() bool {
	return strings.HasSuffix(os.Args[0], ".test")
}

// Returns true if we're running integration tests.
func InIntegrationTestMode() bool {
	return InTestMode() && StringToBool(os.Getenv("INTEGRATION_TESTS"))
}

// FIXME: This probably belongs in some other file, maybe mysql_connection.go.
// func DecodeFieldValue(field *mysql.Field, value mysql.FieldValue) any {
// 	if value.Type == mysql.FieldValueTypeNull {
// 		return nil
// 	}
// 	isUnsigned := field.Flag & mysql.UNSIGNED_FLAG != 0

// 	switch field.Type {
// 	case mysql.MYSQL_TYPE_NULL:
// 		return nil

// 	case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_YEAR,
// 	     mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG:
// 		if isUnsigned {
// 			return uint32(value.AsUint64())
// 		} else {
// 			return int32(value.AsInt64())
// 		}

// 	case mysql.MYSQL_TYPE_LONGLONG:
// 		if isUnsigned {
// 			return value.AsUint64()
// 		} else {
// 			return value.AsInt64()
// 		}

// 	case mysql.MYSQL_TYPE_FLOAT:
// 		return float32(value.AsFloat64())

// 	case mysql.MYSQL_TYPE_DOUBLE:
// 		return value.AsFloat64()

// 	case mysql.MYSQL_TYPE_NEWDECIMAL:
// 		return MustParseInt(string(value.AsString()))

// 	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_TINY_BLOB, mysql.MYSQL_TYPE_MEDIUM_BLOB,
// 	     mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING,
// 			 mysql.MYSQL_TYPE_BIT, mysql.MYSQL_TYPE_ENUM, mysql.MYSQL_TYPE_SET, mysql.MYSQL_TYPE_GEOMETRY,
// 			 mysql.MYSQL_TYPE_JSON:
// 		return string(value.AsString())

// 	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE, mysql.MYSQL_TYPE_TIME:
// 		return uint32(value.AsUint64())

// 	case mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_DATETIME:
// 		return value.AsUint64()

// 	default:
// 		panic(fmt.Errorf("Unknown MySQL type: %d", field.Type))
// 	}
// }
