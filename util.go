package main

import (
	"os"
	"strconv"
	"strings"
	"time"
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

func FormatEpochDate(epochDate int32) string {
	seconds := int64(epochDate) * 24 * 60 * 60
	date := time.Unix(seconds, 0).In(UTC)
	return date.Format("2006-01-02")
}

func FormatMillisecondTime(millisSinceMidnight int32) string {
	epochTime := time.UnixMilli(int64(millisSinceMidnight)).In(UTC)
	return epochTime.Format("15:04:05")
}
