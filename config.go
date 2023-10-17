// A simple global object for storing configuration settings.

package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const DEFAULT_SNAPSHOT_CHUNK_SIZE = 100_000
const DEFAULT_MAX_TIME_PER_BATCH = 20 * time.Minute
const DEFAULT_DATADOG_HOST = "127.0.0.1"
const DEFAULT_DATADOG_PORT = "8125"
const DEFAULT_MYSQL_PORT = "3306"
const DEFAULT_REDIS_PORT = "6379"

type Config struct {
	MysqlHost string
	MysqlDatabase string
	MysqlUser string
	MysqlPassword string
	MysqlPort string

	RedisHost string
	RedisPort string
	RedisPassword string

	S3Path string

	ExcludeTables []string

	SnapshotChunkSize uint64
	MaxTimePerBatch time.Duration

	DatadogHost string
	DatadogPort string

	BugsnagApiKey string
	BugsnagReleaseStage string
	ClioRegion string

	SyntheticColumns []string
	SyntheticColumnTypes map[string]string
	SyntheticColumnNames string
	SyntheticColumnValues string
}

func NewConfig() Config {
	var snapshotChunkSize int64
	var err error
	datadogHost := DEFAULT_DATADOG_HOST
	datadogPort := DEFAULT_DATADOG_PORT
	bugsnagReleaseStage := "production"
	mysqlPort := DEFAULT_MYSQL_PORT
	redisPort := DEFAULT_REDIS_PORT
	excludeTables := []string{}

	value, found := os.LookupEnv("MYSQL_PORT")
	if found {
		mysqlPort = value
	}

	value, found = os.LookupEnv("REDIS_PORT")
	if found {
		redisPort = value
	}

	value, found = os.LookupEnv("SNAPSHOT_CHUNK_SIZE")
	if !found {
		snapshotChunkSize = DEFAULT_SNAPSHOT_CHUNK_SIZE
	} else {
		snapshotChunkSize, err = strconv.ParseInt(value, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("Bogus value for SNAPSHOT_CHUNK_SIZE: '%s'", value))
		}
	}

	value, found = os.LookupEnv("EXCLUDE_TABLES")
	if found {
		excludeTables = strings.Split(value, ",")
	}

	value, found = os.LookupEnv("DATADOG_HOST")
	if found {
		datadogHost = value
	}
	value, found = os.LookupEnv("DATADOG_PORT")
	if found {
		datadogPort = value
	}
	value, found = os.LookupEnv("BUGSNAG_RELEASE_STAGE")
	if found {
		bugsnagReleaseStage = value
	}

	c := Config{
		MysqlHost: os.Getenv("MYSQL_HOST"),
		MysqlDatabase: os.Getenv("MYSQL_DATABASE"),
		MysqlUser: os.Getenv("MYSQL_USER"),
		MysqlPassword: os.Getenv("MYSQL_PASSWORD"),
		MysqlPort: mysqlPort,

		RedisHost: os.Getenv("REDIS_HOST"),
		RedisPassword: os.Getenv("REDIS_PASSWORD"),
		RedisPort: redisPort,

		S3Path: os.Getenv("S3_PATH"),

		ExcludeTables: excludeTables,

		SnapshotChunkSize: uint64(snapshotChunkSize),

		DatadogHost: datadogHost,
		DatadogPort: datadogPort,

		BugsnagApiKey: os.Getenv("BUGSNAG_API_KEY"),
		BugsnagReleaseStage: bugsnagReleaseStage,
		ClioRegion: os.Getenv("CLIO_REGION"),

		SyntheticColumns: []string{},
		SyntheticColumnTypes: map[string]string{},
		SyntheticColumnNames: "",
		SyntheticColumnValues: "",
	}

	value, found = os.LookupEnv("SYNTHETIC_COLUMNS")
	if found {
		c.parseSyntheticColumns(value)
	}
	return c
}

// Parse an environment variable like:
//   SYNTHETIC_COLUMNS="foo,int,1;bar,char(4),'honk'"
// into a Go data structure like:
//   map[string]string{"foo": "int", "bar": "char(4)"}
// and the strings "foo, bar" and "1, 'honk'". The data type will be used to determine the MySQL schema, and the
// strings will get injected into all of our Parquet output.
func (c *Config) parseSyntheticColumns(raw string) {
	columns := strings.Split(raw, ";")
	c.SyntheticColumnTypes = make(map[string]string, len(columns))
	c.SyntheticColumns = make([]string, 0, len(columns))

	for i, column := range columns {
		parts := strings.SplitN(column, ",", 3)
		if len(parts) < 3 {
			logger.Fatal("Malformed SYNTHETIC_COLUMNS string!")
		}

		c.SyntheticColumns = append(c.SyntheticColumns, parts[0])
		c.SyntheticColumnTypes[parts[0]] = parts[1]
		c.SyntheticColumnNames += parts[0]
		c.SyntheticColumnValues += parts[2]
		if i < len(columns) - 1 {
			c.SyntheticColumnNames += ","
			c.SyntheticColumnValues += ","
		}
	}
}
