package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/client"
)

const MIN_MYSQL_CONNS = 2

type IMysqlPool interface {
	Execute(query string, args ...interface{}) (IMysqlResult, error)
	GetConn(ctx context.Context) (IMysqlClient, error)
	PutConn(conn IMysqlClient)
}

type IMysqlClient interface {
	Execute(query string, args ...interface{}) (IMysqlResult, error)
	Close() error
}

type IMysqlResult interface {
	GetString(row, column int) (string, error)
	GetInt(row, column int) (int64, error)
	RowNumber() int
}

// This tiny wrapper is just to make client.Pool conform to the IMysqlPool interface.
type PoolWrapper struct {
	pool *client.Pool
}

func (pw PoolWrapper) Execute(query string, args ...interface{}) (IMysqlResult, error) {
	conn, err := pw.pool.GetConn(context.Background())
	if err != nil {
		return nil, err
	}
	defer pw.pool.PutConn(conn)

	return conn.Execute(query, args...)
}

func (pw PoolWrapper) GetConn(ctx context.Context) (IMysqlClient, error) {
	conn, err := pw.pool.GetConn(ctx)
	return ClientWrapper{conn}, err
}

func (pw PoolWrapper) PutConn(conn IMysqlClient) {
	pw.pool.PutConn(conn.(ClientWrapper).conn)
}

// This tiny wrapper is just to make client.Conn conform to the IMysqlClient interface.
type ClientWrapper struct {
	conn *client.Conn
}

func (cw ClientWrapper) Execute(query string, args ...interface{}) (IMysqlResult, error) {
	return cw.conn.Execute(query, args...)
}

func (cw ClientWrapper) Close() error {
	return cw.conn.Close()
}

type MysqlConnection struct {
	client IMysqlClient
}

func init() {
	if !InTestMode() {
		hostport := fmt.Sprintf("%s:%s", config.MysqlHost, config.MysqlPort)
		pool = PoolWrapper{
			client.NewPool(
				logger.Printf, MIN_MYSQL_CONNS, config.MaxMysqlConns, MIN_MYSQL_CONNS,
				hostport, config.MysqlUser, config.MysqlPassword, config.MysqlDatabase,
			),
		}
	}
}

// Returns a uint64 representing the current position of the MySQL server's binary logs, a string
// containing the executed GTID set, and an error if one occurred. The uint64 will be a combination
// of the binary log filename and position, guaranteed to monotonically increase.
func GetBinlogPosition() (uint64, string, error) {
	_, err := pool.Execute("FLUSH TABLES WITH READ LOCK")
	if err != nil {
		return 0, "", fmt.Errorf("Can't execute FLUSH TABLES: %s", err)
	}
	defer func() { pool.Execute("UNLOCK TABLES") }()

	rows, err := pool.Execute("SHOW MASTER STATUS")
	if err != nil {
		return 0, "", fmt.Errorf("Can't execute SHOW MASTER STATUS: %s", err)
	}

	file, err := rows.GetString(0, 0)
	if err != nil {
		return 0, "", fmt.Errorf("Can't retrieve filename from SHOW MASTER STATUS: %s", err)
	}
	pos, err := rows.GetInt(0, 1)
	if err != nil {
		return 0, "", fmt.Errorf("Can't retrieve position from SHOW MASTER STATUS: %s", err)
	}
	gtidset, err := rows.GetString(0, 4)
	if err != nil {
		return 0, "", fmt.Errorf("Can't retrieve gtid_executed from SHOW MASTER STATUS: %s", err)
	}

	return ParseBinlogPosition(file, pos), gtidset, nil
}

// The "binlog position" is the binary log's index number in the high 24 bits and the position within that log
// in the low 40 bits.
func ParseBinlogPosition(file string, pos int64) uint64 {
	dotIndex := strings.Index(file, ".")
	return uint64(MustParseInt(file[dotIndex+1:]))<<40 | uint64(pos)
}


// Returns true if there exist GTIDs which mysql-exporter has not processed which have expired
// from the server's binary logs.
func DoPurgedGtidsExist(previousGtids, currentGtids string) (bool, error) {
	sql := fmt.Sprintf(`SELECT GTID_SUBSET(GTID_SUBTRACT("%s", "%s"), "%s")`, currentGtids, previousGtids, currentGtids)
	rows, err := pool.Execute(sql)
	if err != nil {
		return false, fmt.Errorf("Can't execute SELECT GTID_SUBSET: %s", err)
	}

	result, err := rows.GetInt(0, 0)
	if err != nil {
		return false, fmt.Errorf("Can't retrieve result from SELECT GTID_SUBSET: %s", err)
	}
	return result == 0, nil
}

func ListTables() ([]string, error) {
	rows, err := pool.Execute("SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("Can't execute SHOW TABLES: %s", err)
	}

	tables := make([]string, rows.RowNumber())
	for i := 0; i < rows.RowNumber(); i++ {
		tables[i], err = rows.GetString(i, 0)
		if err != nil {
			return nil, fmt.Errorf("Can't retrieve table name from SHOW TABLES: %s", err)
		}
	}

	for i := 0; i < len(tables); i++ {
		if StringInList(tables[i], config.ExcludeTables) {
			tables = DeleteFromSlice(tables, i)
			i--
		}
	}

	return tables, nil
}

func GetTableSchema(tableName string) (*TableSchema, error) {
	rows, err := pool.Execute("SHOW CREATE TABLE `" + tableName + "`")
	if err != nil {
		return nil, fmt.Errorf("Can't execute SHOW CREATE TABLE: %s", err)
	}

	createTable, err := rows.GetString(0, 1)
	if err != nil {
		return nil, fmt.Errorf("Can't fetch CREATE TABLE column: %s", err)
	}
	return ParseSchema(createTable), nil
}
