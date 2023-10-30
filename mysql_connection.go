package main

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/client"
)

type IMysqlClient interface {
	Execute(query string, args ...interface{}) (IMysqlResult, error)
	Close() error
}

type IMysqlResult interface {
	GetString(row, column int) (string, error)
	GetInt(row, column int) (int64, error)
	RowNumber() int
}

// This tiny wrapper is just to make the client.Conn conform to the IMysqlClient interface.
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

// FIXME: Use connection pooling to limit the number of open connections.
// https://github.com/go-mysql-org/go-mysql#example-for-connection-pool-v130
func NewMysqlConnection() *MysqlConnection {
	hostport := fmt.Sprintf("%s:%s", config.MysqlHost, config.MysqlPort)
	conn, err := client.Connect(hostport, config.MysqlUser, config.MysqlPassword, config.MysqlDatabase)
	if err != nil {
		panic(fmt.Errorf("Can't connect to database '%s': %s", hostport, err))
	}
	return &MysqlConnection{ClientWrapper{conn}}
}

func (conn *MysqlConnection) Execute(query string, args ...interface{}) (IMysqlResult, error) {
	return conn.client.Execute(query, args...)
}

func (conn *MysqlConnection) Close() error {
	return conn.client.Close()
}

// Returns a uint64 representing the current position of the MySQL server's binary logs, a string
// containing the executed GTID set, and an error if one occurred. The uint64 will be a combination
// of the binary log filename and position, guaranteed to monotonically increase.
func (conn *MysqlConnection) GetBinlogPosition() (uint64, string, error) {
	_, err := conn.client.Execute("FLUSH TABLES WITH READ LOCK")
	if err != nil {
		return 0, "", fmt.Errorf("Can't execute FLUSH TABLES: %s", err)
	}
	defer func() { conn.client.Execute("UNLOCK TABLES")	}()

	rows, err := conn.client.Execute("SHOW MASTER STATUS")
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

// The "binlog position" is the binary log's index number in the high 32 bits and the position within that log
// in the low 32 bits.
func ParseBinlogPosition(file string, pos int64) uint64 {
	dotIndex := strings.Index(file, ".")
	return uint64(MustParseInt(file[dotIndex+1:]))<<32 | uint64(pos)
}


// Returns true if there exist GTIDs which mysql-exporter has not processed which have expired
// from the server's binary logs.
func (conn *MysqlConnection) DoPurgedGtidsExist(previousGtids, currentGtids string) (bool, error) {
	sql := fmt.Sprintf(`SELECT GTID_SUBSET(GTID_SUBTRACT("%s", "%s"), "%s")`, currentGtids, previousGtids, currentGtids)
	rows, err := conn.client.Execute(sql)
	if err != nil {
		return false, fmt.Errorf("Can't execute SELECT GTID_SUBSET: %s", err)
	}

	result, err := rows.GetInt(0, 0)
	if err != nil {
		return false, fmt.Errorf("Can't retrieve result from SELECT GTID_SUBSET: %s", err)
	}
	return result == 0, nil
}
