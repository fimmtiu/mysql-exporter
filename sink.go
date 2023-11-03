package main

import (
	"fmt"
	"math/big"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"
)

type RowsEvent struct {
	ResponseChan chan error
	Schema *TableSchema
	Data [][]any
}

type Sink interface {
	Open(ts *TableSchema) error
	Close(ts *TableSchema) error
	WriteRows(ts *TableSchema, rows RowsEvent)
	SchemaChange(newSchema *TableSchema)
	Exit() error
}

// Writes tables to local CSV files. Not for actual use; we only use this in the tests.
type CsvSink struct {
	Lock sync.Mutex
	RowsChan chan RowsEvent
	Workers *WorkerGroup
	Writers map[*TableSchema]*CsvWriter
}

func NewCsvSink() *CsvSink {
	return &CsvSink{
		sync.Mutex{},
		make(chan RowsEvent),
		NewWorkerGroup(),
		make(map[*TableSchema]*CsvWriter),
	}
}

func (sink *CsvSink) Open(ts *TableSchema) error {
	writer, err := NewCsvWriter(ts, sink.Workers)
	if err == nil {
		sink.Writers[ts] = writer
		sink.Workers.Go(writer.Run)
	}
	return err
}

func (sink *CsvSink) Close(ts *TableSchema) error {
	writer := sink.Writers[ts]
	delete(sink.Writers, ts)
	return writer.Exit()
}

func (sink *CsvSink) WriteRow(ts *TableSchema, rows RowsEvent) {
	writer := sink.Writers[ts]
	writer.RowChan <- rows
}

func (sink *CsvSink) Exit() error {
	sink.Workers.Exit(nil)
	return sink.Workers.Wait()
}

type CsvWriter struct {
	RowChan chan RowsEvent
	ExitChan chan error
	WorkerGroup *WorkerGroup
	Schema *TableSchema
	File *os.File
}

func NewCsvWriter(ts *TableSchema, workerGroup *WorkerGroup) (*CsvWriter, error) {
	err := os.MkdirAll(fmt.Sprintf("/tmp/%d", os.Getpid()), 0755)
	if err != nil {
		return nil, err
	}

	filename := fmt.Sprintf("/tmp/%d/%s.csv", os.Getpid(), ts.Name)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	header := ""
	for i, column := range ts.Columns {
		if i > 0 {
			header += ","
		}
		header += column.Name
	}
	_, err = file.WriteString(header + "\n")
	if err != nil {
		return nil, err
	}

	return &CsvWriter{make(chan RowsEvent), make(chan error), workerGroup, ts, file}, nil
}

func (writer *CsvWriter) Run() error {
	for {
		redo: select {
		case rows := <-writer.RowChan:
			for _, row := range rows.Data {
				line := ""
				for i, column := range writer.Schema.Columns {
					if i > 0 {
						line += ","
					}
					line += formatDatumForCsv(row[i], column)
				}
				_, err := writer.File.WriteString(line + "\n")
				if err != nil {
					rows.ResponseChan <- err
					break redo
				}
			}
			rows.ResponseChan <- nil

		case <-writer.ExitChan:
			err := writer.File.Close()
			writer.ExitChan <- err
			return err

		case <-writer.WorkerGroup.ExitSignal():
			return writer.File.Close()
		}
	}
}

// Synchronous exit: signal it to close and wait for the response.
func (writer *CsvWriter) Exit() error {
	writer.ExitChan <- nil
	return <-writer.ExitChan
}

var needsQuotesRegexp = regexp.MustCompile(`[,"]`)
func formatDatumForCsv(datum any, column Column) string {
	if datum == nil {
		return ""
	}

	switch reflect.TypeOf(datum).Kind() {
	case reflect.String:
		s := datum.(string)
		if needsQuotesRegexp.MatchString(s) {
			s = `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
		}
		return s
	case reflect.Int8:   return fmt.Sprintf("%d", datum.(int8))
	case reflect.Uint8:  return fmt.Sprintf("%d", datum.(uint8))
	case reflect.Int16:  return fmt.Sprintf("%d", datum.(int16))
	case reflect.Uint16: return fmt.Sprintf("%d", datum.(uint16))
	case reflect.Uint32: return fmt.Sprintf("%d", datum.(uint32))
	case reflect.Int64:  return fmt.Sprintf("%d", datum.(int64))
	case reflect.Uint64: return fmt.Sprintf("%d", datum.(uint64))

	case reflect.Int32:
		switch column.SqlType {
		case "date": return `"` + FormatEpochDate(datum.(int32)) + `"`
		case "time": return `"` + FormatMillisecondTime(datum.(int32)) + `"`
		default: return fmt.Sprintf("%d", datum.(int32))
		}

	case reflect.TypeOf(time.Time{}).Kind():
		t := datum.(time.Time)
		return `"` + t.Format("2006-01-02 15:04:05") + `"`

	case reflect.TypeOf(big.Int{}).Kind():
		decimal := datum.(big.Int)
		return fmt.Sprintf("%s", decimal.String())

	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%f", datum)

	default:
		panic(fmt.Sprintf("Unexpected type for CSV: '%v'", reflect.TypeOf(datum)))
	}
}
