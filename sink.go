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

type SchemaChangeEvent struct {
	ResponseChan chan error
	NewSchema *TableSchema
}

type Sink interface {
	Open(ts *TableSchema) error
	Close(ts *TableSchema) error
	WriteRows(rows RowsEvent)
	SchemaChange(newSchema *TableSchema) error
	Exit() error
}

// Writes tables to local CSV files. Not for actual use; we only use this in the tests.
type CsvSink struct {
	Lock sync.Mutex
	RowsChan chan RowsEvent
	Workers *WorkerGroup
	Writers map[string]*CsvWriter
}

func NewCsvSink() *CsvSink {
	return &CsvSink{
		sync.Mutex{},
		make(chan RowsEvent),
		NewWorkerGroup(),
		make(map[string]*CsvWriter),
	}
}

func (sink *CsvSink) Open(ts *TableSchema) error {
	writer, err := NewCsvWriter(ts, sink.Workers)
	if err == nil {
		sink.Writers[ts.Name] = writer
		sink.Workers.Go(writer.Run)
	}
	return err
}

func (sink *CsvSink) Close(ts *TableSchema) error {
	writer, ok := sink.Writers[ts.Name]
	if !ok {
		panic(fmt.Errorf("Can't close non-existent writer for table '%s'!", ts.Name))
	}
	delete(sink.Writers, ts.Name)
	return writer.Exit()
}

func (sink *CsvSink) WriteRows(rows RowsEvent) {
	writer, ok := sink.Writers[rows.Schema.Name]
	if !ok {
		panic(fmt.Errorf("Can't find writer for table '%s'!", rows.Schema.Name))
	}
	writer.RowChan <- rows
}

func (sink *CsvSink) SchemaChange(newSchema *TableSchema) error {
	writer := sink.Writers[newSchema.Name]
	delete(sink.Writers, newSchema.Name)
	sink.Writers[newSchema.Name] = writer

	change := SchemaChangeEvent{make(chan error), newSchema}
	writer.SchemaChangeChan <- change
	return <-change.ResponseChan
}

func (sink *CsvSink) Exit() error {
	sink.Workers.Exit(nil)
	return sink.Workers.Wait()
}

type CsvWriter struct {
	RowChan chan RowsEvent
	SchemaChangeChan chan SchemaChangeEvent
	ExitChan chan error
	WorkerGroup *WorkerGroup
	Schema *TableSchema
	File *os.File
	SchemaVersion int
}

func openCsvFile(ts *TableSchema, version int) (*os.File, error) {
	err := os.MkdirAll(fmt.Sprintf("/tmp/%d", os.Getpid()), 0755)
	if err != nil {
		return nil, err
	}

	filename := fmt.Sprintf("/tmp/%d/%s_%d.csv", os.Getpid(), ts.Name, version)
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
	return file, nil
}

func NewCsvWriter(ts *TableSchema, workerGroup *WorkerGroup) (*CsvWriter, error) {
	file, err := openCsvFile(ts, 1)
	if err != nil {
		return nil, err
	}
	return &CsvWriter{
		make(chan RowsEvent), make(chan SchemaChangeEvent), make(chan error),
		workerGroup, ts, file, 1,
	}, nil
}

func (writer *CsvWriter) Run() error {
	var err error

	loop: for {
		select {
		case rows := <-writer.RowChan:
			for _, row := range rows.Data {
 				line := ""
				for i, column := range writer.Schema.Columns {
					if i > 0 {
						line += ","
					}
					line += convertToCsvString(row[i], column)
				}
				fmt.Printf("Wrote row to %s\n", writer.File.Name())
				_, err = writer.File.WriteString(line + "\n")
				if err != nil {
					rows.ResponseChan <- err
					continue loop
				}
			}
			rows.ResponseChan <- nil

		case <-writer.ExitChan:
			err = writer.File.Close()
			writer.ExitChan <- err
			fmt.Printf("CsvWriter exited with ExitChan: %s\n", err)
			return err

		case change := <-writer.SchemaChangeChan:
			writer.SchemaVersion++
			writer.Schema = change.NewSchema
			if err = writer.File.Close(); err != nil {
				change.ResponseChan <- err
				fmt.Printf("CsvWriter exited with SchemaChange error: %s\n", err)
				return err
			}
			writer.File, err = openCsvFile(writer.Schema, writer.SchemaVersion)
			if err != nil {
				change.ResponseChan <- err
				fmt.Printf("CsvWriter exited with SchemaChange error: %s\n", err)
				return err
			}
			change.ResponseChan <- nil

		case <-writer.WorkerGroup.ExitSignal():
			fmt.Printf("CsvWriter exited from Exitsignal\n")
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
func convertToCsvString(datum any, column Column) string {
	if datum == nil {
		return ""
	}

	switch datum.(type) {
	case string:
		s := datum.(string)
		if needsQuotesRegexp.MatchString(s) {
			s = `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
		}
		return s
	case []uint8:
		// Always quote binary data.
		return `"` + strings.ReplaceAll(string(datum.([]uint8)), `"`, `""`) + `"`

	case int8:   return fmt.Sprintf("%d", datum.(int8))
	case uint8:  return fmt.Sprintf("%d", datum.(uint8))
	case int16:  return fmt.Sprintf("%d", datum.(int16))
	case uint16: return fmt.Sprintf("%d", datum.(uint16))
	case int32:  return fmt.Sprintf("%d", datum.(int32))
	case uint32: return fmt.Sprintf("%d", datum.(uint32))
	case int64:  return fmt.Sprintf("%d", datum.(int64))
	case uint64: return fmt.Sprintf("%d", datum.(uint64))

	case time.Time:
		switch column.SqlType {
		case "datetime", "timestamp": return datum.(time.Time).Format("2006-01-02 15:04:05")
		case "date": return datum.(time.Time).Format("2006-01-02")
		case "time": return datum.(time.Time).Format("15:04:05")
		default: panic(fmt.Errorf("Unexpected time type for CSV: '%s'", column.SqlType))
		}

	// THE BIGGEST RAT YOU EVER SAW
	case *big.Rat:
		decimal := datum.(*big.Rat)
		return decimal.String()

	case float32, float64:
		return fmt.Sprintf("%f", datum)

	default:
		panic(fmt.Sprintf("Unexpected type for CSV: '%v'", reflect.TypeOf(datum)))
	}
}
