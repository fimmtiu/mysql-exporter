package main

import (
	"fmt"
	"os"
	"sync"
)

type RowsEvent struct {
	ResponseChan chan error
	Schema *TableSchema
	Data []any   // Instances of Schema.goType, which isn't defined at compile time.
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
		select {
		case rows := <-writer.RowChan:
			// FIXME: write the actual data
			_, err := writer.File.WriteString("this is a row\n")
			rows.ResponseChan <- err
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
