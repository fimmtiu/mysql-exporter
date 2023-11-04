package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/bugsnag/bugsnag-go"
)

// Thread-safe singletons shared throughout the entire app.
var logger *log.Logger
var config Config
var datadog *statsd.Client
var UTC *time.Location
var stateStorage StateStorage
var pool IMysqlPool
var snapshotter *Snapshotter
var sinks []Sink

// Common code for initializing tests.
func init() {
	logger = log.New(os.Stdout, "", log.Ldate | log.Ltime)
	config = NewConfig()
	// mysqlController = NewWorkerGroup()

	var err error
	UTC, err = time.LoadLocation("UTC")
	if err != nil {
		logger.Fatal(err)
	}
	stateStorage = NewStateStorage()
}

func main() {
	var err error
  datadog, err = statsd.New(fmt.Sprintf("%s:%s", config.DatadogHost, config.DatadogPort))
	if err != nil {
		logger.Fatal(err)
	}

	bugsnag.Configure(bugsnag.Configuration{
		APIKey:          config.BugsnagApiKey,
		ReleaseStage:    config.BugsnagReleaseStage,
		ProjectPackages: []string{"main"},
		NotifyReleaseStages: []string{"production", "staging"},
	})

	listenForSignals()

	snapshotter = NewSnapshotter()
	if snapshotter.Run() {
		snapshotter = nil
		// FIXME: Start binlog replay
	}

	logger.Print("Exited.")
}

// Sets up the signal handling: die gracefully on INT or TERM, panic on USR1.
func listenForSignals() {
	var terminateChannel chan os.Signal
	var panicChannel chan os.Signal

	signal.Notify(terminateChannel, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(panicChannel, syscall.SIGUSR1)

	go func() {
		select {
		case sig := <-terminateChannel:
			logger.Printf("Received %s signal (%d).", sig.String(), sig)
			gracefulShutdown()
		case <-panicChannel:
			panic("OH GOD WE'RE BONED LET'S FREAK OUT")   // For testing the panic behaviour.
		}
	}()
}

func gracefulShutdown() {
	if snapshotter != nil {
		snapshotter.Exit()
	}
	// FIXME: Stop binlog replay
}
