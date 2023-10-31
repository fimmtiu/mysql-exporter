package main

import (
	"fmt"
	"log"
	"os"
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

// Common code for initializing tests.
func init() {
	logger = log.New(os.Stdout, "", log.Ldate | log.Ltime)
	config = NewConfig()
	// mysqlController = NewAsyncController()

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

	StartShutdownMonitor()

	logger.Print("Exited.")
}
