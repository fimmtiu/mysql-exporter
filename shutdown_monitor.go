// Listens for INT and TERM signals, then tells the various components of the program to gracefully die.
// Listens for USR1 signals, then panics. (For testing the panic behaviour.)
//
// FIXME: Should this just go into main.go? Probably.

package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

type ShutdownMonitor struct {
	terminateChannel chan os.Signal
	panicChannel chan os.Signal
}

func StartShutdownMonitor() {
	monitor := &ShutdownMonitor{make(chan os.Signal), make(chan os.Signal)}
	signal.Notify(monitor.terminateChannel, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(monitor.panicChannel, syscall.SIGUSR1)

	go func() {
		select {
		case sig := <-monitor.terminateChannel:
			message := fmt.Sprintf("Received %s signal (%d).", sig.String(), sig)
			logger.Print(message)

			// Graceful shutdown:

			// if snapshot != nil {
			// 	snapshot.Exit()
			// }
			// if binlogReader != nil {
			// 	binlogReader.Exit()
			// }

			// FIXME: Close all open MySQL connections
			// FIXME: Close all open Redis connections
			return

		case <-monitor.panicChannel:
			panic("OH GOD WE'RE BONED LET'S FREAK OUT")
		}
	}()
}
