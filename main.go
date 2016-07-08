package main

import (
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/martinhoefling/saltbeat/beater"
	"os"
)

func main() {
	logp.Debug("main", "Starting saltbeat")
	err := beat.Run("saltbeat", "", beater.New())
	if err != nil {
		os.Exit(1)
	}
}
