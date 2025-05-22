package taskiq

import (
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Global logger instance
var Logger zerolog.Logger

func init() {
	logLevelStr := os.Getenv("TASKIQ_LOG_LEVEL")
	logLevel := zerolog.InfoLevel // Default to Info

	switch strings.ToLower(logLevelStr) {
	case "debug":
		logLevel = zerolog.DebugLevel
	case "warn":
		logLevel = zerolog.WarnLevel
	case "error":
		logLevel = zerolog.ErrorLevel
	case "fatal":
		logLevel = zerolog.FatalLevel
	case "panic":
		logLevel = zerolog.PanicLevel
	case "trace":
		logLevel = zerolog.TraceLevel
	}

	// Console writer for human-readable output, with color
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339, NoColor: false}
	
	// For structured logging (e.g., to a file or log management system), you might use:
	// zerolog.SetGlobalLevel(logLevel)
	// Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()

	Logger = zerolog.New(output).With().Timestamp().Logger().Level(logLevel)
	Logger.Info().Msg("Zerolog initialized")
}
