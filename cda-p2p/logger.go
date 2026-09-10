package p2pcommon

import (
	"log"
	"os"
	"strings"
	"sync"
)

// LogLevel represents the logging severity level.
type LogLevel int

const (
	LevelTrace LogLevel = iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
)

var (
	currentLogLevel = LevelInfo
	logLevelMu      sync.RWMutex
)

func init() {
	lvl := strings.ToUpper(strings.TrimSpace(os.Getenv("CDA_LOG_LEVEL")))
	switch lvl {
	case "TRACE":
		currentLogLevel = LevelTrace
	case "DEBUG":
		currentLogLevel = LevelDebug
	case "WARN", "WARNING":
		currentLogLevel = LevelWarn
	case "ERROR":
		currentLogLevel = LevelError
	default:
		currentLogLevel = LevelInfo
	}
}

// SetLogLevel allows setting the global log level programmatically.
func SetLogLevel(lvl LogLevel) {
	logLevelMu.Lock()
	defer logLevelMu.Unlock()
	currentLogLevel = lvl
}

// GetLogLevel returns the current global log level.
func GetLogLevel() LogLevel {
	logLevelMu.RLock()
	defer logLevelMu.RUnlock()
	return currentLogLevel
}

// IsTrace checks whether trace-level logging is enabled.
func IsTrace() bool {
	logLevelMu.RLock()
	defer logLevelMu.RUnlock()
	return currentLogLevel <= LevelTrace
}

// IsDebug checks whether debug-level logging is enabled.
func IsDebug() bool {
	logLevelMu.RLock()
	defer logLevelMu.RUnlock()
	return currentLogLevel <= LevelDebug
}

// LogTrace logs a formatted message if the current level is TRACE.
func LogTrace(format string, args ...any) {
	if IsTrace() {
		log.Printf("[TRACE] "+format, args...)
	}
}

// LogDebug logs a formatted message if the current level is DEBUG or lower.
func LogDebug(format string, args ...any) {
	if IsDebug() {
		log.Printf(format, args...)
	}
}

// LogInfo logs a formatted message if the current level is INFO or lower.
func LogInfo(format string, args ...any) {
	logLevelMu.RLock()
	lvl := currentLogLevel
	logLevelMu.RUnlock()
	if lvl <= LevelInfo {
		log.Printf(format, args...)
	}
}

// LogWarn logs a formatted message if the current level is WARN or lower.
func LogWarn(format string, args ...any) {
	logLevelMu.RLock()
	lvl := currentLogLevel
	logLevelMu.RUnlock()
	if lvl <= LevelWarn {
		log.Printf(format, args...)
	}
}

// LogError logs a formatted message if the current level is ERROR or lower.
func LogError(format string, args ...any) {
	log.Printf(format, args...)
}
