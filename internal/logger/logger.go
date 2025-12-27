package logger

import (
	"fmt"
	"log"
	"runtime"
	"strings"
)

// Level represents log severity.
type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

// Logger is a structured logger that emits key=value lines and
// automatically annotates entries with package, type, and method
// derived from the call stack.
type Logger struct {
	base  *log.Logger
	level Level
}

// New constructs a Logger around a standard library *log.Logger.
func New(base *log.Logger) *Logger {
	return &Logger{
		base:  base,
		level: InfoLevel,
	}
}

// SetLevel updates the minimum level for this logger.
func (l *Logger) SetLevel(level Level) {
	l.level = level
}

// Global is the shared logger instance for the entire process.
var Global = New(log.Default())

// SetGlobalLevel changes the level of the global logger.
func SetGlobalLevel(level Level) {
	Global.SetLevel(level)
}

// Shorthand helpers that delegate to the global logger.
// These are convenient for packages that just need logging
// without managing a logger instance.
func Debug(msg string, kv ...interface{}) { Global.Debug(msg, kv...) }
func Info(msg string, kv ...interface{})  { Global.Info(msg, kv...) }
func Warn(msg string, kv ...interface{})  { Global.Warn(msg, kv...) }
func Error(err error, msg string, kv ...interface{}) {
	Global.Error(err, msg, kv...)
}

// Printf/Println wrappers to ease migration from the standard log package.
// These log at INFO level with the formatted message as msg.
func Printf(format string, args ...interface{}) {
	Global.Info(fmt.Sprintf(format, args...))
}

func Println(args ...interface{}) {
	Global.Info(fmt.Sprint(args...))
}

// callerInfo extracts package, receiver type (if any), and method name
// from the runtime call stack.
//
// Example full name:
//
//	github.com/edkuperman/chronosched/internal/scheduler.(*Scheduler).tick
func callerInfo(skip int) (pkg, typ, method string) {
	pc, _, _, ok := runtime.Caller(skip)
	if !ok {
		return "unknown", "unknown", "unknown"
	}

	full := runtime.FuncForPC(pc).Name()

	parts := strings.Split(full, "/")
	last := parts[len(parts)-1]

	pkgAndRest := strings.SplitN(last, ".", 2)
	pkg = pkgAndRest[0]
	if len(pkgAndRest) == 1 {
		return pkg, "", ""
	}

	rest := pkgAndRest[1]

	// pointer receiver: (*Type).Method
	if strings.HasPrefix(rest, "(*") {
		idx := strings.Index(rest, ").")
		if idx != -1 {
			typ = rest[2:idx]
			method = rest[idx+2:]
			return
		}
	}

	// value receiver: (Type).Method
	if strings.HasPrefix(rest, "(") {
		idx := strings.Index(rest, ").")
		if idx != -1 {
			typ = rest[1:idx]
			method = rest[idx+2:]
			return
		}
	}

	// package-level function
	method = rest
	return
}

func formatKeyValue(kv []interface{}) string {
	if len(kv)%2 != 0 {
		return "invalid_key_value_pairs=1"
	}

	var b strings.Builder
	for i := 0; i < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(' ')
		}
		key := fmt.Sprintf("%v", kv[i])
		val := fmt.Sprintf("%v", kv[i+1])
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(val)
	}
	return b.String()
}

// log is the internal implementation for all severity-specific methods.
func (l *Logger) log(level Level, levelStr string, msg string, err error, kv ...interface{}) {
	if level < l.level {
		return
	}

	pkg, typ, method := callerInfo(4) // Debug/Info/Warn/Error -> log() -> caller

	var b strings.Builder
	b.WriteString("level=")
	b.WriteString(levelStr)

	b.WriteString(" pkg=")
	b.WriteString(pkg)

	b.WriteString(" type=")
	if typ == "" {
		b.WriteString("none")
	} else {
		b.WriteString(typ)
	}

	b.WriteString(" method=")
	b.WriteString(method)

	b.WriteString(" msg=")
	b.WriteString(fmt.Sprintf("%q", msg))

	if err != nil {
		b.WriteString(" error=")
		b.WriteString(fmt.Sprintf("%q", err.Error()))
	}

	kvString := formatKeyValue(kv)
	if kvString != "" {
		b.WriteByte(' ')
		b.WriteString(kvString)
	}

	l.base.Println(b.String())
}

// Debug logs at DEBUG level using the global logger.
func (l *Logger) Debug(msg string, kv ...interface{}) {
	l.log(DebugLevel, "DEBUG", msg, nil, kv...)
}

// Info logs at INFO level using the global logger.
func (l *Logger) Info(msg string, kv ...interface{}) {
	l.log(InfoLevel, "INFO", msg, nil, kv...)
}

// Warn logs at WARN level using the global logger.
func (l *Logger) Warn(msg string, kv ...interface{}) {
	l.log(WarnLevel, "WARN", msg, nil, kv...)
}

// Error logs at ERROR level using the global logger.
func (l *Logger) Error(err error, msg string, kv ...interface{}) {
	l.log(ErrorLevel, "ERROR", msg, err, kv...)
}
