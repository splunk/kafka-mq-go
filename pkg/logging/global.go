package logging

var (
	globalLogger = NewNoOp()
)

// Global returns the global logger. The default logger
// is a no-op logger.
func Global() *Logger {
	return globalLogger
}

// SetGlobalLogger sets the global logger. By default it is
// a no-op logger. Passing nil will panic.
func SetGlobalLogger(l *Logger) {
	if l == nil {
		panic("The global logger can not be nil")
	}
	globalLogger = l
}

// NewGlobal creates a new global logger, which has INFO as its default log level.
func NewGlobal(name string) *Logger {
	logger := New(name)
	logger.SetLevel(InfoLevel)
	SetGlobalLogger(logger)
	return logger
}

// These shortcut functions are meant to help you to use the global logger a bit
// easier, so instead of writting `logging.Global().Xxx()` you can just write
// `logging.Xxx()`.

// WithComponent includes the specified component id as a field with each log message sent by this logger
func WithComponent(id string) *Logger {
	return Global().WithComponent(id)
}

// WithError includes the specified error with each log message sent by this logger
func WithError(err error) *Logger {
	return Global().WithError(err)
}

// With includes the specified fields with each log message sent by this logger
func With(fields ...interface{}) *Logger {
	return Global().With(fields...)
}
