package logging

import (
	"io"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger is a logger implementation that wraps go-observation/logging
type Logger struct {
	logger *zap.SugaredLogger
	level  *zap.AtomicLevel
}

// With includes the specified fields with each log message sent by this logger
func (l *Logger) With(fields ...interface{}) *Logger {
	if len(fields) == 0 {
		return l
	}
	return &Logger{
		logger: l.logger.With(fields...),
		level:  l.level,
	}
}

// WithError includes the specified error as a field with each log message sent by this logger
func (l *Logger) WithError(err error) *Logger {
	return l.With(ErrorKey, err)
}

// WithComponent includes the specified component id as a field with each log message sent by this logger
func (l *Logger) WithComponent(id string) *Logger {
	return l.With(ComponentKey, id)
}

// SetLevel sets the log level for this logger
func (l *Logger) SetLevel(lvl Level) *Logger {
	l.level.SetLevel(zapcore.Level(lvl))
	return l
}

// SetLevelFromString sets the log level for this logger
func (l *Logger) SetLevelFromString(lvlStr string) *Logger {
	if lvl, err := ParseLevel(lvlStr); err != nil {
		l.logger.Warn("failed to parse log level, ignored", "level", lvlStr, ErrorKey, err)
	} else {
		l.SetLevel(lvl)
	}
	return l
}

// SetAsGlobal sets this logger as the global logger
func (l *Logger) SetAsGlobal() {
	SetGlobalLogger(l)
}

// Flush ensures that all buffered messages are written. Normally it only needs to be called
// before program exit.
func (l *Logger) Flush() error {
	return l.logger.Sync()
}

// Debug logs a message at DebugLevel
func (l *Logger) Debug(msg string, fields ...interface{}) {
	l.logger.Debugw(msg, fields...)
}

// Info logs a message at InfoLevel
func (l *Logger) Info(msg string, fields ...interface{}) {
	l.logger.Infow(msg, fields...)
}

// Error logs a message at ErrorLevel. err is traced as {"error": err.Error()}
func (l *Logger) Error(err error, msg string, fields ...interface{}) {
	fields = append(fields, ErrorKey, err)
	l.logger.Errorw(msg, fields...)
}

// Warn logs a message at WarnLevel
func (l *Logger) Warn(msg string, fields ...interface{}) {
	l.logger.Warnw(msg, fields...)
}

// Fatal logs a message at FatalLevel and then calls os.Exit(1). err is traced
// as {"error": err.Error()}
func (l *Logger) Fatal(err error, msg string, fields ...interface{}) {
	fields = append(fields, ErrorKey, err)
	l.logger.Fatalw(msg, fields...)
}

// New constructs a new logger using the default stdout for output.
// The serviceName argument will be traced as the standard "service"
// field on every trace.
func New(serviceName string) *Logger {
	return NewWithOutput(serviceName, os.Stdout)
}

// NewWithOutput constructs a new logger and writes output to writer.
// writer is an io.writer that also supports concurrent writes.
// The writer will be wrapped with zapcore.AddSync()
func NewWithOutput(serviceName string, writer io.Writer) *Logger {
	encoderCfg := zapcore.EncoderConfig{
		MessageKey:     MessageKey,
		LevelKey:       LevelKey,
		NameKey:        "logger",
		TimeKey:        TimeKey,
		StacktraceKey:  CallstackKey,
		CallerKey:      LocationKey,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     utcTimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	atomicLevel := zap.NewAtomicLevelAt(zapcore.InfoLevel)
	stacktrace := zap.AddStacktrace(zap.FatalLevel)
	w := zapcore.Lock(zapcore.AddSync(writer))
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoderCfg), w, atomicLevel)
	hostname, _ := os.Hostname()
	requiredFields := zap.Fields(
		zap.String(ServiceKey, serviceName),
		zap.String(HostnameKey, hostname))
	logger := zap.New(core, requiredFields, stacktrace, zap.AddCaller(), zap.AddCallerSkip(1))
	return &Logger{
		logger: logger.Sugar(),
		level:  &atomicLevel,
	}
}

// NewNoOp returns a no-op Logger that doesn't emit any logs. This is
// the default global logger.
func NewNoOp() *Logger {
	// Does not matter what level as this is NoOp.
	atomicLevel := zap.NewAtomicLevelAt(zapcore.InfoLevel)
	logger := zap.New(nil)
	return &Logger{
		logger: logger.Sugar(),
		level:  &atomicLevel,
	}
}

// utcTimeEncoder encodes the time as a UTC ISO8601 timestamp
func utcTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.UTC().Format("2006-01-02T15:04:05.000Z"))
}
