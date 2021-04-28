package logging

import (
	"go.uber.org/zap"
)

func (l *Logger) Logger() *zap.SugaredLogger {
	return l.logger
}
