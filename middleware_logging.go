package workers

import (
	"fmt"
	"log/slog"
	"runtime"
	"time"
)

// LogMiddleware is the default logging middleware
func LogMiddleware(queue string, mgr *Manager, next JobFunc) JobFunc {
	return func(message *Msg) (err error) {
		prefix := fmt.Sprint(queue, " JID-", message.Jid())

		start := time.Now()
		mgr.logger.Info("job start", "prefix", prefix)
		mgr.logger.Debug("job args", "prefix", prefix, "args", message.Args().ToJson())

		defer func() {
			if e := recover(); e != nil {
				var ok bool
				if err, ok = e.(error); !ok {
					err = fmt.Errorf("%v", e)
				}

				if err != nil {
					logProcessError(mgr.logger, prefix, start, err)
				}
			}

		}()

		err = next(message)
		if err != nil {
			logProcessError(mgr.logger, prefix, start, err)
		} else {
			mgr.logger.Info("job done", "prefix", prefix, "duration", time.Since(start))
		}

		return
	}

}

func logProcessError(logger *slog.Logger, prefix string, start time.Time, err error) {
	buf := make([]byte, 4096)
	buf = buf[:runtime.Stack(buf, false)]
	logger.Error("job fail",
		"prefix", prefix,
		"duration", time.Since(start),
		"error", err,
		"stack", string(buf),
	)
}
