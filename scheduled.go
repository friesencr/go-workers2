package workers

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/friesencr/go-workers2/storage"
)

type scheduledWorker struct {
	opts   Options
	done   chan bool
	logger *slog.Logger
}

func (s *scheduledWorker) run() {
	for {
		select {
		case <-s.done:
			return
		default:
		}

		s.poll()

		time.Sleep(s.opts.PollInterval)
	}
}

func (s *scheduledWorker) quit() {
	close(s.done)
}

func (s *scheduledWorker) poll() {
	now := nowToSecondsWithNanoPrecision()

	for {
		rawMessage, err := s.opts.store.DequeueScheduledMessage(context.Background(), now)

		if err != nil {
			if err != storage.NoMessage {
				s.logger.Error("scheduled dequeue failed", "error", err)
			}
			break
		}

		message, err := NewMsg(rawMessage)
		if err != nil {
			s.logger.Error("scheduled message parse failed", "error", err)
			break
		}
		queue, _ := message.Get("queue").String()
		queue = strings.TrimPrefix(queue, s.opts.Namespace)
		message.Set("enqueued_at", nowToSecondsWithNanoPrecision())

		if err := s.opts.store.EnqueueMessageNow(context.Background(), queue, message.ToJson()); err != nil {
			s.logger.Error("scheduled enqueue failed", "queue", queue, "error", err)
			break
		}
	}

	for {
		rawMessage, err := s.opts.store.DequeueRetriedMessage(context.Background(), now)

		if err != nil {
			if err != storage.NoMessage {
				s.logger.Error("retry dequeue failed", "error", err)
			}
			break
		}

		message, err := NewMsg(rawMessage)
		if err != nil {
			s.logger.Error("retry message parse failed", "error", err)
			break
		}
		queue, _ := message.Get("queue").String()
		queue = strings.TrimPrefix(queue, s.opts.Namespace)
		message.Set("enqueued_at", nowToSecondsWithNanoPrecision())

		if err := s.opts.store.EnqueueMessageNow(context.Background(), queue, message.ToJson()); err != nil {
			s.logger.Error("retry enqueue failed", "queue", queue, "error", err)
			break
		}
	}
}

func newScheduledWorker(opts Options) *scheduledWorker {
	return &scheduledWorker{
		opts:   opts,
		done:   make(chan bool),
		logger: opts.Logger,
	}
}
