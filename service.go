package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/nats-io/nats.go"
)

type serviceEvent int

const (
	serviceEventBecameSubscriber serviceEvent = iota
	serviceEventClosed
	serviceJoinSubscriber
)

type service struct {
	lg   log.Logger
	name string

	nc    *nats.Conn
	topic string
	reply string

	notifyCh   chan serviceEvent
	closeCh    chan struct{}
}

type serviceConfig struct {
	lg   log.Logger
	name string

	nc    *nats.Conn
	topic string
	reply string
}

func (sc *serviceConfig) validate() error {
	if sc.name == "" {
		return errors.New("unexpected name")
	}

	if sc.reply == "" {
		return errors.New("unexpected reply")
	}

	return nil
}

func newService(cfg serviceConfig) (*service, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	lg := cfg.lg
	if lg == nil {
		lg = log.NewNopLogger()
	}

	return &service{
		lg:      lg,
		name:    cfg.name,
		nc:      cfg.nc,
		topic:   cfg.topic,
		reply:   cfg.reply,
		closeCh: make(chan struct{}),
	}, nil
}

func (s *service) subscribe(ctx context.Context, subject string) error {
	msgCh := make(chan *nats.Msg)
	sub, err := s.nc.ChanSubscribe(topic, msgCh)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	go func(sub *nats.Subscription, msgch chan *nats.Msg) {
		defer func() {
			s.lg.Log("action", "exiting")
			close(s.closeCh)
		}()

		defer sub.Unsubscribe()

		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-msgCh:
				s.lg.Log("action", "receive message", "message", string(msg.Data))

				msg.Reply += "." + s.reply
				err := msg.Respond([]byte("hello from service \"" + s.name + "\""))
				if err != nil {
					s.lg.Log("action", "respond message", "msg", "failed to respond message", "err", err)
					return
				}

				s.lg.Log("action", "respond message", "msg", "success to respond message")
				return
			}
		}
	}(sub, msgCh)

	return nil
}

func (s *service) notify() chan serviceEvent {
}
