package main

import (
	"context"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/nats-io/nats.go"
)

const topic = "kek"
const replyTopic = "kek_inbox"

func main() {
	lg := log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
	mlg := log.WithPrefix(lg, "component", "main")
	mlg.Log("action", "connect to NATS server")

	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		panic(err)
	}

	defer nc.Close()

	mlg.Log("action", "spawn subscribers")

	wg := &sync.WaitGroup{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	go func() {
		mlg.Log("action", "got signal", "signal", <-sigCh)
		cancel()
	}()

	workers := 10
	bootstrapCh := make(chan bool)
	subjects := [10]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(ctx context.Context, wg *sync.WaitGroup, i int, lg log.Logger, nc *nats.Conn) {
			lg = log.WithPrefix(lg, "component", "subscriber #"+strconv.Itoa(i))
			defer func(lg log.Logger) {
				lg.Log("action", "exiting")
				wg.Done()
			}(lg)

			msgCh := make(chan *nats.Msg)
			sub, err := nc.ChanSubscribe(topic, msgCh)
			if err != nil {
				bootstrapCh <- false
				return
			}
			bootstrapCh <- true

			defer sub.Unsubscribe()

			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-msgCh:
					lg.Log("action", "receive message", "message", string(msg.Data))

					msg.Reply += "." + subjects[i]
					err := msg.Respond([]byte("hello from worker #" + strconv.Itoa(i)))
					if err != nil {
						lg.Log("action", "respond message", "msg", "failed to respond message", "err", err)
						return
					}

					lg.Log("action", "respond message", "msg", "success to respond message")
					return
				}
			}
		}(ctx, wg, i, lg, nc)
	}

	for i := 0; i < workers; i++ {
		if !<-bootstrapCh {
			workers -= 1
		}

		runtime.Gosched()
	}

	err = nc.PublishRequest(topic, replyTopic, []byte("hello from server"))
	if err != nil {
		mlg.Log("action", "publish relaxed message", "msg", "failed to publish relaxed message", "err", err)
		cancel()
	}

	sub, err := nc.SubscribeSync(replyTopic + ".*")
	if err != nil {
		mlg.Log("action", "subscribe to reply topic", "msg", "failed to subscribe to reply topic", "err", err)
	}

LOOP:
	for i := 0; i < workers; i++ {
		select {
		case <-ctx.Done():
			break LOOP
		default:
		}

		wctx, _ := context.WithTimeout(ctx, time.Second * 3)
		msg, err := sub.NextMsgWithContext(wctx)
		if err != nil {
			mlg.Log("action", "read reply", "msg", "failed to read reply", "err", err)
		}

		if msg != nil {
			mlg.Log("action", "read reply", "topic", msg.Subject, "msg", string(msg.Data))
		}
	}

	mlg.Log("action", "waiting for signal")
	wg.Wait()
}
