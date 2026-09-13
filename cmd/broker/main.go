// Command broker is the RAVEN message broker: a small Kafka-style log
// broker with a custom binary TCP protocol. Ports and env vars are
// fixed by docs/contracts/ports-and-env.md (TCP 9100, ops HTTP 9101).
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	brokersvc "github.com/refleeexzz/RAVEN/services/broker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := brokersvc.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "broker: %v\n", err)
		os.Exit(1)
	}
}
