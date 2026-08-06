// Command ppp-ctl-server is the central control plane of ppp. It serves the
// Control gRPC service (tree lifecycle, node registration, topology and
// banned-list pushes, job orchestration) and persists its state in bbolt.
//
// This build is single-instance; HA (leader election) comes in a later phase.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/akzj/ppp/internal/ctl"
)

func main() {
	cfg := ctl.DefaultConfig()
	cfg.RegisterFlags(flag.CommandLine)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("ppp-ctl-server starting: addr=%s db=%s heartbeat-timeout=%s",
		cfg.Addr, cfg.DBPath, cfg.HeartbeatTimeout)
	if err := ctl.Run(ctx, cfg); err != nil {
		log.Fatalf("ppp-ctl-server: %v", err)
	}
}
