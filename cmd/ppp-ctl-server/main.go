// Command ppp-ctl-server is the central control plane of ppp. It serves the
// Control gRPC service (tree lifecycle, node registration, topology and
// banned-list pushes, job orchestration) and persists its state in
// PostgreSQL. Multiple instances share the same PG; a PG lease elects exactly
// one leader, which serves mutations and watch streams (LB routes to it via
// the /leader health endpoint). There is no raft.
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

	log.Printf("ppp-ctl-server %s starting: gRPC=%s http=/leader=%s pg=%s heartbeat-timeout=%s",
		cfg.InstanceID, cfg.Addr, cfg.HTTPAddr, cfg.PGDSN, cfg.HeartbeatTimeout)
	if err := ctl.Run(ctx, cfg); err != nil {
		log.Fatalf("ppp-ctl-server: %v", err)
	}
}
