// Command ppp-service is the ppp edge data node. It registers with the
// control plane, serves the Data gRPC service (GetPiece/DownloadFile/
// Subscribe/Unsubscribe) and downloads files from the source or upstream
// peers into a local piece store.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/akzj/ppp/internal/agent"
)

func main() {
	cfg := agent.DefaultConfig()
	cfg.RegisterFlags(flag.CommandLine)
	flag.Parse()
	// Finalize converts the parsed role string into the typed proto role and
	// validates the configuration (NewAgent expects a finalized config).
	if err := cfg.Finalize(); err != nil {
		log.Fatalf("ppp-service: %v", err)
	}

	a, err := agent.NewAgent(cfg)
	if err != nil {
		log.Fatalf("ppp-service: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := a.Start(ctx); err != nil {
		log.Fatalf("ppp-service: %v", err)
	}
	log.Printf("ppp-service %s started: addr=%s tree=%s role=%s", a.NodeID(), a.Addr(), cfg.Tree, cfg.Role)
	<-ctx.Done()
	a.Stop()
	log.Printf("ppp-service %s stopped", a.NodeID())
}
