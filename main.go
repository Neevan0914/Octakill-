// Command Server is the OCTAKILL game + online multiplayer server.
//
// It embeds the browser game (octa-kill_1.html) into a single static
// Linux x86_64 binary, serves it over HTTP and hosts online multiplayer
// on /ws (see multiplayer.go).
package main

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

//go:embed octa-kill_1.html
var indexHTML []byte

const defaultPort = 7770

func main() {
	port := flag.Int("port", envPort(), "port to listen on (env PORT)")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", newHub().serveWS)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/index.html" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Write(indexHTML)
	})

	srv := &http.Server{
		Addr:              net.JoinHostPort("0.0.0.0", strconv.Itoa(*port)),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("OCTAKILL server listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Hosts (Render, PlayFlow, ...) stop instances with SIGTERM; shut down cleanly.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Print("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}

func envPort() int {
	if p, err := strconv.Atoi(os.Getenv("PORT")); err == nil && p > 0 {
		return p
	}
	return defaultPort
}
