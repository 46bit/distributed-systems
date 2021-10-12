package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/46bit/distributed-systems/ticket-issuer-api/api"
)

func main() {
	//ctx, cancel := context.WithCancel(context.Background())

	config, err := LoadConfig(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := NewGrpcServer()

	ticketIssuerServer := NewTicketIssuerServer(config)
	api.RegisterTicketIssuerServer(grpcServer, ticketIssuerServer)

	exitSignals := make(chan os.Signal, 1)
	signal.Notify(exitSignals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-exitSignals
		log.Println("Exiting...")
		//cancel()
		grpcServer.GracefulStop()
	}()

	c, err := net.Listen("tcp", config.BindAddress)
	if err != nil {
		log.Fatal(err)
	}
	if err := grpcServer.Serve(c); err != nil {
		log.Fatal(err)
	}
}
