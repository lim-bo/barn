package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/lim-bo/barn/internal/services/pb"
	"github.com/lim-bo/barn/internal/settings"
	"github.com/lim-bo/barn/pkg/cleanup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	cfg := settings.GetConfig()

	gwMux := runtime.NewServeMux(
		runtime.WithIncomingHeaderMatcher(headerMatcher),
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &OctetStreamMarshaller{}))
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		err := pb.RegisterAuthServiceHandlerFromEndpoint(context.Background(),
			gwMux,
			cfg.GetString("auth_service.mask_address"),
			opts)
		if err != nil {
			log.Fatal("error registering auth service: ", err)
		}
	}()

	go func() {
		defer wg.Done()
		err := pb.RegisterBucketServiceHandlerFromEndpoint(context.Background(),
			gwMux,
			cfg.GetString("bucket_service.mask_address"),
			opts)
		if err != nil {
			log.Fatal("error registering bucket service: ", err)
		}
	}()

	go func() {
		defer wg.Done()
		err := pb.RegisterObjectServiceHandlerFromEndpoint(context.Background(),
			gwMux,
			cfg.GetString("object_service.mask_address"),
			opts)
		if err != nil {
			log.Fatal("error registering object service: ", err)
		}
	}()
	wg.Wait()

	addr := cfg.GetString("gateway.address")
	server := &http.Server{
		Addr:    addr,
		Handler: CORSMiddleware(gwMux),
	}

	cleanup.Register(&cleanup.Job{
		Name: "stopping server",
		Func: func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return server.Shutdown(ctx)
		},
	})

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGINT,
	)
	defer cancel()
	errCh := make(chan error, 1)

	go func() {
		log.Printf("running server at %s", addr)
		if err := server.ListenAndServe(); err != nil {
			errCh <- err
		}
	}()

	<-ctx.Done()
	log.Println("shutting down server...")
	cleanup.CleanUp()
	log.Println("server stopped")
}
