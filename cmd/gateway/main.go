package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	gateway "github.com/lim-bo/barn/internal/gateway_additions"
	"github.com/lim-bo/barn/internal/services/pb"
	"github.com/lim-bo/barn/internal/settings"
	"github.com/lim-bo/barn/pkg/cleanup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	cfg := settings.GetConfig()
	slog.SetLogLoggerLevel(slog.Level(cfg.GetInt("gateway.log_level")))
	// Preparing mux
	gwMux := runtime.NewServeMux(
		runtime.WithIncomingHeaderMatcher(headerMatcher),
		runtime.WithOutgoingHeaderMatcher(headerMatcher),
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.HTTPBodyMarshaler{Marshaler: &runtime.JSONPb{}}),
	)

	conn, err := grpc.NewClient(
		cfg.GetString("object_service.mask_address"),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal(err)
	}
	cleanup.Register(&cleanup.Job{
		Name: "Closing object service client",
		Func: func() error {
			return conn.Close()
		},
	})
	objClient := pb.NewObjectServiceClient(conn)

	mx := gateway.NewExtendedGWMux(objClient, gwMux)

	// Registering services
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		mu.Lock()
		defer mu.Unlock()
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
		mu.Lock()
		defer mu.Unlock()
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
		mu.Lock()
		defer mu.Unlock()
		err := pb.RegisterObjectServiceHandlerFromEndpoint(context.Background(),
			gwMux,
			cfg.GetString("object_service.mask_address"),
			opts)
		if err != nil {
			log.Fatal("error registering object service: ", err)
		}
	}()
	wg.Wait()
	// Running
	addr := cfg.GetString("gateway.address")
	server := &http.Server{
		Addr: addr,
		Handler: gateway.CORSMiddleware(
			mx,
		),
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
