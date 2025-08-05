package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	repos "github.com/lim-bo/barn/internal/repository"
	"github.com/lim-bo/barn/internal/services"
	"github.com/lim-bo/barn/internal/services/pb"
	"github.com/lim-bo/barn/internal/settings"
	"github.com/lim-bo/barn/internal/storage"
	"github.com/lim-bo/barn/pkg/cleanup"
	"google.golang.org/grpc"
)

func main() {
	cfg := settings.GetConfig()
	storageEngine := storage.NewBucketLocalFS(cfg.GetString("storage.root"))
	bucketsRepo := repos.NewBucketRepo(&repos.DBConfig{
		Address:  cfg.GetString("postgres.address"),
		User:     cfg.GetString("postgres.user"),
		Password: cfg.GetString("postgres.password"),
		DB:       cfg.GetString("postgres.db"),
	})

	bs := services.NewBucketService(bucketsRepo, storageEngine)
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(services.RequestIDInterceptor, services.AuthInterceptor))
	pb.RegisterBucketServiceServer(s, bs)

	cleanup.Register(&cleanup.Job{
		Name: "shutting down server",
		Func: func() error {
			s.GracefulStop()
			return nil
		},
	})

	addr := cfg.GetString("bucket_service.address")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	errCh := make(chan error, 1)

	go func() {
		log.Printf("running server at %s", addr)
		if err := s.Serve(lis); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-stop:
		stop = nil
		break
	case err := <-errCh:
		log.Printf("server error: %s", err.Error())
		errCh = nil
		break
	}
	log.Println("shutting down")
	cleanup.CleanUp()
	log.Println("service stopped")
}
