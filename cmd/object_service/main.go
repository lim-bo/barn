package main

import (
	"context"
	"log"
	"log/slog"
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
	slog.SetLogLoggerLevel(slog.Level(cfg.GetInt("object_service.log_level")))

	objStorageEngine := storage.NewLocalFS(cfg.GetString("storage.root"))
	objRepository := repos.NewObjectsRepo(&repos.DBConfig{
		Address:  cfg.GetString("postgres.address"),
		User:     cfg.GetString("postgres.user"),
		Password: cfg.GetString("postgres.password"),
		DB:       cfg.GetString("postgres.db"),
	})

	usersRepo := repos.NewUsersRepo(&repos.DBConfig{
		Address:  cfg.GetString("postgres.address"),
		User:     cfg.GetString("postgres.user"),
		Password: cfg.GetString("postgres.password"),
		DB:       cfg.GetString("postgres.db"),
	})

	sigValidator := services.NewSignatureValidator(usersRepo)

	objService := services.NewObjectService(services.ObjectServiceConfig{
		ObjRepo:    objRepository,
		ObjStorage: objStorageEngine,
	})
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(
		services.RequestIDInterceptor,
		sigValidator.AuthInterceptor,
		services.LoggerSettingInterceptor(slog.Default()),
	))
	pb.RegisterObjectServiceServer(s, objService)

	cleanup.Register(&cleanup.Job{
		Name: "shutting down server",
		Func: func() error {
			s.GracefulStop()
			return nil
		},
	})

	addr := cfg.GetString("object_service.address")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

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
		if err := s.Serve(lis); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
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
