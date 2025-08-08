package main

import (
	"context"
	"job-queue/internal/api"
	"job-queue/internal/jobs"
	"log"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("❌ Failed to connect to Redis: %v", err)
	} else {
		log.Println("✅ Successfully connected to Redis")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer stop()

	numWorkers := 3
	var wg sync.WaitGroup

	for range numWorkers {
		wg.Add(1)
		go jobs.StartWorker(rdb, ctx, &wg)
	}

	r := gin.Default()
	r.POST("/job", api.HandleJob(rdb))
	r.GET("/job/:id", api.GetJobHandler(rdb))
	r.GET("/jobs", api.GetAllJobsHandler(rdb))

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ Failed to start server: %v", err)
		}
	}()
	log.Println("Running on localhost:8080")

	<-ctx.Done()
	log.Println("Shutting down server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("❌ Failed to shutdown server: %v", err)
	}

	wg.Wait()
	log.Println("Server stopped gracefully")
}
