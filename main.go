package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Job struct {
	ID         uuid.UUID         `json:"id"`
	JobType    string            `json:"job_type"`
	Payload    map[string]string `json:"payload"`
	Status     string            `json:"status"`
	Attempts   int               `json:"attempts"`
	MaxRetries int               `json:"max_retries"`
}

var (
	ctx        = context.Background()
	queueKey   = "jobs:queue"
	storageKey = "jobs:"
)

func newJob(jobtype string, payload map[string]string) *Job {
	return &Job{
		ID:         uuid.New(),
		JobType:    jobtype,
		Payload:    payload,
		Status:     "pending",
		Attempts:   0,
		MaxRetries: 3,
	}
}

func handleJob(rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			Type    string            `json:"job_type"`
			Payload map[string]string `json:"payload"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		job := newJob(req.Type, req.Payload)
		b, _ := json.Marshal(job)
		rdb.LPush(ctx, queueKey, b)
		rdb.Set(ctx, storageKey+job.ID.String(), b, 0)
		c.JSON(200, gin.H{"id": job.ID})
	}
}

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

	r := gin.Default()
	r.POST("/job", handleJob(rdb))

	r.Run(":8080")
	log.Println("Running on localhost:8080")
}
