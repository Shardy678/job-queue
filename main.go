package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

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
		if req.Type == "" {
			c.JSON(400, gin.H{"error": "job_type is required"})
			return
		}
		if len(req.Payload) == 0 {
			c.JSON(400, gin.H{"error": "payload is required and cannot be empty"})
			return
		}
		job := newJob(req.Type, req.Payload)
		b, _ := json.Marshal(job)
		rdb.LPush(ctx, queueKey, b)
		rdb.Set(ctx, storageKey+job.ID.String(), b, 0)
		c.JSON(200, gin.H{"id": job.ID})
	}
}

func startWorker(rdb *redis.Client, ctx context.Context) {
	for {
		res, err := rdb.BLPop(ctx, 5*time.Second, queueKey).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			fmt.Println(err)
			continue
		}
		if len(res) < 2 {
			continue
		}
		jobJSON := res[1]
		var job Job
		err = json.Unmarshal([]byte(jobJSON), &job)
		if err != nil {
			fmt.Println(err)
			continue
		}
		processJob(rdb, ctx, job)
	}
}

func processJob(rdb *redis.Client, ctx context.Context, job Job) {
	job.Status = "processing"
	jobJSON, err := json.Marshal(job)
	if err != nil {
		fmt.Println(err)
	}
	result := rdb.Set(ctx, storageKey+job.ID.String(), jobJSON, 0)
	err = result.Err()
	if err != nil {
		fmt.Println(err)
	}
	err = doJob()
	if err != nil {
		job.Attempts += 1
		if job.Attempts < job.MaxRetries {
			job.Status = "pending"
			jobJSON, err = json.Marshal(job)
			if err != nil {
				fmt.Println(err)
			}
			result := rdb.LPush(ctx, queueKey, jobJSON)
			err = result.Err()
			if err != nil {
				fmt.Println(err)
			}
		} else {
			job.Status = "failed"
		}
	} else {
		job.Status = "completed"
	}
	jobJSON, err = json.Marshal(job)
	if err != nil {
		fmt.Println(err)
	}
	result = rdb.Set(ctx, storageKey+job.ID.String(), jobJSON, 0)
	err = result.Err()
	if err != nil {
		fmt.Println(err)
	}
}

func doJob() error {
	num := rand.Intn(2)
	if num == 0 {
		return errors.New("job failed")
	}
	return nil
}

func getJobHandler(rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		_, err := uuid.Parse(id)
		if err != nil {
			c.JSON(400, gin.H{"error": "invalid job id"})
		}
		key := storageKey + id
		jobJSON, err := rdb.Get(ctx, key).Result()
		if err == redis.Nil {
			c.JSON(404, gin.H{"error": "job not found"})
			return
		}
		if err != nil {
			fmt.Println(err)
			c.JSON(500, gin.H{"error": "internal server error"})
			return
		}
		var job Job
		err = json.Unmarshal([]byte(jobJSON), &job)
		if err != nil {
			fmt.Println(err)
			c.JSON(500, gin.H{"error": "internal server error"})
			return
		}
		c.JSON(200, job)
	}
}

func getAllJobsHandler(rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		var jobs []Job
		// SCAN for job keys
		iter := rdb.Scan(ctx, 0, storageKey+"*", 0).Iterator()
		for iter.Next(ctx) {
			jobJSON, err := rdb.Get(ctx, iter.Val()).Result()
			if err != nil {
				fmt.Println("Redis get error:", err)
				continue // skip this job if error
			}
			var job Job
			if err := json.Unmarshal([]byte(jobJSON), &job); err != nil {
				fmt.Println("Unmarshal error:", err)
				continue // skip this job if error
			}
			jobs = append(jobs, job)
		}
		if err := iter.Err(); err != nil {
			c.JSON(500, gin.H{"error": "error scanning jobs"})
			return
		}
		c.JSON(200, jobs)
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
	numWorkers := 3
	for i := 0; i < numWorkers; i++ {
		go startWorker(rdb, ctx)
	}
	r := gin.Default()
	r.POST("/job", handleJob(rdb))
	r.GET("/job/:id", getJobHandler(rdb))
	r.GET("/jobs", getAllJobsHandler(rdb))
	r.Run(":8080")
	log.Println("Running on localhost:8080")
}
