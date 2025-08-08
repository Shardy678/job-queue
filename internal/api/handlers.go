package api

import (
	"encoding/json"
	"fmt"
	"job-queue/internal/jobs"
	"job-queue/internal/storage"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func HandleJob(rdb *redis.Client) gin.HandlerFunc {
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
		job := jobs.NewJob(req.Type, req.Payload)
		b, _ := json.Marshal(job)
		ctx := c.Request.Context()
		rdb.LPush(ctx, storage.QueueKey, b)
		rdb.Set(ctx, storage.StorageKey+job.ID.String(), b, 0)
		c.JSON(200, gin.H{"id": job.ID})
	}
}

func GetJobHandler(rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		_, err := uuid.Parse(id)
		if err != nil {
			c.JSON(400, gin.H{"error": "invalid job id"})
		}
		key := storage.StorageKey + id
		ctx := c.Request.Context()
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
		var job jobs.Job
		err = json.Unmarshal([]byte(jobJSON), &job)
		if err != nil {
			fmt.Println(err)
			c.JSON(500, gin.H{"error": "internal server error"})
			return
		}
		c.JSON(200, job)
	}
}

func GetAllJobsHandler(rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		var allJobs []jobs.Job
		ctx := c.Request.Context()
		iter := rdb.Scan(ctx, 0, storage.StorageKey+"*", 0).Iterator()
		for iter.Next(ctx) {
			jobJSON, err := rdb.Get(ctx, iter.Val()).Result()
			if err != nil {
				fmt.Println("Redis get error:", err)
				continue
			}

			var job jobs.Job
			if err := json.Unmarshal([]byte(jobJSON), &job); err != nil {
				fmt.Println("Unmarshal error:", err)
				continue
			}
			allJobs = append(allJobs, job)
		}

		if err := iter.Err(); err != nil {
			c.JSON(500, gin.H{"error": "error scanning jobs"})
			return
		}
		c.JSON(200, allJobs)
	}
}
