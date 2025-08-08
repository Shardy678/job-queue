package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"job-queue/internal/storage"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func StartWorker(rdb *redis.Client, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Worker stopped")
			return
		default:
			res, err := rdb.BLPop(ctx, 5*time.Second, storage.QueueKey).Result()
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
			ProcessJob(rdb, ctx, job)
		}
	}
}

func ProcessJob(rdb *redis.Client, ctx context.Context, job Job) {
	job.Status = "processing"
	jobJSON, err := json.Marshal(job)
	if err != nil {
		fmt.Println(err)
	}
	result := rdb.Set(ctx, storage.StorageKey+job.ID.String(), jobJSON, 0)
	err = result.Err()
	if err != nil {
		fmt.Println(err)
	}
	err = DoJob(job)
	if err != nil {
		job.Attempts += 1
		if job.Attempts < job.MaxRetries {
			job.Status = "pending"
			jobJSON, err = json.Marshal(job)
			if err != nil {
				fmt.Println(err)
			}
			result := rdb.LPush(ctx, storage.QueueKey, jobJSON)
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
	result = rdb.Set(ctx, storage.StorageKey+job.ID.String(), jobJSON, 0)
	err = result.Err()
	if err != nil {
		fmt.Println(err)
	}
}
