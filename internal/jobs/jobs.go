package jobs

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Job struct {
	ID         uuid.UUID         `json:"id"`
	JobType    string            `json:"job_type"`
	Payload    map[string]string `json:"payload"`
	Status     string            `json:"status"`
	Attempts   int               `json:"attempts"`
	MaxRetries int               `json:"max_retries"`
}

func NewJob(jobtype string, payload map[string]string) *Job {
	return &Job{
		ID:         uuid.New(),
		JobType:    jobtype,
		Payload:    payload,
		Status:     "pending",
		Attempts:   0,
		MaxRetries: 3,
	}
}

func DoJob(job Job) error {
	switch job.JobType {
	case "email":
		to := job.Payload["to"]
		subject := job.Payload["subject"]
		body := job.Payload["body"]
		if to == "" || subject == "" || body == "" {
			return errors.New("missing email fields")
		}
		fmt.Printf("Simulated sending email to %s: %s - %s\n", to, subject, body)
		time.Sleep(1 * time.Second)
		return nil

	case "math":
		aStr := job.Payload["a"]
		bStr := job.Payload["b"]
		if aStr == "" || bStr == "" {
			return errors.New("missing math operands")
		}
		var a, b int
		_, errA := fmt.Sscan(aStr, &a)
		_, errB := fmt.Sscan(bStr, &b)
		if errA != nil || errB != nil {
			return errors.New("invalid math operands")
		}
		result := a + b
		fmt.Printf("Math job: %d + %d = %d\n", a, b, result)
		return nil

	case "long":
		fmt.Println("Simulating a long task...")
		time.Sleep(2 * time.Second)
		return nil

	default:
		return errors.New("unsupported job type: " + job.JobType)
	}
}
