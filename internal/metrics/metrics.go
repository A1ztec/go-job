package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	JobsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "jobs_processed_total",
			Help: "Total number of jobs processed, by type and outcome.",
		},
		[]string{"job_type", "status"},
	)

	JobDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "job_duration_seconds",
			Help: "Time taken to process a job, by type.",
		},
		[]string{"job_type"},
	)

	QueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "queue_depth",
			Help: "Current number of jobs waiting in the queue.",
		},
	)

	WorkersBusy = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "workers_busy",
			Help: "Current number of workers actively processing a job.",
		},
	)
)

func init() {
	prometheus.MustRegister(JobsProcessed, JobDuration, QueueDepth, WorkersBusy)
}
