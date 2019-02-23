package kafka

import (
	"fmt"
	"time"
)

//BacklogReporter reports the kafka message backlog
type BacklogReporter struct {
	Name       string
	Retriever  BacklogRetriever
	Logger     func(name string, count int, err error)
	Shutdown   *bool
	ReportRate time.Duration
}

//NewBacklogReporter creates a backlog reporter for a consumer
func NewBacklogReporter(name string, retriever BacklogRetriever, logger func(name string, count int, err error), shutdown *bool, reportRateMs int) (*BacklogReporter, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is nil")
	}
	if retriever == nil {
		return nil, fmt.Errorf("retrieve is nil")
	}

	if shutdown == nil {
		return nil, fmt.Errorf("shutdown is nil")
	}

	return &BacklogReporter{
		Name:       name,
		Retriever:  retriever,
		Logger:     logger,
		Shutdown:   shutdown,
		ReportRate: time.Duration(reportRateMs) * time.Millisecond,
	}, nil
}

//Run calculates the rate and calls the execute method with the rate
func (r *BacklogReporter) Run() {
	for range time.NewTicker(r.ReportRate).C {
		backlog, err := r.Retriever.GetBacklog()
		r.Logger(r.Name, backlog, err)

		if *r.Shutdown {
			break
		}
	}
}
