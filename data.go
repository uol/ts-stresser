package main

import (
	"math/rand"
	"time"
)

// Timestamp is a unix timestamp
type Timestamp int64

// DataPoint is a point in the timeseries data
type DataPoint struct {
	// Metric of the timeseries point
	Metric string `json:"metric"`
	// Tags of the timeseries point
	Tags map[string]string `json:"tags"`
	// Timestamp of the point
	Timestamp Timestamp `json:"timestamp"`
	// Value of the point
	Value int64 `json:"value"`
}

// Randomize changes the values of the point
func (point *DataPoint) Randomize() error {
	point.Value = rand.Int63()
	point.Timestamp = ConvertTimestamp(time.Now())
	return nil
}

// ChangeDefaultTags allow reuse of allocated data
func (point *DataPoint) ChangeDefaultTags(host, service, keyspace string) error {
	if point.Tags == nil {
		point.Tags = make(map[string]string)
	}
	point.Tags["host"] = host
	point.Tags["service"] = service
	point.Tags["ksid"] = keyspace
	return nil
}

// DataList abstracts a list of DataPoints
type DataList []DataPoint

// ConvertTimestamp takes a date and returns a timestamp
func ConvertTimestamp(date time.Time) Timestamp {
	return Timestamp(date.Unix())
}
