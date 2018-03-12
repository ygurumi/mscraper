package main

import (
	"context"
	"flag"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	log "github.com/sirupsen/logrus"
	"github.com/ygurumi/mscraper"
	pb_monitoring "google.golang.org/genproto/googleapis/monitoring/v3"
)

const (
	chunkSize int = 200
)

func main() {
	file := flag.String("config", "config.json", "")
	debug := flag.Bool("debug", false, "")
	flag.Parse()

	if *debug {
		log.SetFormatter(&log.TextFormatter{})
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetFormatter(&log.JSONFormatter{})
		log.SetLevel(log.InfoLevel)
	}

	cfgs, err := mscraper.ReadConfig(*file)
	if err != nil {
		log.Panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		log.Panic(err)
	}

	log.Infof("Start %v worker(s)", len(cfgs))

	for _, cfg := range cfgs {
		go workerLoop(ctx, client, cfg)
	}

	select {}
}

func eachChunk(ts []*pb_monitoring.TimeSeries, f func([]*pb_monitoring.TimeSeries) error) error {
	lenTs := len(ts)

	for i := 0; i < lenTs; i += chunkSize {
		endChunk := i + chunkSize
		if endChunk > lenTs {
			endChunk = lenTs
		}

		err := f(ts[i:endChunk])
		if err != nil {
			return err
		}
	}

	return nil
}

func workerLoop(ctx context.Context, client *monitoring.MetricClient, cfg mscraper.Config) {
	tick := time.NewTicker(cfg.Interval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			break
		case <-tick.C:
			ts, err := mscraper.FetchTimeSeries(cfg, chunkSize)
			if err == nil {
				eachChunk(ts, func(c []*pb_monitoring.TimeSeries) error {
					if err := client.CreateTimeSeries(ctx, &pb_monitoring.CreateTimeSeriesRequest{
						Name:       monitoring.MetricProjectPath(cfg.Project),
						TimeSeries: c,
					}); err != nil {
						log.Error(err)
					}
					return nil
				})
			} else {
				log.Error(err)
			}
		}
	}
}
