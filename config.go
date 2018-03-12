package mscraper

import (
	"encoding/json"
	"io/ioutil"
	"regexp"
	"time"
)

type Resource struct {
	Type   string            `json:"type"`
	Labels map[string]string `json:"labels"`
}

type Metric struct {
	Prefix []string          `json:"prefix"`
	Labels map[string]string `json:"labels"`
	Filter string            `json:"filter"`
}

type ConfigExpr struct {
	Target   string   `json:"target"`
	Resource Resource `json:"resource"`
	Metric   Metric   `json:"metric"`
	Interval string   `json:"interval"`
}

type Config struct {
	Target   string
	Resource Resource
	Metric   Metric
	Prefix   []string
	Interval time.Duration
	Project  string
	Filter   *regexp.Regexp
}

func ReadConfig(path string) ([]Config, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfgs []ConfigExpr
	if err := json.Unmarshal(bytes, &cfgs); err != nil {
		return nil, err
	}

	ret := make([]Config, 0, len(cfgs))
	for _, cfg := range cfgs {
		duration, err := time.ParseDuration(cfg.Interval)
		if err != nil {
			return nil, err
		}

		project := cfg.Resource.Labels["project_id"]
		filterStr := cfg.Metric.Filter
		if filterStr == "" {
			filterStr = "^.+$"
		}
		filter, err := regexp.Compile(filterStr)
		if err != nil {
			return nil, err
		}

		cfg := Config{
			Target:   cfg.Target,
			Resource: cfg.Resource,
			Metric:   cfg.Metric,
			Interval: duration,
			Project:  project,
			Filter:   filter,
		}

		ret = append(ret, cfg)
	}

	return ret, nil
}
