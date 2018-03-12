package mscraper

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	pb_timestamp "github.com/golang/protobuf/ptypes/timestamp"
	client_model "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	pb_metric "google.golang.org/genproto/googleapis/api/metric"
	pb_monitoredres "google.golang.org/genproto/googleapis/api/monitoredres"
	pb_monitoring "google.golang.org/genproto/googleapis/monitoring/v3"
)

func fetchMetricFamilies(url string) (map[string]*client_model.MetricFamily, error) {
	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("app error: code = InvalidStatusCode desc = HTTP response status code `%v`", res.StatusCode)
	}

	parser := expfmt.TextParser{}
	return parser.TextToMetricFamilies(res.Body)
}

func timestampNow() *pb_timestamp.Timestamp {
	return &pb_timestamp.Timestamp{
		Seconds: time.Now().Unix(),
	}
}

func timeIntervalEnd() *pb_monitoring.TimeInterval {
	return &pb_monitoring.TimeInterval{
		EndTime: timestampNow(),
	}
}

func newPbMetricType(prefix []string, nameOrig string) string {
	nameLower := strings.ToLower(nameOrig)
	reg := regexp.MustCompile(`[^a-z0-9]+`)
	name := reg.ReplaceAllString(nameLower, "_")

	strs := make([]string, 0, 2+len(prefix))
	strs = append(strs, "custom.googleapis.com")
	for _, str := range prefix {
		if str != "" {
			strs = append(strs, reg.ReplaceAllString(str, "_"))
		}
	}
	strs = append(strs, name)

	return strings.Join(strs, "/")
}

func labelPairsToMap(pairs []*client_model.LabelPair) map[string]string {
	ret := map[string]string{}
	for _, pair := range pairs {
		ret[pair.GetName()] = pair.GetValue()
	}
	return ret
}

func toDoubleValue(r float64) *pb_monitoring.TypedValue {
	return &pb_monitoring.TypedValue{
		Value: &pb_monitoring.TypedValue_DoubleValue{
			DoubleValue: r,
		},
	}
}

func mergeMaps(ms ...map[string]string) map[string]string {
	ret := map[string]string{}

	for _, m := range ms {
		for k, v := range m {
			ret[k] = v
		}
	}

	return ret
}

type typedValueCallback = func(*pb_monitoring.TypedValue, pb_metric.MetricDescriptor_MetricKind, map[string]string) error

type promMetricValues interface {
	each(f typedValueCallback) error
}

type promGauge struct {
	v interface{ GetValue() float64 }
}

type promSummary struct {
	v *client_model.Summary
}

type promHistgram struct {
	v *client_model.Histogram
}

func (pgauge promGauge) each(f typedValueCallback) error {
	return f(toDoubleValue(pgauge.v.GetValue()), pb_metric.MetricDescriptor_GAUGE, map[string]string{})
}

func (psummary promSummary) each(f typedValueCallback) error {
	summary := psummary.v

	if err := f(toDoubleValue(summary.GetSampleSum()), pb_metric.MetricDescriptor_GAUGE, map[string]string{
		"mode": "sum",
	}); err != nil {
		return err
	}
	if err := f(toDoubleValue(float64(summary.GetSampleCount())), pb_metric.MetricDescriptor_GAUGE, map[string]string{
		"mode": "count",
	}); err != nil {
		return err
	}
	for _, quantile := range summary.GetQuantile() {
		if err := f(toDoubleValue(quantile.GetValue()), pb_metric.MetricDescriptor_GAUGE, map[string]string{
			"mode":     "quantile",
			"quantile": fmt.Sprintf("%v", quantile.GetQuantile()),
		}); err != nil {
			return err
		}
	}

	return nil
}

func (phistgram promHistgram) each(f typedValueCallback) error {
	histgram := phistgram.v

	if err := f(toDoubleValue(histgram.GetSampleSum()), pb_metric.MetricDescriptor_GAUGE, map[string]string{
		"mode": "sum",
	}); err != nil {
		return err
	}
	if err := f(toDoubleValue(float64(histgram.GetSampleCount())), pb_metric.MetricDescriptor_GAUGE, map[string]string{
		"mode": "count",
	}); err != nil {
		return err
	}
	for _, bucket := range histgram.GetBucket() {
		if err := f(toDoubleValue(float64(bucket.GetCumulativeCount())), pb_metric.MetricDescriptor_GAUGE, map[string]string{
			"mode": "bucket",
			"le":   fmt.Sprintf("%v", bucket.GetUpperBound()),
		}); err != nil {
			return err
		}
	}

	return nil
}

func newPromMetricValues(metricType client_model.MetricType, metric *client_model.Metric) (promMetricValues, error) {
	switch metricType {
	case client_model.MetricType_SUMMARY:
		return promSummary{metric.GetSummary()}, nil
	case client_model.MetricType_HISTOGRAM:
		return promHistgram{metric.GetHistogram()}, nil
	case client_model.MetricType_UNTYPED:
		return promGauge{metric.GetUntyped()}, nil
	case client_model.MetricType_COUNTER:
		return promGauge{metric.GetCounter()}, nil
	case client_model.MetricType_GAUGE:
		return promGauge{metric.GetGauge()}, nil
	default:
		return nil, fmt.Errorf("app error: code = Unknown metric type desc = Metric type `%v`", metricType)
	}
}

func toTimeSeries(cfg Config, family *client_model.MetricFamily) ([]*pb_monitoring.TimeSeries, error) {
	interval := timeIntervalEnd()
	name := family.GetName()
	metricType := family.GetType()
	metrics := family.GetMetric()

	pbMetricType := newPbMetricType(cfg.Metric.Prefix, name)
	resource := pb_monitoredres.MonitoredResource{
		Type:   cfg.Resource.Type,
		Labels: cfg.Resource.Labels,
	}

	ret := make([]*pb_monitoring.TimeSeries, 0, len(metrics))

	for _, metric := range metrics {
		labelsOrig := labelPairsToMap(metric.GetLabel())

		metricValue, err := newPromMetricValues(metricType, metric)
		if err != nil {
			return nil, err
		}

		if err := metricValue.each(func(value *pb_monitoring.TypedValue, kind pb_metric.MetricDescriptor_MetricKind, labelsAdd map[string]string) error {
			labels := mergeMaps(labelsOrig, labelsAdd, cfg.Metric.Labels)

			metric := pb_metric.Metric{
				Type:   pbMetricType,
				Labels: labels,
			}

			ret = append(ret, &pb_monitoring.TimeSeries{
				Resource:   &resource,
				Metric:     &metric,
				MetricKind: kind,
				Points: []*pb_monitoring.Point{
					&pb_monitoring.Point{
						Interval: interval,
						Value:    value,
					},
				},
			})

			return nil
		}); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

func FetchTimeSeries(cfg Config, capacity int) ([]*pb_monitoring.TimeSeries, error) {
	ret := make([]*pb_monitoring.TimeSeries, 0, capacity)

	families, err := fetchMetricFamilies(cfg.Target)
	if err != nil {
		return nil, err
	}

	for name, family := range families {
		if cfg.Filter.MatchString(name) {
			ts, err := toTimeSeries(cfg, family)
			if err != nil {
				return nil, err
			}
			ret = append(ret, ts...)
		}
	}

	return ret, nil
}
