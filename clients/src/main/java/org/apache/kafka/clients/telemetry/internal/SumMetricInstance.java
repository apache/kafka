package org.apache.kafka.clients.telemetry.internal;

import io.opentelemetry.proto.metrics.v1.Metric;
import java.util.Map;

public class SumMetricInstance implements MetricInstance {

    Map<Map<MetricLabel, String>, Integer> values;

    public void increment(Map<MetricLabel, String> labels) {
        values.merge(labels, 1, Integer::sum);
    }

    @Override
    public void clear() {
        values.clear();
    }

    @Override
    public Metric toMetric() {
        return null;
    }
}
