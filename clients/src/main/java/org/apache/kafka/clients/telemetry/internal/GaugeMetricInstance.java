package org.apache.kafka.clients.telemetry.internal;

import io.opentelemetry.proto.metrics.v1.Metric;

public class GaugeMetricInstance implements MetricInstance {

    public void increment() {
        // TODO: implementation
    }

    public void decrement() {
        // TODO: implementation
    }

    @Override
    public void clear() {
        // TODO: reset value in implementations
    }

    @Override
    public Metric toMetric() {
        return null;
    }
}
