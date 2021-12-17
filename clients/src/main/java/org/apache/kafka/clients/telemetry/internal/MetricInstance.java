package org.apache.kafka.clients.telemetry.internal;

import io.opentelemetry.proto.metrics.v1.Metric;

public interface MetricInstance {

    void clear();

    Metric toMetric();

}
