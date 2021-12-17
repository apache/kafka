package org.apache.kafka.clients.telemetry.internal;

import java.util.Collections;
import java.util.Map;

public class ClientInstanceLevelMetrics {

    private final SumMetricInstance clientConnectionCreations = new SumMetricInstance();

    private final GaugeMetricInstance clientConnectionCount = new GaugeMetricInstance();

    private final SumMetricInstance clientRequestErrors = new SumMetricInstance();

    public void incrementClientConnectionCreations(String brokerId) {
        Map<MetricLabel, String> labels = Collections.singletonMap(MetricLabel.brokerId, brokerId);
        clientConnectionCreations.increment(labels);
    }

    public void incrementClientConnectionCount() {
        clientConnectionCount.increment();
    }

    public void decrementClientConnectionCount() {
        clientConnectionCount.decrement();
    }

}
