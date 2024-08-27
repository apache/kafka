package org.apache.kafka.coordinator.group.metrics;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetricsImpl;

public class GroupCoordinatorRuntimeMetrics extends CoordinatorRuntimeMetricsImpl {
    public static final String METRICS_GROUP = "group-coordinator-metrics";

    public GroupCoordinatorRuntimeMetrics(Metrics metrics) {
        super(metrics, METRICS_GROUP);
    }
}
