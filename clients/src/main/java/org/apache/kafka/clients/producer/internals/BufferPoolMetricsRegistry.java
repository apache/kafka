package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.metrics.Metrics;

public class BufferPoolMetricsRegistry extends MetricsRegistry {

    public BufferPoolMetricsRegistry(Metrics metrics) {
        super(metrics);

    }

}
