package org.apache.kafka.clients.producer;

import java.util.Set;

import org.apache.kafka.common.network.SelectorMetricsRegistry;

public class ProducerMetrics {

    public SelectorMetricsRegistry selectorMetrics;

    public ProducerMetrics(Set<String> keySet, String metricGrpPrefix) {
        this.selectorMetrics = new SelectorMetricsRegistry(keySet, metricGrpPrefix);
    }

}
