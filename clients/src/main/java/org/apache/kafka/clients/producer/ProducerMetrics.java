package org.apache.kafka.clients.producer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.SpecificMetrics;
import org.apache.kafka.common.network.SelectorMetricsRegistry;

public class ProducerMetrics {

    public SelectorMetricsRegistry selectorMetrics;

    public ProducerMetrics(Set<String> keySet, String metricGrpPrefix) {
        this.selectorMetrics = new SelectorMetricsRegistry(keySet, metricGrpPrefix);
    }

    private List<MetricNameTemplate> getAllTemplates() {
        List<MetricNameTemplate> l = new ArrayList<>();
        l.addAll(this.selectorMetrics.getAllTemplates());
        return l;
    }

    public static void main(String[] args) {
        Set<String> tags = new HashSet<>();
        tags.add("client-id");
        ProducerMetrics metrics = new ProducerMetrics(tags, "producer");
        System.out.println(SpecificMetrics.toHtmlTable("kafka.producer", metrics.getAllTemplates()));
    }

}