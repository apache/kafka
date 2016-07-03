package org.apache.kafka.streams;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.processor.internals.StreamThreadMetricsRegistry;

public class StreamsMetricsRegistry {

    public StreamThreadMetricsRegistry streamThreadMetrics;
    
    public StreamsMetricsRegistry() {
        this.streamThreadMetrics = new StreamThreadMetricsRegistry();
    }

    public static void main(String[] args) {
        Set<String> tags = new HashSet<>();
        tags.add("client-id");
        StreamsMetricsRegistry metrics = new StreamsMetricsRegistry();
        System.out.println(Metrics.toHtmlTable("kafka.streams", metrics.getAllTemplates()));
    }

    private List<MetricNameTemplate> getAllTemplates() {
        return this.streamThreadMetrics.getAllTemplates();
    }
}
