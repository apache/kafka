package org.apache.kafka.clients.producer.internals;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

public class MetricsRegistry {

    protected final Metrics metrics;
    protected final Set<String> tags;
    protected final List<MetricNameTemplate> allTemplates;

    public MetricsRegistry(Metrics metrics) {
        this.metrics = metrics;
        this.tags = this.metrics.config().tags().keySet();
        this.allTemplates = new ArrayList<MetricNameTemplate>();
    }

    protected MetricName createMetricName(String name, String group, String description, Set<String> tags) {
        return this.metrics.metricInstance(createTemplate(name, group, description, tags));
    }

    private MetricNameTemplate createTemplate(String name, String group, String description, Set<String> tags) {
        MetricNameTemplate template = new MetricNameTemplate(name, group, description, tags);
        this.allTemplates.add(template);
        return template;
    }

    public List<MetricNameTemplate> getAllTemplates() {
        return allTemplates;
    }

    public Sensor sensor(String name) {
        return this.metrics.sensor(name);
    }

    public void addMetric(MetricName m, Measurable measurable) {
        this.metrics.addMetric(m, measurable);
    }

}