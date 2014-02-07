package org.apache.kafka.common.metrics;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.utils.Time;

public final class KafkaMetric implements Metric {

    private final String name;
    private final String description;
    private final Object lock;
    private final Time time;
    private final Measurable measurable;
    private MetricConfig config;

    KafkaMetric(Object lock, String name, String description, Measurable measurable, MetricConfig config, Time time) {
        super();
        this.name = name;
        this.description = description;
        this.lock = lock;
        this.measurable = measurable;
        this.config = config;
        this.time = time;
    }

    MetricConfig config() {
        return this.config;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String description() {
        return this.description;
    }

    @Override
    public double value() {
        synchronized (this.lock) {
            return value(time.nanoseconds());
        }
    }

    double value(long time) {
        return this.measurable.measure(config, time);
    }

    public void config(MetricConfig config) {
        synchronized (lock) {
            this.config = config;
        }
    }
}
