package org.apache.kafka.clients.consumer.internals.metrics;

import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Max;

public class HeartbeatMetrics extends AbstractConsumerMetrics {

    private final Metrics metrics;
    public final Sensor heartbeatSensor;

    public HeartbeatMetrics(Metrics metrics) {
        super();
        this.metrics = metrics;
        heartbeatSensor = metrics.sensor("heartbeat-latency");
        heartbeatSensor.add(metrics.metricName("heartbeat-response-time-max",
                groupMetricsName,
                "The max time taken to receive a response to a heartbeat request"), new Max());
        heartbeatSensor.add(createMeter(metrics, groupMetricsName, "heartbeat", "heartbeats"));

    }

    public void registerMeasurable(String name,
                                   String description,
                                   Measurable measurable) {
        metrics.addMetric(
                metrics.metricName(name, groupMetricsName, description),
                measurable);
    }
}
