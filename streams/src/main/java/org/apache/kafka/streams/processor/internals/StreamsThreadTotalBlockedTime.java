package org.apache.kafka.streams.processor.internals;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

public class StreamsThreadTotalBlockedTime {
  final Consumer<?, ?> consumer;
  final Consumer<?, ?> restoreConsumer;
  final Supplier<Double> producerTotalBlockedTime;

  StreamsThreadTotalBlockedTime(
      Consumer<?, ?> consumer,
      Consumer<?, ?> restoreConsumer,
      Supplier<Double> producerTotalBlockedTime
  ) {
    this.consumer = consumer;
    this.restoreConsumer = restoreConsumer;
    this.producerTotalBlockedTime = producerTotalBlockedTime;
  }

  final double getMetricValue(Map<MetricName, ? extends Metric> metrics, String name) {
    return metrics.keySet().stream()
        .filter(n -> n.name().equals(name))
        .findFirst()
        .map(n -> (Double) metrics.get(n).metricValue())
        .orElse(0.0);
  }

  public double getTotalBlockedTime() {
    return getMetricValue(consumer.metrics(), "io-waittime-total")
        + getMetricValue(consumer.metrics(), "iotime-total")
        + getMetricValue(consumer.metrics(), "committed-time-total")
        + getMetricValue(consumer.metrics(), "commit-sync-time-total")
        + getMetricValue(restoreConsumer.metrics(), "io-waittime-total")
        + getMetricValue(restoreConsumer.metrics(), "iotime-total")
        + producerTotalBlockedTime.get();
  }
}
