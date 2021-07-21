package org.apache.kafka.streams.processor.internals;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

class StreamsThreadTotalBlockedTimeTest {
  @Mock
  Consumer<?, ?> consumer;
  @Mock
  Consumer<?, ?> restoreConsumer;
  @Mock
  Supplier<Double> producerBlocked;

  private StreamsThreadTotalBlockedTime blockedTime;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() {
    blockedTime = new StreamsThreadTotalBlockedTime(consumer, restoreConsumer, producerBlocked);
    Map<MetricName, ? extends Metric> consumerMetrics = new HashMap<>();
    when(consumer.metrics()).thenAnswer(a -> new MetricsBuilder()
        .addMetric("iotime-total", 1)
        .addMetric("io-waittime-total", 2)
        .addMetric("committed-time-total", 3)
        .addMetric("commit-sync-time-total", 4)
        .build()
    );
    when(restoreConsumer.metrics()).thenAnswer(a -> new MetricsBuilder()
        .addMetric("iotime-total", 5)
        .addMetric("io-waittime-total", 6)
        .build()
    );
    when(producerBlocked.get()).thenReturn(7.0);
  }

  @Test
  public void shouldComputeTotalBlockedTime() {
    assertThat(blockedTime.getTotalBlockedTime(), equalTo(1 + 2 + 3 + 4 + 5 + 6 + 7));
  }

  private static class MetricsBuilder {
    private final HashMap<MetricName, Metric> metrics = new HashMap<>();

    private MetricsBuilder addMetric(final String name, final double value) {
      final MetricName metricName = new MetricName(name, "", "", Collections.emptyMap());
      metrics.put(
          metricName,
          new Metric() {
            @Override
            public MetricName metricName() {
              return metricName;
            }

            @Override
            public Object metricValue() {
              return value;
            }
          }
      );
      return this;
    }

    public Map<MetricName, ? extends Metric> build() {
      return Collections.unmodifiableMap(metrics);
    }
  }
}