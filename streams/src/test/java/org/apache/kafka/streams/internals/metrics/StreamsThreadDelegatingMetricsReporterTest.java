package org.apache.kafka.streams.internals.metrics;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamsThreadDelegatingMetricsReporterTest {

    private MockConsumer<?,?> mockConsumer;
    private StreamsThreadDelegatingMetricsReporter streamsThreadDelegatingMetricsReporter;

    private KafkaMetric kafkaMetricOneHasThreadIdTag;
    private KafkaMetric kafkaMetricTwoHasThreadIdTag;
    private KafkaMetric kafkaMetricThreeHasThreadIdTag;
    private KafkaMetric kafkaMetricWithoutThreadIdTag;
    private final Object lock = new Object();
    private final MetricConfig metricConfig = new MetricConfig();


    @BeforeEach
    void setUp() {
        Map<String, String> threadIdTagMap = new HashMap<>();
        String threadId = "abcxyz-StreamThread-1";
        threadIdTagMap.put("thread-id", threadId);

        Map<String, String> threadIdWithStateUpdaterTagMap = new HashMap<>();
        String stateUpdaterId = "deftuv-StateUpdater-1";
        threadIdWithStateUpdaterTagMap.put("thread-id", stateUpdaterId);

        Map<String, String> noThreadIdTagMap = new HashMap<>();
        noThreadIdTagMap.put("client-id", "foo");

        mockConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        streamsThreadDelegatingMetricsReporter = new StreamsThreadDelegatingMetricsReporter(mockConsumer, threadId, stateUpdaterId);

        MetricName metricNameOne = new MetricName("metric-one", "test-group-one", "foo bar baz", threadIdTagMap);
        MetricName metricNameTwo = new MetricName("metric-two", "test-group-two", "description two", threadIdWithStateUpdaterTagMap);
        MetricName metricNameThree = new MetricName("metric-three", "test-group-three", "description three", threadIdTagMap);
        MetricName metricNameFour = new MetricName("metric-four", "test-group-three", "description three", noThreadIdTagMap);
        
        kafkaMetricOneHasThreadIdTag = new KafkaMetric(lock, metricNameOne, (Measurable)(m, now) -> 1.0, metricConfig, Time.SYSTEM);
        kafkaMetricTwoHasThreadIdTag = new KafkaMetric(lock, metricNameTwo, (Measurable)(m, now) -> 2.0, metricConfig, Time.SYSTEM);
        kafkaMetricThreeHasThreadIdTag = new KafkaMetric(lock, metricNameThree, (Measurable)(m, now) -> 3.0, metricConfig, Time.SYSTEM);
        kafkaMetricWithoutThreadIdTag = new KafkaMetric(lock, metricNameFour, (Measurable)(m, now) -> 4.0, metricConfig, Time.SYSTEM);
    }
    @AfterEach
    void tearDown() {
        mockConsumer.close();
    }
    

    @Test
    @DisplayName("Init method should register metrics it receives as parameters")
    void shouldInitMetrics() {
         List<KafkaMetric> allMetrics = Arrays.asList(kafkaMetricOneHasThreadIdTag, kafkaMetricTwoHasThreadIdTag, kafkaMetricThreeHasThreadIdTag);
         List<KafkaMetric> expectedMetrics = Arrays.asList(kafkaMetricOneHasThreadIdTag, kafkaMetricTwoHasThreadIdTag, kafkaMetricThreeHasThreadIdTag);
         streamsThreadDelegatingMetricsReporter.init(allMetrics);
         assertEquals(expectedMetrics, mockConsumer.addedMetrics());
    }

    @Test
    @DisplayName("Should register metrics with thread-id in tag map")
    void shouldRegisterMetrics() {
         streamsThreadDelegatingMetricsReporter.metricChange(kafkaMetricOneHasThreadIdTag);
         assertEquals(kafkaMetricOneHasThreadIdTag, mockConsumer.addedMetrics().get(0));
    }

    @Test
    @DisplayName("Should remove metrics")
    void shouldRemoveMetrics() {
        streamsThreadDelegatingMetricsReporter.metricChange(kafkaMetricOneHasThreadIdTag);
        streamsThreadDelegatingMetricsReporter.metricChange(kafkaMetricTwoHasThreadIdTag);
        streamsThreadDelegatingMetricsReporter.metricChange(kafkaMetricThreeHasThreadIdTag);
        List<KafkaMetric> expected = Arrays.asList(kafkaMetricOneHasThreadIdTag, kafkaMetricTwoHasThreadIdTag, kafkaMetricThreeHasThreadIdTag);
        assertEquals(expected, mockConsumer.addedMetrics());
        streamsThreadDelegatingMetricsReporter.metricRemoval(kafkaMetricOneHasThreadIdTag);
        expected = Arrays.asList(kafkaMetricTwoHasThreadIdTag, kafkaMetricThreeHasThreadIdTag);
        assertEquals(expected, mockConsumer.addedMetrics());
    }

    @Test
    @DisplayName("Should not register metrics without thread-id tag")
    void shouldNotRegisterMetricsWithoutThreadIdTag() {
        streamsThreadDelegatingMetricsReporter.metricChange(kafkaMetricWithoutThreadIdTag);
        assertEquals(0, mockConsumer.addedMetrics().size());
    }

    @Test
    @DisplayName("Should set its reference to the consumer to null on closing")
    void shouldSetConsumerToNullOnClose() {
        streamsThreadDelegatingMetricsReporter.close();
        assertThrows(NullPointerException.class,
                () -> streamsThreadDelegatingMetricsReporter.metricChange(kafkaMetricOneHasThreadIdTag));
    }
}