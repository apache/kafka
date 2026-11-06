/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.metrics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.log.metrics.BrokerTopicMetrics;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.management.ObjectName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = Type.KRAFT,
    brokers = 2,
    serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.NUM_PARTITIONS_CONFIG, value = "2"),
        @ClusterConfigProperty(key = ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "5"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0"),
        @ClusterConfigProperty(key = JmxReporter.EXCLUDE_CONFIG, value = MetricsTest.KAFKA_SERVER_PREFIX + "=ClusterId")
    }
)
public class MetricsTest {
    static final String KAFKA_SERVER_PREFIX = "kafka.server:type=KafkaServer,name";
    private static final int NUM_MESSAGES = 2;

    private final ClusterInstance cluster;

    MetricsTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest
    public void testMetricsReporterAfterDeletingTopic() {
        var topic = "test-topic-metric";
        cluster.createTopic(topic, 1, (short) 1);
        cluster.deleteTopic(topic);
        assertEquals(Set.of(), topicMetricGroups(topic), "Topic metrics exists after deleteTopic");
    }

    @ClusterTest
    public void testBrokerTopicMetricsUnregisteredAfterDeletingTopic() throws Exception {
        var topic = "test-broker-topic-metric";
        cluster.createTopic(topic, 2, (short) 1);
        // Produce a few messages to create the metrics.
        // Don't consume messages as it may cause metrics to be re-created causing the test to fail, see KAFKA-5238.
        produceMessages(topic);
        assertFalse(topicMetricGroups(topic).isEmpty(), "Topic metrics don't exist");
        cluster.brokers().values().forEach(broker -> assertNotNull(broker.brokerTopicStats().topicStats(topic)));
        cluster.deleteTopic(topic);
        assertEquals(Set.of(), topicMetricGroups(topic), "Topic metrics exists after deleteTopic");
    }

    @ClusterTest
    public void testClusterIdMetric() {
        assertEquals(1, metricCount(KAFKA_SERVER_PREFIX + "=ClusterId"));
    }

    @ClusterTest
    public void testBrokerStateMetric() {
        assertEquals(1, metricCount(KAFKA_SERVER_PREFIX + "=BrokerState"));
    }

    @ClusterTest
    public void testYammerMetricsCountMetric() {
        assertEquals(1, metricCount(KAFKA_SERVER_PREFIX + "=yammer-metrics-count"));
    }

    @ClusterTest
    public void testLinuxIoMetrics() {
        // The linux-disk-* metrics exist only when I/O metrics can be collected on this platform.
        int expectedCount = new LinuxIoMetricsCollector("/proc", Time.SYSTEM).usable() ? 1 : 0;
        Set.of(
            "linux-disk-read-bytes",
            "linux-disk-write-bytes",
            "linux-disk-rchar",
            "linux-disk-wchar",
            "linux-disk-syscr",
            "linux-disk-syscw",
            "linux-disk-cancelled-write-bytes"
        ).forEach(name -> assertEquals(expectedCount, metricCount(KAFKA_SERVER_PREFIX + "=" + name)));
    }

    @ClusterTest
    public void testJMXFilter() throws Exception {
        assertTrue(ManagementFactory.getPlatformMBeanServer().isRegistered(
            new ObjectName("kafka.controller:type=KafkaController,name=ActiveControllerCount")));
        assertFalse(ManagementFactory.getPlatformMBeanServer().isRegistered(new ObjectName(KAFKA_SERVER_PREFIX + "=ClusterId")));
    }

    @ClusterTest
    public void testUpdateJMXFilter(ClusterInstance cluster) throws Exception {
        // Verify previously exposed metrics are removed and existing matching metrics are added.
        cluster.brokers().values().forEach(broker -> broker.kafkaYammerMetrics().reconfigure(
            Map.of(JmxReporter.EXCLUDE_CONFIG, "kafka.controller:type=KafkaController,name=ActiveControllerCount")));
        assertFalse(ManagementFactory.getPlatformMBeanServer().isRegistered(
            new ObjectName("kafka.controller:type=KafkaController,name=ActiveControllerCount")));
        assertTrue(ManagementFactory.getPlatformMBeanServer().isRegistered(new ObjectName(KAFKA_SERVER_PREFIX + "=ClusterId")));
    }

    @ClusterTest
    public void testGeneralBrokerTopicMetricsAreGreedilyRegistered(ClusterInstance cluster) throws Exception {
        var topic = "test-broker-topic-metric";
        cluster.createTopic(topic, 2, (short) 1);

        // The broker metrics for all topics should be greedily registered.
        assertFalse(topicMetrics().isEmpty(), "General topic metrics don't exist");
        var broker = cluster.brokers().values().iterator().next();
        assertEquals(broker.brokerTopicStats().allTopicsStats().metricMapKeySet().size(), topicMetrics().size());
        assertEquals(0, broker.brokerTopicStats().allTopicsStats().metricGaugeMap().size());
        // Topic metrics should be lazily registered.
        assertTrue(topicMetricGroups(topic).isEmpty(), "Topic metrics aren't lazily registered");
        produceMessages(topic);
        assertFalse(topicMetricGroups(topic).isEmpty(), "Topic metrics aren't registered");
    }

    @ClusterTest
    public void testWindowsStyleTagNames() {
        var path = "C:\\windows-path\\kafka-logs";
        var tags = Map.of("dir", path);
        var expectedMBeanName = "dir=" + ObjectName.quote(path);

        // Changing the package or class name may cause incompatibility with existing code and metrics configuration.
        var metric = new KafkaMetricsGroup("kafka.metrics", "MetricsTest").metricName("test-metric", tags);
        assertTrue(metric.getMBeanName().endsWith(expectedMBeanName));
    }

    @ClusterTest
    public void testClassicConsumerBrokerTopicMetricsBytesInOut() throws Exception {
        testBrokerTopicMetricsBytesInOut(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerBrokerTopicMetricsBytesInOut() throws Exception {
        testBrokerTopicMetricsBytesInOut(GroupProtocol.CONSUMER);
    }

    private void testBrokerTopicMetricsBytesInOut(GroupProtocol groupProtocol) throws Exception {
        var topic = "test-bytes-in-out";
        var replicationBytesIn = BrokerTopicMetrics.REPLICATION_BYTES_IN_PER_SEC;
        var replicationBytesOut = BrokerTopicMetrics.REPLICATION_BYTES_OUT_PER_SEC;
        var bytesIn = BrokerTopicMetrics.BYTES_IN_PER_SEC + ",topic=" + topic;
        var bytesOut = BrokerTopicMetrics.BYTES_OUT_PER_SEC + ",topic=" + topic;

        cluster.createTopic(topic, 1, (short) 2, Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"));
        produceMessages(topic);

        // Check log sizes to distinguish replication failures from metrics failures.
        var topicPartition = new TopicPartition(topic, 0);
        for (var broker : cluster.brokers().values()) {
            var log = broker.logManager().getLog(topicPartition, false);
            assertTrue(
                log.isPresent() && !log.get().isEmpty(),
                "Expected broker " + broker.config().brokerId() + " to have a Log for " + topicPartition + " with positive size, actual: " + log
            );
        }

        consumeMessages(topic, groupProtocol);
        long initialReplicationBytesIn = meterCount(replicationBytesIn);
        long initialReplicationBytesOut = meterCount(replicationBytesOut);
        long initialBytesIn = meterCount(bytesIn);
        long initialBytesOut = meterCount(bytesOut);

        produceMessages(topic);
        assertTrue(meterCount(replicationBytesIn) > initialReplicationBytesIn);
        assertTrue(meterCount(replicationBytesOut) > initialReplicationBytesOut);
        assertTrue(meterCount(bytesIn) > initialBytesIn);
        // BytesOut doesn't include replication, so it shouldn't have changed.
        assertEquals(initialBytesOut, meterCount(bytesOut));

        consumeMessages(topic, groupProtocol);
        assertTrue(meterCount(bytesOut) > initialBytesOut);
    }

    @ClusterTest
    public void testKRaftControllerMetrics() {
        Set.of(
            "kafka.controller:type=KafkaController,name=ActiveControllerCount",
            "kafka.controller:type=KafkaController,name=GlobalPartitionCount",
            "kafka.controller:type=KafkaController,name=GlobalTopicCount",
            "kafka.controller:type=KafkaController,name=LastAppliedRecordLagMs",
            "kafka.controller:type=KafkaController,name=LastAppliedRecordOffset",
            "kafka.controller:type=KafkaController,name=LastAppliedRecordTimestamp",
            "kafka.controller:type=KafkaController,name=LastCommittedRecordOffset",
            "kafka.controller:type=KafkaController,name=MetadataErrorCount",
            "kafka.controller:type=KafkaController,name=OfflinePartitionsCount",
            "kafka.controller:type=KafkaController,name=PreferredReplicaImbalanceCount"
        ).forEach(expected -> assertEquals(1, metricCount(expected), "Unable to find " + expected));
    }

    private long metricCount(String mBeanName) {
        return KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
            .filter(name -> name.getMBeanName().equals(mBeanName)).count();
    }

    private Set<String> topicMetrics() {
        return filterByTopicMetricRegex(KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
            .map(MetricName::getMBeanName).collect(Collectors.toSet()), "");
    }

    private Set<String> topicMetricGroups(String topic) {
        return filterByTopicMetricRegex(KafkaYammerMetrics.defaultRegistry().groupedMetrics(MetricPredicate.ALL).keySet(), "(" + topic + ")$");
    }

    private Set<String> filterByTopicMetricRegex(Collection<String> metrics, String topicPattern) {
        var pattern = Pattern.compile(".*BrokerTopicMetrics.*" + topicPattern);
        return metrics.stream().filter(name -> pattern.matcher(name).matches()).collect(Collectors.toSet());
    }

    private long meterCount(String metricName) {
        var metric = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(entry -> entry.getKey().getMBeanName().endsWith(metricName))
            .map(Map.Entry::getValue).findFirst()
            .orElseThrow(() -> new AssertionError("Unable to find metric " + metricName));
        return ((Meter) metric).count();
    }

    private void produceMessages(String topic) throws Exception {
        try (var producer = cluster.<byte[], byte[]>producer()) {
            for (int i = 0; i < NUM_MESSAGES; i++) {
                var key = ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
                producer.send(new ProducerRecord<>(topic, key, ("test-" + i).getBytes(StandardCharsets.UTF_8))).get();
            }
        }
    }

    private void consumeMessages(String topic, GroupProtocol groupProtocol) throws Exception {
        try (var consumer = cluster.<byte[], byte[]>consumer(Map.of(
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name,
            ConsumerConfig.GROUP_ID_CONFIG, "group"))) {
            consumer.subscribe(Set.of(topic));
            int[] received = {0};
            TestUtils.waitForCondition(() -> {
                received[0] += consumer.poll(Duration.ofMillis(100)).count();
                return received[0] >= NUM_MESSAGES;
            }, "Failed to consume " + NUM_MESSAGES + " records");
            assertEquals(NUM_MESSAGES, received[0], "Consumed more records than expected");
        }
    }
}
