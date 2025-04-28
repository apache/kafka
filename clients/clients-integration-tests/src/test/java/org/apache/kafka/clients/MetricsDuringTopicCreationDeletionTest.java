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
package org.apache.kafka.clients;

import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;

import org.junit.jupiter.api.BeforeEach;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetricsDuringTopicCreationDeletionTest {

    private static final String TOPIC_NAME_PREFIX = "topic";
    private static final int TOPIC_NUM = 2;
    private static final int CREATE_DELETE_ITERATIONS = 3;
    private static final int REPLICATION_FACTOR = 1;
    private static final int PARTITION_NUM = 3;

    private final ClusterInstance clusterInstance;
    private final List<String> topics;
    private volatile boolean running = true;

    private int initialOfflinePartitionsCount = 0;
    private int initialPreferredReplicaImbalanceCount = 0;
    private int initialUnderReplicatedPartitionCount = 0;

    public MetricsDuringTopicCreationDeletionTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
        this.topics = new ArrayList<>();
        for (int n = 0; n < TOPIC_NUM; n++) {
            topics.add(TOPIC_NAME_PREFIX + n);
        }
    }

    /*
     * Captures initial values of key controller metrics.
     * These will be compared with final values to detect any changes.
     */
    @BeforeEach
    public void setUp() {
        initialOfflinePartitionsCount = getGauge("OfflinePartitionsCount").value();
        initialPreferredReplicaImbalanceCount = getGauge("PreferredReplicaImbalanceCount").value();
        initialUnderReplicatedPartitionCount = getGauge("UnderReplicatedPartitions").value();
    }

    private Closeable runThread() {
        var closed = new AtomicBoolean(false);
        var f = CompletableFuture.runAsync(() -> {
            while (!closed.get()) {
                if (running) {
                    // Get UnderReplicatedPartitions through JMX
                    Optional<Integer> underReplicatedCount = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
                            .filter(entry -> entry.getKey().getName().endsWith("UnderReplicatedPartitions"))
                            .map(entry -> ((Gauge<Integer>) entry.getValue()).value())
                            .findFirst();

                    int count = underReplicatedCount.orElse(0);
                    if (count != initialUnderReplicatedPartitionCount) {
                        running = false;
                    }
                }

                int offlinePartitionsCount = getGauge("OfflinePartitionsCount").value();
                if (offlinePartitionsCount != initialOfflinePartitionsCount) {
                    running = false;
                }

                int preferredReplicaImbalanceCount = getGauge("PreferredReplicaImbalanceCount").value();
                if (preferredReplicaImbalanceCount != initialPreferredReplicaImbalanceCount) {
                    running = false;
                }
            }
        });
        return () -> {
            closed.set(true);
            f.join();
        };
    }

    /*
     * Checking all metrics we care in a single test is faster though it would be more elegant to have 3 @Test methods
     */
    @ClusterTest(
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, value = "true"),
            @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"),
            @ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000"),
            @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
            // speed up the test for UnderReplicatedPartitions, which relies on the ISR expiry thread to execute concurrently with topic creation
            // But the replica.lag.time.max.ms value still need to consider the slow Jenkins testing environment
            @ClusterConfigProperty(key = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_CONFIG, value = "4000")
        }
    )
    public void testMetricsDuringTopicCreateDelete() throws Exception {

        // For OfflinePartitionsCount and PreferredReplicaImbalanceCount even with https://issues.apache.org/jira/browse/KAFKA-4605
        // the test has worked reliably because the metric that gets triggered is the one generated by the first started server (controller)
        final Gauge<Integer> offlinePartitionsCountGauge = getGauge("OfflinePartitionsCount");
        int offlinePartitionsCount = offlinePartitionsCountGauge.value();

        final Gauge<Integer> preferredReplicaImbalanceCountGauge = getGauge("PreferredReplicaImbalanceCount");
        int preferredReplicaImbalanceCount = preferredReplicaImbalanceCountGauge.value();

        // For UnderReplicatedPartitions, because of https://issues.apache.org/jira/browse/KAFKA-4605
        // we can't access the metrics value of each server. So instead we directly invoke the method
        // replicaManager.underReplicatedPartitionCount() that defines the metrics value.
        int underReplicatedPartitionCount = 0;

        // Sanity check: ensure metrics haven't changed before test starts
        assertEquals(initialOfflinePartitionsCount, offlinePartitionsCount);
        assertEquals(initialPreferredReplicaImbalanceCount, preferredReplicaImbalanceCount);
        assertEquals(initialUnderReplicatedPartitionCount, underReplicatedPartitionCount);

        running = true;
        try (var ignored = runThread()) {
            for (int i = 1; i <= CREATE_DELETE_ITERATIONS && running; i++) {
                // Create topics
                for (String topic : topics) {
                    if (!running) break;
                    try {
                        clusterInstance.createTopic(topic, PARTITION_NUM, (short) REPLICATION_FACTOR);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                // Delete topics
                for (String topic : topics) {
                    if (!running) break;
                    try {
                        clusterInstance.deleteTopic(topic);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        offlinePartitionsCount = offlinePartitionsCountGauge.value();
        preferredReplicaImbalanceCount = preferredReplicaImbalanceCountGauge.value();

        assertEquals(initialOfflinePartitionsCount, offlinePartitionsCount,
                "Expect offlinePartitionsCount to be " + initialOfflinePartitionsCount + ", but got: " + offlinePartitionsCount);
        assertEquals(initialPreferredReplicaImbalanceCount, preferredReplicaImbalanceCount,
                "Expect PreferredReplicaImbalanceCount to be " + initialPreferredReplicaImbalanceCount + ", but got: " + preferredReplicaImbalanceCount);
        assertEquals(initialUnderReplicatedPartitionCount, underReplicatedPartitionCount,
                "Expect UnderReplicatedPartitionCount to be " + initialUnderReplicatedPartitionCount + ", but got: " + underReplicatedPartitionCount);
    }

    private Gauge<Integer> getGauge(String metricName) {
        return KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(entry -> entry.getKey().getName().endsWith(metricName))
            .findFirst()
            .map(entry -> (Gauge<Integer>) entry.getValue())
            .orElseThrow(() -> new AssertionError("Unable to find metric " + metricName));
    }
}
