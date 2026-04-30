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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetricsDuringTopicCreationDeletionTest {

    private static final String TOPIC_NAME_PREFIX = "topic";
    private static final int TOPIC_NUM = 2;
    private static final int CREATE_DELETE_ITERATIONS = 3;
    private static final short REPLICATION_FACTOR = 1;
    private static final int PARTITION_NUM = 3;

    private final ClusterInstance clusterInstance;
    private final List<String> topics;
    private volatile boolean running = true;

    public MetricsDuringTopicCreationDeletionTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
        this.topics = new ArrayList<>();
        for (int n = 0; n < TOPIC_NUM; n++) {
            topics.add(TOPIC_NAME_PREFIX + n);
        }
    }

    /*
     * Checking all metrics we care in a single test is faster though it would be more elegant to have 3 @Test methods
     */
    @ClusterTest(
        types = {Type.KRAFT},
        brokers = 1,
        serverProperties = {
            @ClusterConfigProperty(key = ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, value = "true"),
            @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"),
            @ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000"),
            @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
            // speed up the test for UnderReplicatedPartitions, which relies on the ISR expiry thread to execute concurrently with topic creation
            // But the replica.lag.time.max.ms value still need to consider the slow testing environment
            @ClusterConfigProperty(key = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_CONFIG, value = "4000")
        }
    )
    public void testMetricsDuringTopicCreateDelete() throws Exception {

        final int initialOfflinePartitionsCount = getGauge("OfflinePartitionsCount").value();
        final int initialPreferredReplicaImbalanceCount = getGauge("PreferredReplicaImbalanceCount").value();
        final int initialUnderReplicatedPartitionsCount = getGauge("UnderReplicatedPartitions").value();

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            while (running) {
                int offlinePartitionsCount = getGauge("OfflinePartitionsCount").value();
                int preferredReplicaImbalanceCount = getGauge("PreferredReplicaImbalanceCount").value();
                int underReplicatedPartitionsCount = getGauge("UnderReplicatedPartitions").value();

                if (offlinePartitionsCount != initialOfflinePartitionsCount ||
                    preferredReplicaImbalanceCount != initialPreferredReplicaImbalanceCount ||
                    underReplicatedPartitionsCount != initialUnderReplicatedPartitionsCount) {
                    running = false;
                }

                try {
                    // Avoid busy loop
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException ignored) {

                }
            }
        });

        Closeable runThread = () -> {
            running = false;
            future.join();
        };

        try (runThread) {
            createAndDeleteTopics();
        }

        final int finalOfflinePartitionsCount = getGauge("OfflinePartitionsCount").value();
        final int finalPreferredReplicaImbalanceCount = getGauge("PreferredReplicaImbalanceCount").value();
        final int finalUnderReplicatedPartitionsCount = getGauge("UnderReplicatedPartitions").value();

        assertEquals(initialOfflinePartitionsCount, finalOfflinePartitionsCount,
            "Expect offlinePartitionsCount to be " + initialOfflinePartitionsCount + ", but got: " + finalOfflinePartitionsCount);
        assertEquals(initialPreferredReplicaImbalanceCount, finalPreferredReplicaImbalanceCount,
            "Expect PreferredReplicaImbalanceCount to be " + initialPreferredReplicaImbalanceCount + ", but got: " + finalPreferredReplicaImbalanceCount);
        assertEquals(initialUnderReplicatedPartitionsCount, finalUnderReplicatedPartitionsCount,
            "Expect UnderReplicatedPartitionCount to be " + initialUnderReplicatedPartitionsCount + ", but got: " + finalUnderReplicatedPartitionsCount);
    }

    private void createAndDeleteTopics() {
        for (int i = 1; i <= CREATE_DELETE_ITERATIONS && running; i++) {
            for (String topic : topics) {
                if (!running) return;
                try {
                    clusterInstance.createTopic(topic, PARTITION_NUM, REPLICATION_FACTOR);
                } catch (Exception ignored) { }
            }

            for (String topic : topics) {
                if (!running) return;
                try {
                    clusterInstance.deleteTopic(topic);
                } catch (Exception ignored) { }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Gauge<Integer> getGauge(String metricName) {
        return KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(entry -> entry.getKey().getName().endsWith(metricName))
            .findFirst()
            .map(entry -> (Gauge<Integer>) entry.getValue())
            .orElseThrow(() -> new AssertionError("Unable to find metric " + metricName));
    }
}
