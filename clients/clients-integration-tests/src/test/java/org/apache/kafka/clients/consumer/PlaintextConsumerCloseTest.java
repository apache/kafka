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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.BROKER_COUNT;
import static org.apache.kafka.clients.ClientsTestUtils.consumeRecords;
import static org.apache.kafka.clients.ClientsTestUtils.sendRecords;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
        @ClusterConfigProperty(key = GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, value = "60000"),
        @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "10"),
    }
)
public class PlaintextConsumerCloseTest {

    private final ClusterInstance cluster;

    public PlaintextConsumerCloseTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest
    public void testClassicConsumerCloseWithDefaultTakesAtLeastFetchMaxWaitMs() throws Exception {
        testCloseWithDefaultTakesAtLeastFetchMaxWaitMs(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerCloseWithDefaultTakesAtLeastFetchMaxWaitMs() throws Exception {
        testCloseWithDefaultTakesAtLeastFetchMaxWaitMs(GroupProtocol.CONSUMER);
    }

    private void testCloseWithDefaultTakesAtLeastFetchMaxWaitMs(GroupProtocol groupProtocol) throws Exception {
        long closeMs = calculateConsumerCloseDelay(groupProtocol, Consumer::close);
        assertTrue(
            closeMs >= DEFAULT_FETCH_MAX_WAIT_MS,
            "Closing a consumer with the default close() should take longer than " + DEFAULT_FETCH_MAX_WAIT_MS + " ms, but actually took " + closeMs + " ms"
        );
    }

    @ClusterTest
    public void testClassicConsumerCloseWithTimeoutIgnoresFetchMaxWaitMs() throws Exception {
        testCloseWithTimeoutIgnoresFetchMaxWaitMs(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerCloseWithTimeoutIgnoresFetchMaxWaitMs() throws Exception {
        testCloseWithTimeoutIgnoresFetchMaxWaitMs(GroupProtocol.CONSUMER);
    }

    private void testCloseWithTimeoutIgnoresFetchMaxWaitMs(GroupProtocol groupProtocol) throws Exception {
        long timeoutMs = 100;

        // Close the Consumer with a specified timeout that's much shorter than the default fetch timeout
        // to ensure the fetch.max.wait.ms is effectively ignored.
        long closeMs = calculateConsumerCloseDelay(
            groupProtocol,
            c -> c.close(CloseOptions.timeout(Duration.ofMillis(timeoutMs)))
        );
        assertTrue(
            closeMs <= DEFAULT_FETCH_MAX_WAIT_MS,
            "Closing a consumer with a timeout of " + timeoutMs + " ms should take less than " + DEFAULT_FETCH_MAX_WAIT_MS + " ms, but actually took " + closeMs + " ms"
        );
    }

    private long calculateConsumerCloseDelay(GroupProtocol groupProtocol,
                                             java.util.function.Consumer<Consumer<byte[], byte[]>> closeOperation) throws Exception {
        Map<String, Object> consumerConfig = Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            AUTO_OFFSET_RESET_CONFIG, "earliest",
            GROUP_ID_CONFIG, "group_test",
            BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()
        );
        var topicName = "calculate-consumer-close-delay";
        var numRecords = 100;
        cluster.createTopic(topicName, 2, (short) 2, Map.of());

        sendRecords(cluster, new TopicPartition(topicName, 0), numRecords);

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)) {
            consumer.subscribe(Set.of(topicName));
            consumeRecords(consumer, numRecords);

            long start = System.currentTimeMillis();
            closeOperation.accept(consumer);
            long end = System.currentTimeMillis();
            return end - start;
        }
    }
}
