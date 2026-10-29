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
package org.apache.kafka.server;

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.metadata.LeaderAndIsr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    /**
     * Wait until a valid leader is propagated to the metadata cache in each broker.
     * It assumes that the leader propagated to each broker is the same.
     *
     * @param brokers The list of brokers that the metadata should reach
     * @param topic The topic name
     * @param partition The partitionId
     * @return The metadata of the partition.
     */
    public static LeaderAndIsr waitForPartitionMetadata(Collection<KafkaBroker> brokers, String topic, int partition) throws Exception {
        if (brokers.isEmpty()) {
            throw new IllegalArgumentException("Empty broker list");
        }
        waitForCondition(
                () -> brokers.stream().allMatch(broker -> {
                    Optional<LeaderAndIsr> leaderAndIsr = broker.metadataCache().getLeaderAndIsr(topic, partition);
                    return leaderAndIsr.filter(andIsr -> FetchRequest.isValidBrokerId(andIsr.leader())).isPresent();
                }),
                DEFAULT_MAX_WAIT_MS,
                "Partition [" + topic + "," + partition + "] metadata not propagated after " + DEFAULT_MAX_WAIT_MS + " ms");

        return brokers.iterator().next().metadataCache().getLeaderAndIsr(topic, partition).orElseThrow(() ->
                new IllegalStateException("Cannot get topic: " + topic + ", partition: " + partition + " in server metadata cache"));
    }

    private static <K, V> List<ConsumerRecord<K, V>> pollUntilAtLeastNumRecords(Consumer<K, V> consumer, int numRecords) throws Exception {
        final List<ConsumerRecord<K, V>> records = new ArrayList<>();
        Function<ConsumerRecords<K, V>, Boolean> pollAction = polledRecords -> {
            for (ConsumerRecord<K, V> polledRecord : polledRecords) {
                records.add(polledRecord);
            }
            return records.size() >= numRecords;
        };
        pollRecordsUntilTrue(consumer, pollAction,
                () -> "Consumed " + records.size() + " records before timeout instead of the expected " + numRecords + " records");
        return records;
    }

    /**
     * Wait for the consumer to consumer numRecords records
     *
     * @param consumer The consumer instance
     * @param numRecords The number of records to consume
     * @return The list of consumed records
     * @throws Exception if the consumer can't consume numRecords
     */
    public static <K, V> List<ConsumerRecord<K, V>> consumeRecords(Consumer<K, V> consumer, int numRecords) throws Exception {
        List<ConsumerRecord<K, V>> records = pollUntilAtLeastNumRecords(consumer, numRecords);
        assertEquals(numRecords, records.size(), "Consumed more records than expected");
        return records;
    }

    private static <K, V>  void pollRecordsUntilTrue(Consumer<K, V> consumer, Function<ConsumerRecords<K, V>, Boolean> action, Supplier<String> msg) throws Exception {
        waitForCondition(() -> action.apply(consumer.poll(Duration.ofMillis(100L))), DEFAULT_MAX_WAIT_MS, msg.get());
    }

    /**
     * Produce the message to the specified topic
     *
     * @param cluster The ClusterInstance to retrieve a producer for
     * @param topic The topic name, as used as the record key
     * @param message The value of the record
     * @param deliveryTimeoutMs The delivery.timeout.ms configuration
     * @param requestTimeoutMs The request.timeout.ms configuration
     * @throws Exception Any exception thrown by {@link Producer#send(ProducerRecord)}
     */
    public static void produceMessage(ClusterInstance cluster, String topic, String message, int deliveryTimeoutMs, int requestTimeoutMs) throws Exception {
        try (Producer<String, String> producer = cluster.producer(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.LINGER_MS_CONFIG, 0,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs
        ))) {
            producer.send(new ProducerRecord<>(topic, topic, message)).get();
        }
    }

    /**
     * Produce the message to the specified topic
     *
     * @param cluster The ClusterInstance to use
     * @param topic The topic name, as used as the record key
     * @param message The value of the record
     * @throws Exception Any exception thrown by {@link Producer#send(ProducerRecord)}
     */
    public static void produceMessage(ClusterInstance cluster, String topic, String message) throws Exception {
        produceMessage(cluster, topic, message, 30 * 1000, 20 * 1000);
    }

    /**
     * Find the current leader or wait for the optionally specified expected leader
     *
     * @param cluster The ClusterInstance to use
     * @param tp The topic partition to check the leader for
     * @param expectedLeaderOpt The new expected leader
     * @return The current leader for the topic partition
     * @throws InterruptedException If waitForCondition is interrupted
     */
    public static int awaitLeaderChange(ClusterInstance cluster, TopicPartition tp, Optional<Integer> expectedLeaderOpt) throws InterruptedException {
        return awaitLeaderChange(cluster, tp, expectedLeaderOpt, DEFAULT_MAX_WAIT_MS);
    }

    /**
     * Find the current leader or wait for the optionally specified expected leader
     *
     * @param cluster The ClusterInstance to use
     * @param tp The topic partition to check the leader for
     * @param expectedLeaderOpt The new expected leader
     * @param timeout The duration in ms to wait for the leader
     * @return The current leader for the topic partition
     * @throws InterruptedException If waitForCondition is interrupted
     */
    public static int awaitLeaderChange(ClusterInstance cluster, TopicPartition tp, Optional<Integer> expectedLeaderOpt, long timeout) throws InterruptedException {
        if (expectedLeaderOpt.isPresent()) {
            LOG.debug("Checking leader that has changed to {}", expectedLeaderOpt.get());
        } else {
            LOG.debug("Checking the elected leader");
        }

        Supplier<Optional<Integer>> newLeaderExists = () -> cluster.brokers().values().stream()
                .filter(broker -> expectedLeaderOpt.isEmpty() || broker.config().brokerId() == expectedLeaderOpt.get())
                .filter(broker -> broker.replicaManager().onlinePartition(tp).exists(partition -> partition.leaderLogIfLocal().isDefined()))
                .map(broker -> broker.config().brokerId())
                .findFirst();

        waitForCondition(() -> newLeaderExists.get().isPresent(),
                timeout, "Did not observe leader change for partition " + tp + " after " + timeout + " ms");

        return newLeaderExists.get().get();
    }
}
