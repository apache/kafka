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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.CO_KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "true"),
        @ClusterConfigProperty(key = "log.unclean.leader.election.enable", value = "false"),
        @ClusterConfigProperty(key = ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0"),
        @ClusterConfigProperty(key = TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, value = "200")
    }
)
public class TransactionsWithMaxInFlightOneTest {
    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "topic2";
    private static final String HEADER_KEY = "transactionStatus";
    private static final byte[] ABORTED_VALUE = "aborted".getBytes();
    private static final byte[] COMMITTED_VALUE = "committed".getBytes();

    @ClusterTest
    public void testTransactionalProducerSingleBrokerMaxInFlightOne(ClusterInstance clusterInstance) throws InterruptedException {
        // We want to test with one broker to verify multiple requests queued on a connection
        assertEquals(1, clusterInstance.brokers().size());

        clusterInstance.createTopic(TOPIC1, 4, (short) 1);
        clusterInstance.createTopic(TOPIC2, 4, (short) 1);

        try (Producer<byte[], byte[]> producer = clusterInstance.producer(Map.of(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-producer",
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1
        ))
        ) {
            producer.initTransactions();

            producer.beginTransaction();
            producer.send(new ProducerRecord<>(TOPIC2, null, "2".getBytes(), "2".getBytes(), Collections.singleton(new RecordHeader(HEADER_KEY, ABORTED_VALUE))));
            producer.send(new ProducerRecord<>(TOPIC1, null, "4".getBytes(), "4".getBytes(), Collections.singleton(new RecordHeader(HEADER_KEY, ABORTED_VALUE))));
            producer.flush();
            producer.abortTransaction();

            producer.beginTransaction();
            producer.send(new ProducerRecord<>(TOPIC1, null, "1".getBytes(), "1".getBytes(), Collections.singleton(new RecordHeader(HEADER_KEY, COMMITTED_VALUE))));
            producer.send(new ProducerRecord<>(TOPIC2, null, "3".getBytes(), "3".getBytes(), Collections.singleton(new RecordHeader(HEADER_KEY, COMMITTED_VALUE))));
            producer.commitTransaction();

            for (GroupProtocol groupProtocol : clusterInstance.supportedGroupProtocols()) {
                ArrayList<ConsumerRecord<byte[], byte[]>> consumerRecords = new ArrayList<>();
                try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(
                        ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name(),
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"
                    )
                )) {
                    consumer.subscribe(List.of(TOPIC1, TOPIC2));
                    TestUtils.waitForCondition(() -> {
                        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                        records.forEach(consumerRecords::add);
                        return consumerRecords.size() == 2;
                    }, 15_000, () -> "Consumer with protocol " + groupProtocol.name + " should consume 2 records, but get " + consumerRecords.size());
                }
                consumerRecords.forEach(record -> {
                    Iterator<Header> headers = record.headers().headers(HEADER_KEY).iterator();
                    assertTrue(headers.hasNext());
                    Header header = headers.next();
                    assertArrayEquals(COMMITTED_VALUE, header.value(), "Record does not have the expected header value");
                });
            }
        }
    }
}
