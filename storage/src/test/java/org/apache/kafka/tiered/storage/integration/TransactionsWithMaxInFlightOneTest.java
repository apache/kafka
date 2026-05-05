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
package org.apache.kafka.tiered.storage.integration;

import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.tiered.storage.integration.TransactionsTestHelper.TOPIC1;
import static org.apache.kafka.tiered.storage.integration.TransactionsTestHelper.TOPIC2;
import static org.apache.kafka.tiered.storage.integration.TransactionsTestHelper.producerRecordWithExpectedTransactionStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;

// Placed in the storage module to avoid a cross-module dependency from clients-integration-tests on storage.
@ClusterTestDefaults(
    types = {Type.CO_KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "true"),
        @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = "group.initial.rebalance.delay.ms", value = "0"),
        @ClusterConfigProperty(key = TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, value = "200")
    }
)
public class TransactionsWithMaxInFlightOneTest {

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
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "2", false));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "4", "4", false));
            producer.flush();
            producer.abortTransaction();

            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", true));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "3", "3", true));
            producer.commitTransaction();

            for (GroupProtocol groupProtocol : clusterInstance.supportedGroupProtocols()) {
                try (var consumer = TransactionsTestHelper.createReadCommittedConsumer(
                        clusterInstance, groupProtocol, "max-inflight-" + groupProtocol.name())) {
                    consumer.subscribe(List.of(TOPIC1, TOPIC2));
                    var records = TransactionsTestHelper.consumeRecords(consumer, 2);
                    records.forEach(TransactionsTestHelper::assertCommittedAndGetValue);
                }
            }
        }
    }
}
