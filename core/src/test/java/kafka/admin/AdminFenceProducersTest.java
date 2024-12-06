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

package kafka.admin;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.FenceProducersOptions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig;
import org.apache.kafka.server.config.ServerLogConfigs;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(serverProperties = {
    @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
    @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, value = "1"),
    @ClusterConfigProperty(key = TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, value = "2000")
})
@ExtendWith(ClusterTestExtensions.class)
public class AdminFenceProducersTest {
    private static final String TOPIC_NAME = "mytopic";
    private static final String TXN_ID = "mytxnid";
    private static final String INCORRECT_BROKER_PORT = "225";
    private static final ProducerRecord<byte[], byte[]> RECORD = new ProducerRecord<>(TOPIC_NAME, null, new byte[1]);
    private final ClusterInstance clusterInstance;

    AdminFenceProducersTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    private KafkaProducer<byte[], byte[]> createProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TXN_ID);
        config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "2000");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return new KafkaProducer<>(config);
    }

    @ClusterTest
    void testFenceAfterProducerCommit() throws Exception {
        clusterInstance.createTopic(TOPIC_NAME, 1, (short) 1);

        try (KafkaProducer<byte[], byte[]> producer = createProducer();
             Admin adminClient = clusterInstance.admin()) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(RECORD).get();
            producer.commitTransaction();

            adminClient.fenceProducers(Collections.singletonList(TXN_ID)).all().get();

            producer.beginTransaction();
            ExecutionException exceptionDuringSend = assertThrows(
                    ExecutionException.class,
                    () -> producer.send(RECORD).get(), "expected InvalidProducerEpochException"
            );

            // In Transaction V2, the ProducerFencedException will be converted to InvalidProducerEpochException when
            // coordinator handles AddPartitionRequest.
            assertInstanceOf(InvalidProducerEpochException.class, exceptionDuringSend.getCause());

            // InvalidProducerEpochException is treated as fatal error. The commitTransaction will return this last
            // fatal error.
            assertThrows(InvalidProducerEpochException.class, producer::commitTransaction);
        }
    }

    @ClusterTest
    void testFenceProducerTimeoutMs() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + INCORRECT_BROKER_PORT);

        try (Admin adminClient = clusterInstance.admin(config)) {
            ExecutionException exception = assertThrows(
                    ExecutionException.class, () ->
                            adminClient.fenceProducers(Collections.singletonList(TXN_ID), new FenceProducersOptions().timeoutMs(0)).all().get());
            assertInstanceOf(TimeoutException.class, exception.getCause());
        }
    }

    @ClusterTest
    void testFenceBeforeProducerCommit() throws Exception {
        clusterInstance.createTopic(TOPIC_NAME, 1, (short) 1);

        try (KafkaProducer<byte[], byte[]> producer = createProducer();
             Admin adminClient = clusterInstance.admin()) {

            producer.initTransactions();
            producer.beginTransaction();
            producer.send(RECORD).get();

            adminClient.fenceProducers(Collections.singletonList(TXN_ID)).all().get();

            ExecutionException exceptionDuringSend = assertThrows(
                    ExecutionException.class, () ->
                            producer.send(RECORD).get(), "expected ProducerFencedException"
            );
            assertTrue(exceptionDuringSend.getCause() instanceof ProducerFencedException ||
                    exceptionDuringSend.getCause() instanceof InvalidProducerEpochException);

            ApiException exceptionDuringCommit = assertThrows(
                    ApiException.class,
                    producer::commitTransaction, "Expected Exception"
            );
            assertTrue(exceptionDuringCommit instanceof ProducerFencedException ||
                    exceptionDuringCommit instanceof InvalidProducerEpochException);
        }
    }
}