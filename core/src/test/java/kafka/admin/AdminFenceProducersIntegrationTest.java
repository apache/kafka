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

import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.FenceProducersOptions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ClusterTestDefaults(serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, value = "2000")
})
@ExtendWith(ClusterTestExtensions.class)
public class AdminFenceProducersIntegrationTest {
    private static final String TOPIC_NAME = "mytopic";
    private static final String TXN_ID = "mytxnid";
    private static final String INCORRECT_BROKER_PORT = "225";
    private static final ProducerRecord<byte[], byte[]> RECORD = new ProducerRecord<>(TOPIC_NAME, null, new byte[1]);
    private final ClusterInstance clusterInstance;
    private Admin adminClient;
    private KafkaProducer<byte[], byte[]> producer;

    AdminFenceProducersIntegrationTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    private KafkaProducer<byte[], byte[]> createProducer() {
        Properties config = new Properties();
        config.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TXN_ID);
        config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "2000");
        config.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return new KafkaProducer<>(config);
    }

    @ClusterTest
    void testFenceAfterProducerCommit() throws Exception {
        clusterInstance.createTopic(TOPIC_NAME, 1, (short) 1);

        producer = createProducer();
        producer.initTransactions();
        producer.beginTransaction();
        producer.send(RECORD).get();
        producer.commitTransaction();

        adminClient = clusterInstance.createAdminClient();
        adminClient.fenceProducers(Collections.singletonList(TXN_ID)).all().get();

        producer.beginTransaction();
        try {
            producer.send(RECORD).get();
            fail("expected ProducerFencedException");
        } catch (ProducerFencedException e) {
            // expected
        } catch (ExecutionException e) {
            assertInstanceOf(ProducerFencedException.class, e.getCause());
        }

        assertThrows(ProducerFencedException.class, producer::commitTransaction);

        adminClient.close();
        producer.close();
    }

    @ClusterTest
    @Timeout(30)
    void testFenceProducerTimeoutMs() {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + INCORRECT_BROKER_PORT);
        adminClient = clusterInstance.createAdminClient(config);
        try {
            ExecutionException e = assertThrows(ExecutionException.class, () ->
                    adminClient.fenceProducers(Collections.singletonList(TXN_ID), new FenceProducersOptions().timeoutMs(0)).all().get());
            assertInstanceOf(TimeoutException.class, e.getCause());
        } finally {
            adminClient.close(Duration.ofSeconds(0));
        }

    }

    @ClusterTest
    void testFenceBeforeProducerCommit() throws Exception {
        clusterInstance.createTopic(TOPIC_NAME, 1, (short) 1);
        producer = createProducer();
        producer.initTransactions();
        producer.beginTransaction();
        producer.send(RECORD).get();

        adminClient = clusterInstance.createAdminClient();
        adminClient.fenceProducers(Collections.singletonList(TXN_ID)).all().get();

        try {
            producer.send(RECORD).get();
            fail("expected Exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ProducerFencedException ||
                            e.getCause() instanceof InvalidProducerEpochException,
                    "Unexpected ExecutionException cause " + e.getCause());
        }

        try {
            producer.commitTransaction();
            fail("expected Exception");
        } catch (ProducerFencedException | InvalidProducerEpochException e) {
            // expected
        }
        adminClient.close();
        producer.close();
    }
}