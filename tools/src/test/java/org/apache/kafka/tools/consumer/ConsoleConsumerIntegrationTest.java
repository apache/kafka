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
package org.apache.kafka.tools.consumer;

import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(value = ClusterTestExtensions.class)
@Tag("integration")
public class ConsoleConsumerIntegrationTest {

    private final ClusterInstance cluster;
    private final String topic = "test-topic";
    private final String transactionId = "transactional-id";
    private final TransactionLogKey txnLogKey = new TransactionLogKey()
            .setTransactionalId(transactionId);
    private final TransactionLogValue txnLogValue = new TransactionLogValue()
            .setProducerId(100)
            .setProducerEpoch((short) 50)
            .setTransactionStatus((byte) 4)
            .setTransactionStartTimestampMs(750L)
            .setTransactionLastUpdateTimestampMs(1000L)
            .setTransactionTimeoutMs(500)
            .setTransactionPartitions(emptyList());

    public ConsoleConsumerIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT}, brokers = 3)
    public void testTransactionLogMessageFormatter() throws Exception {
        try (Admin admin = cluster.createAdminClient()) {

            List<TransactionLogTestcase> testcases = generateTestcases();
            
            NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            admin.createTopics(singleton(newTopic));
            produceMessages(cluster, testcases);

            String[] transactionLogMessageFormatter = new String[]{
                "--bootstrap-server", cluster.bootstrapServers(),
                "--topic", topic,
                "--partition", "0",
                "--formatter", "org.apache.kafka.tools.consumer.TransactionLogMessageFormatter",
                "--isolation-level", "read_committed",
                "--from-beginning"
            };

            ConsoleConsumerOptions options = new ConsoleConsumerOptions(transactionLogMessageFormatter);
            Consumer<byte[], byte[]> consumer = createConsumer(cluster);
            ConsoleConsumer.ConsumerWrapper consoleConsumer = new ConsoleConsumer.ConsumerWrapper(options, consumer);
            testcases.forEach(testcase -> {
                ConsumerRecord<byte[], byte[]> record = consoleConsumer.receive();
                try (MessageFormatter formatter = new TransactionLogMessageFormatter()) {
                    formatter.configure(emptyMap());
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    formatter.writeTo(record, new PrintStream(out));
                    assertEquals(testcase.expectedOutput, out.toString());
                }
            });
            consoleConsumer.cleanup();
        }
    }

    private void produceMessages(ClusterInstance cluster, List<TransactionLogTestcase> testcases) {
        try (Producer<byte[], byte[]> producer = createProducer(cluster)) {
            producer.initTransactions();
            producer.beginTransaction();
            testcases.forEach(testcase -> producer.send(new ProducerRecord<>(topic, testcase.keyBuffer, testcase.valueBuffer)));
            producer.commitTransaction();
        }
    }

    private Producer<byte[], byte[]> createProducer(ClusterInstance cluster) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ACKS_CONFIG, "all");
        props.put(TRANSACTIONAL_ID_CONFIG, transactionId);
        props.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private Consumer<byte[], byte[]> createConsumer(ClusterInstance cluster) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        props.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    private List<TransactionLogTestcase> generateTestcases() {
        List<TransactionLogTestcase> testcases = new ArrayList<>();
        testcases.add(new TransactionLogTestcase(
                MessageUtil.toVersionPrefixedByteBuffer((short) 10, txnLogKey).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 10, txnLogValue).array(),
                "{\"key\":{\"version\":10,\"data\":\"unknown\"}," +
                        "\"value\":{\"version\":10,\"data\":\"unknown\"}}"
        ));
        testcases.add(new TransactionLogTestcase(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 0, txnLogKey).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 1, txnLogValue).array(),
                        "{\"key\":{\"version\":0,\"data\":{\"transactionalId\":\"transactional-id\"}}," +
                                "\"value\":{\"version\":1,\"data\":{\"producerId\":100,\"producerEpoch\":50," +
                                "\"transactionTimeoutMs\":500,\"transactionStatus\":4,\"transactionPartitions\":[]," +
                                "\"transactionLastUpdateTimestampMs\":1000,\"transactionStartTimestampMs\":750}}}"
        ));
        testcases.add(new TransactionLogTestcase(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 0, txnLogKey).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 5, txnLogValue).array(),
                        "{\"key\":{\"version\":0,\"data\":{\"transactionalId\":\"transactional-id\"}}," +
                                "\"value\":{\"version\":5,\"data\":\"unknown\"}}"
        ));
        testcases.add(new TransactionLogTestcase(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 1, txnLogKey).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 1, txnLogValue).array(),
                        "{\"key\":{\"version\":1,\"data\":\"unknown\"}," +
                                "\"value\":{\"version\":1,\"data\":{\"producerId\":100,\"producerEpoch\":50," +
                                "\"transactionTimeoutMs\":500,\"transactionStatus\":4,\"transactionPartitions\":[]," +
                                "\"transactionLastUpdateTimestampMs\":1000,\"transactionStartTimestampMs\":750}}}"
        ));
        testcases.add(new TransactionLogTestcase(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 0, txnLogKey).array(),
                        null,
                        "{\"key\":{\"version\":0,\"data\":{\"transactionalId\":\"transactional-id\"}}," +
                                "\"value\":null}"
        ));
        testcases.add(new TransactionLogTestcase(
                        null,
                        MessageUtil.toVersionPrefixedByteBuffer((short) 1, txnLogValue).array(),
                        "{\"key\":null," +
                                "\"value\":{\"version\":1,\"data\":{\"producerId\":100,\"producerEpoch\":50," +
                                "\"transactionTimeoutMs\":500,\"transactionStatus\":4,\"transactionPartitions\":[]," +
                                "\"transactionLastUpdateTimestampMs\":1000,\"transactionStartTimestampMs\":750}}}"
        ));
        testcases.add(new TransactionLogTestcase(null, null, "{\"key\":null,\"value\":null}"));
        return testcases;
    }
    
    private static class TransactionLogTestcase {
        byte[] keyBuffer;
        byte[] valueBuffer;
        String expectedOutput;

        public TransactionLogTestcase(byte[] keyBuffer, byte[] valueBuffer, String expectedOutput) {
            this.keyBuffer = keyBuffer;
            this.valueBuffer = valueBuffer;
            this.expectedOutput = expectedOutput;
        }
    }
}
