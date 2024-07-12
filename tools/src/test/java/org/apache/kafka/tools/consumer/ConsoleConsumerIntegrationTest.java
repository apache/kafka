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
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@ExtendWith(value = ClusterTestExtensions.class)
@Tag("integration")
public class ConsoleConsumerIntegrationTest {

    private final String defaultBrokerId = "0";
    private final ClusterInstance cluster;

    public ConsoleConsumerIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT}, brokers = 1)
    public void testTransactionLogMessageFormatter() throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            // send transaction by producer
            String topic = "test";
            NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            admin.createTopics(Collections.singleton(newTopic));
            produceMessages(cluster, topic);

            // read the data from transaction topic by ConsoleConsumer with new formatter
            String[] transactionLogMessageFormatter = new String[]{
                "--bootstrap-server", cluster.bootstrapServers(),
                "--topic", "test",
                "--partition", "0",
                "--formatter", "org.apache.kafka.tools.consumer.TransactionLogMessageFormatter",
            };

            ConsoleConsumerOptions options = new ConsoleConsumerOptions(transactionLogMessageFormatter);
            Consumer<byte[], byte[]> consumer = createConsumer(cluster);
            consumer.subscribe(Collections.singletonList(topic));
            ConsoleConsumer.ConsumerWrapper consoleConsumer = new ConsoleConsumer.ConsumerWrapper(options, consumer);
            consoleConsumer.recordIter.forEachRemaining(record -> {
                String value = new String(record.value());
                System.out.println(value);
            });
            consoleConsumer.cleanup();
        }
    }

    private void produceMessages(ClusterInstance cluster, String topic) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, new byte[100 * 1000]);
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(0);
        ConsumerGroupMetadata groupMetadata = new ConsumerGroupMetadata("group-id");
        try (Producer<byte[], byte[]> producer = createProducer(cluster)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.sendOffsetsToTransaction(Collections.singletonMap(topicPartition, offsetAndMetadata), groupMetadata);
            producer.commitTransaction();
        }
    }

    private Producer<byte[], byte[]> createProducer(ClusterInstance cluster) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ACKS_CONFIG, "all");
        props.put(TRANSACTIONAL_ID_CONFIG, "transactional-id");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }
    
    private Consumer<byte[], byte[]> createConsumer(ClusterInstance cluster) {
        HashMap<String, Object> configs = new HashMap<>();
        configs.put(BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        configs.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        configs.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        return new KafkaConsumer<>(configs);
    }
}
