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
import kafka.test.junit.ClusterTestExtensions;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKeyJsonConverter;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValueJsonConverter;
import org.apache.kafka.server.util.MockTime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(ClusterTestExtensions.class)
public class ConsoleConsumerTest {

    private final String topic = "test-topic";
    private final String transactionId = "transactional-id";
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void setup() {
        ConsoleConsumer.messageCount = 0;
    }

    @Test
    public void shouldThrowTimeoutExceptionWhenTimeoutIsReached() throws IOException {
        final Time time = new MockTime();
        final int timeoutMs = 1000;

        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> mockConsumer = mock(Consumer.class);

        when(mockConsumer.poll(Duration.ofMillis(timeoutMs))).thenAnswer(invocation -> {
            time.sleep(timeoutMs / 2 + 1);
            return ConsumerRecords.EMPTY;
        });

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--timeout-ms", String.valueOf(timeoutMs)
        };

        ConsoleConsumer.ConsumerWrapper consumer = new ConsoleConsumer.ConsumerWrapper(
            new ConsoleConsumerOptions(args),
            mockConsumer
        );

        assertThrows(TimeoutException.class, consumer::receive);
    }

    @Test
    public void shouldResetUnConsumedOffsetsBeforeExit() throws IOException {
        String topic = "test";
        int maxMessages = 123;
        int totalMessages = 700;
        long startOffset = 0L;

        MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        TopicPartition tp1 = new TopicPartition(topic, 0);
        TopicPartition tp2 = new TopicPartition(topic, 1);

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", topic,
            "--timeout-ms", "1000"
        };

        ConsoleConsumer.ConsumerWrapper consumer = new ConsoleConsumer.ConsumerWrapper(
            new ConsoleConsumerOptions(args),
            mockConsumer
        );

        mockConsumer.rebalance(Arrays.asList(tp1, tp2));
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(tp1, startOffset);
        offsets.put(tp2, startOffset);
        mockConsumer.updateBeginningOffsets(offsets);

        for (int i = 0; i < totalMessages; i++) {
            // add all records, each partition should have half of `totalMessages`
            mockConsumer.addRecord(new ConsumerRecord<>(topic, i % 2, i / 2, "key".getBytes(), "value".getBytes()));
        }

        MessageFormatter formatter = mock(MessageFormatter.class);

        ConsoleConsumer.process(maxMessages, formatter, consumer, System.out, false);
        assertEquals(totalMessages, mockConsumer.position(tp1) + mockConsumer.position(tp2));

        consumer.resetUnconsumedOffsets();
        assertEquals(maxMessages, mockConsumer.position(tp1) + mockConsumer.position(tp2));

        verify(formatter, times(maxMessages)).writeTo(any(), any());
        consumer.cleanup();
    }

    @Test
    public void shouldLimitReadsToMaxMessageLimit() {
        ConsoleConsumer.ConsumerWrapper consumer = mock(ConsoleConsumer.ConsumerWrapper.class);
        MessageFormatter formatter = mock(MessageFormatter.class);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("foo", 1, 1, new byte[0], new byte[0]);

        int messageLimit = 10;
        when(consumer.receive()).thenReturn(record);

        ConsoleConsumer.process(messageLimit, formatter, consumer, System.out, true);

        verify(consumer, times(messageLimit)).receive();
        verify(formatter, times(messageLimit)).writeTo(any(), any());

        consumer.cleanup();
    }

    @Test
    public void shouldStopWhenOutputCheckErrorFails() {
        ConsoleConsumer.ConsumerWrapper consumer = mock(ConsoleConsumer.ConsumerWrapper.class);
        MessageFormatter formatter = mock(MessageFormatter.class);
        PrintStream printStream = mock(PrintStream.class);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("foo", 1, 1, new byte[0], new byte[0]);

        when(consumer.receive()).thenReturn(record);
        //Simulate an error on System.out after the first record has been printed
        when(printStream.checkError()).thenReturn(true);

        ConsoleConsumer.process(-1, formatter, consumer, printStream, true);

        verify(formatter).writeTo(any(), eq(printStream));
        verify(consumer).receive();
        verify(printStream).checkError();

        consumer.cleanup();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSeekWhenOffsetIsSet() throws IOException {
        Consumer<byte[], byte[]> mockConsumer = mock(Consumer.class);
        TopicPartition tp0 = new TopicPartition("test", 0);

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", tp0.topic(),
            "--partition", String.valueOf(tp0.partition()),
            "--timeout-ms", "1000"
        };

        ConsoleConsumer.ConsumerWrapper consumer = new ConsoleConsumer.ConsumerWrapper(
            new ConsoleConsumerOptions(args),
            mockConsumer
        );

        verify(mockConsumer).assign(eq(Collections.singletonList(tp0)));
        verify(mockConsumer).seekToEnd(eq(Collections.singletonList(tp0)));
        consumer.cleanup();
        reset(mockConsumer);

        args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", tp0.topic(),
            "--partition", String.valueOf(tp0.partition()),
            "--offset", "123",
            "--timeout-ms", "1000"
        };

        consumer = new ConsoleConsumer.ConsumerWrapper(new ConsoleConsumerOptions(args), mockConsumer);

        verify(mockConsumer).assign(eq(Collections.singletonList(tp0)));
        verify(mockConsumer).seek(eq(tp0), eq(123L));
        consumer.cleanup();
        reset(mockConsumer);

        args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", tp0.topic(),
            "--partition", String.valueOf(tp0.partition()),
            "--offset", "earliest",
            "--timeout-ms", "1000"
        };

        consumer = new ConsoleConsumer.ConsumerWrapper(new ConsoleConsumerOptions(args), mockConsumer);

        verify(mockConsumer).assign(eq(Collections.singletonList(tp0)));
        verify(mockConsumer).seekToBeginning(eq(Collections.singletonList(tp0)));
        consumer.cleanup();
        reset(mockConsumer);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldWorkWithoutTopicOption() throws IOException {
        Consumer<byte[], byte[]> mockConsumer = mock(Consumer.class);

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--include", "includeTest*",
            "--from-beginning"
        };

        ConsoleConsumer.ConsumerWrapper consumer = new ConsoleConsumer.ConsumerWrapper(
            new ConsoleConsumerOptions(args),
            mockConsumer
        );

        verify(mockConsumer).subscribe(any(Pattern.class));
        consumer.cleanup();
    }

    @ClusterTest(brokers = 3)
    public void testTransactionLogMessageFormatter(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {

            NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            admin.createTopics(singleton(newTopic));
            produceMessages(cluster);

            String[] transactionLogMessageFormatter = new String[]{
                "--bootstrap-server", cluster.bootstrapServers(),
                "--topic", Topic.TRANSACTION_STATE_TOPIC_NAME,
                "--formatter", "org.apache.kafka.tools.consumer.TransactionLogMessageFormatter",
                "--from-beginning"
            };

            ConsoleConsumerOptions options = new ConsoleConsumerOptions(transactionLogMessageFormatter);
            ConsoleConsumer.ConsumerWrapper consumerWrapper = new ConsoleConsumer.ConsumerWrapper(options, createConsumer(cluster));
            
            try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                 PrintStream output = new PrintStream(out)) {
                ConsoleConsumer.process(1, options.formatter(), consumerWrapper, output, true);
                
                JsonNode jsonNode = objectMapper.reader().readTree(out.toByteArray());
                JsonNode keyNode = jsonNode.get("key");

                TransactionLogKey logKey =
                        TransactionLogKeyJsonConverter.read(keyNode.get("data"), TransactionLogKey.HIGHEST_SUPPORTED_VERSION);
                assertNotNull(logKey);
                assertEquals(transactionId, logKey.transactionalId());

                JsonNode valueNode = jsonNode.get("value");
                TransactionLogValue logValue =
                        TransactionLogValueJsonConverter.read(valueNode.get("data"), TransactionLogValue.HIGHEST_SUPPORTED_VERSION);
                assertNotNull(logValue);
                assertEquals(0, logValue.producerId());
                assertEquals(0, logValue.transactionStatus());
            } finally {
                consumerWrapper.cleanup();
            }
        }
    }

    private void produceMessages(ClusterInstance cluster) {
        try (Producer<byte[], byte[]> producer = createProducer(cluster)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, new byte[1_000 * 100]));
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
        props.put(GROUP_ID_CONFIG, "test-group");
        return new KafkaConsumer<>(props);
    }
}
