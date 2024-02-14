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
package kafka.test.api;

import kafka.api.AbstractConsumerTest;
import kafka.api.BaseConsumerTest;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Function0;
import scala.collection.mutable.ArrayBuffer;
import scala.jdk.javaapi.CollectionConverters;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;

@Timeout(600)
public class PlaintextShareConsumerTest extends AbstractConsumerTest {
    public static final String TEST_WITH_PARAMETERIZED_QUORUM_NAME = "{displayName}.quorum={argumentsWithNames}";

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testSubscriptionAndPoll(String quorum) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(10000));
        shareConsumer.close();
        //TODO: the expected value should be changed to 1 and more verification should be added once the fetch functionality is in place
        assertEquals(0, records.count());
    }

    @Disabled("TODO: The consumer needs to be converted to share consumers once the functionality for this test is available")
    @Test
    public void testHeaders() {
        int numRecords = 1;
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());

        record.headers().add("headerKey", "headerValue".getBytes());

        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        producer.send(record);

        Consumer<byte[], byte[]> consumer = createConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        assertEquals(0, consumer.assignment().size());
        consumer.assign(Collections.singleton(tp()));
        assertEquals(1, consumer.assignment().size());

        consumer.seek(tp(), 0);
        ArrayBuffer<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, numRecords, Integer.MAX_VALUE);

        assertEquals(numRecords, records.size());

        for (Iterator<ConsumerRecord<byte[], byte[]>> iter = CollectionConverters.asJava(records.toIterator()); iter.hasNext();) {
            ConsumerRecord<byte[], byte[]> consumerRecord = iter.next();
            Header header = consumerRecord.headers().lastHeader("headerKey");
            if (header != null)
                assertEquals("headerValue", new String(header.value()));
        }
    }

    private void testHeadersSerializeDeserialize(Serializer<byte[]> serializer, Deserializer<byte[]> deserializer) {
        int numRecords = 1;
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());

        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), serializer, new Properties());
        producer.send(record);

        Consumer<byte[], byte[]> consumer = createConsumer(new ByteArrayDeserializer(), deserializer,
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        assertEquals(0, consumer.assignment().size());
        consumer.assign(Collections.singleton(tp()));
        assertEquals(1, consumer.assignment().size());

        consumer.seek(tp(), 0);
        ArrayBuffer<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, numRecords, Integer.MAX_VALUE);

        assertEquals(numRecords, records.size());
    }

    @Disabled("TODO: The consumer needs to be converted to share consumers once the functionality for this test is available")
    @Test
    public void testHeadersSerializerDeserializer() {
        testHeadersSerializeDeserialize(new BaseConsumerTest.SerializerImpl(), new BaseConsumerTest.DeserializerImpl());
    }

    @Disabled("TODO: The consumer needs to be converted to share consumers once the functionality for this test is available")
    @Test
    public void testMaxPollRecords() {
        int maxPollRecords = 2;
        int numRecords = 10000;

        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        long startingTimestamp = System.currentTimeMillis();
        sendRecords(producer, numRecords, tp(), startingTimestamp);

        consumerConfig().setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
        Consumer<byte[], byte[]> consumer = createConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        consumer.assign(Collections.singleton(tp()));
        consumeAndVerifyRecords(consumer, numRecords, 0, 0, startingTimestamp,
                TimestampType.CREATE_TIME, tp(), maxPollRecords);

    }

    @Disabled("TODO: The consumer needs to be converted to share consumers once the functionality for this test is available")
    @Test
    public void testMaxPollIntervalMs() throws InterruptedException {
        consumerConfig().setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(1000));
        consumerConfig().setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(500));
        consumerConfig().setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(2000));

        Consumer<byte[], byte[]> consumer = createConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());

        TestConsumerReassignmentListener listener = new TestConsumerReassignmentListener();
        consumer.subscribe(Collections.singleton(topic()), listener);

        // rebalance to get the initial assignment
        awaitRebalance(consumer, listener);
        assertEquals(1, listener.callsToAssigned());
        assertEquals(0, listener.callsToRevoked());

        // after we extend longer than max.poll a rebalance should be triggered
        // NOTE we need to have a relatively much larger value than max.poll to let heartbeat expired for sure
        Thread.sleep(3000);

        awaitRebalance(consumer, listener);
        assertEquals(2, listener.callsToAssigned());
        assertEquals(1, listener.callsToRevoked());
    }

    @Disabled("TODO: The consumer needs to be converted to share consumers once the functionality for this test is available")
    @Test
    public void testMaxPollIntervalMsDelayInRevocation() {
        consumerConfig().setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(5000));
        consumerConfig().setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(500));
        consumerConfig().setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(1000));
        consumerConfig().setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

        Consumer<byte[], byte[]> consumer = createConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        final boolean[] commitCompleted = {false};
        long committedPosition = -1;

        TestConsumerReassignmentListener listener = new TestConsumerReassignmentListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                if (!partitions.isEmpty() && partitions.contains(tp())) {
                    // on the second rebalance (after we have joined the group initially), sleep longer
                    // than session timeout and then try a commit. We should still be in the group,
                    // so the commit should succeed
                    Utils.sleep(1500);
                    long committedPosition = consumer.position(tp());
                    Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                    map.put(tp(), new OffsetAndMetadata(committedPosition));
                    consumer.commitSync(map);
                    commitCompleted[0] = true;
                }
                super.onPartitionsRevoked(partitions);
            }
        };

        consumer.subscribe(Collections.singleton(topic()), listener);

        // rebalance to get the initial assignment
        awaitRebalance(consumer, listener);

        // force a rebalance to trigger an invocation of the revocation callback while in the group
        consumer.subscribe(Collections.singleton("otherTopic"), listener);
        awaitRebalance(consumer, listener);

        assertEquals(0, committedPosition);
        assertTrue(commitCompleted[0]);
    }

    @Disabled("TODO: The consumer needs to be converted to share consumers once the functionality for this test is available")
    @Test
    public void testMaxPollIntervalMsDelayInAssignment() {
        consumerConfig().setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(5000));
        consumerConfig().setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(500));
        consumerConfig().setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(1000));
        consumerConfig().setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

        Consumer<byte[], byte[]> consumer = createConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        TestConsumerReassignmentListener listener = new TestConsumerReassignmentListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                Utils.sleep(1500);
                super.onPartitionsAssigned(partitions);
            }
        };
        consumer.subscribe(Collections.singleton(topic()), listener);

        // rebalance to get the initial assignment
        awaitRebalance(consumer, listener);

        // We should still be in the group after this invocation
        ensureNoRebalance(consumer, listener);
    }

    @Disabled("TODO: The consumer needs to be converted to share consumers once the functionality for this test is available")
    @Test
    public void testAutoCommitOnClose() {
        consumerConfig().setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        Consumer<byte[], byte[]> consumer = createConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());

        int numRecords = 10000;
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        sendRecords(producer, numRecords, tp(), System.currentTimeMillis());

        consumer.subscribe(Collections.singleton(topic()));
        awaitAssignment(consumer, new HashSet<>(Arrays.asList(tp(), tp2())));

        // should auto-commit sought positions before closing
        consumer.seek(tp(), 300);
        consumer.seek(tp2(), 500);
        consumer.close();

        // now we should see the committed positions from another consumer
        Consumer<byte[], byte[]> anotherConsumer = createConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        assertEquals(300, anotherConsumer.committed(new HashSet<>(Collections.singletonList(tp()))).get(tp()).offset());
        assertEquals(500, anotherConsumer.committed(new HashSet<>(Collections.singletonList(tp2()))).get(tp2()).offset());
    }

    @Disabled("TODO: The consumer needs to be converted to share consumers once the functionality for this test is available")
    @Test
    public void testAutoCommitOnCloseAfterWakeup() {
        consumerConfig().setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        Consumer<byte[], byte[]> consumer = createConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());

        int numRecords = 10000;
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        sendRecords(producer, numRecords, tp(), System.currentTimeMillis());

        consumer.subscribe(Collections.singleton(topic()));
        awaitAssignment(consumer, new HashSet<>(Arrays.asList(tp(), tp2())));

        // should auto-commit sought positions before closing
        consumer.seek(tp(), 300);
        consumer.seek(tp2(), 500);

        // wakeup the consumer before closing to simulate trying to break a poll
        // loop from another thread
        consumer.wakeup();
        consumer.close();

        // now we should see the committed positions from another consumer
        Consumer<byte[], byte[]> anotherConsumer = createConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        assertEquals(300, anotherConsumer.committed(new HashSet<>(Collections.singletonList(tp()))).get(tp()).offset());
        assertEquals(500, anotherConsumer.committed(new HashSet<>(Collections.singletonList(tp2()))).get(tp2()).offset());
    }

    private void awaitAssignment(Consumer<byte[], byte[]> consumer, Set<TopicPartition> expectedAssignment) {
        Function0<Object> action = () -> consumer.assignment().equals(expectedAssignment);
        Function0<String> messageSupplier = () -> String.format("Timed out while awaiting expected assignment %s. " +
                "The current assignment is %s", expectedAssignment, consumer.assignment());
        TestUtils.pollUntilTrue(consumer, action, messageSupplier, DEFAULT_MAX_WAIT_MS);
    }
}
