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

import kafka.api.AbstractShareConsumerTest;
import kafka.api.BaseConsumerTest;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.collection.mutable.ArrayBuffer;
import scala.jdk.javaapi.CollectionConverters;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(600)
public class PlaintextShareConsumerTest extends AbstractShareConsumerTest {
    public static final String TEST_WITH_PARAMETERIZED_QUORUM_NAME = "{displayName}.quorum={argumentsWithNames}";

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testPollNoSubscribeFails(String quorum) {
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        assertEquals(Collections.emptySet(), shareConsumer.subscription());
        // "Consumer is not subscribed to any topics."
        assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(2000)));
        shareConsumer.close();
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testSubscribeAndPollNoRecords(String quorum) {
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        Set<String> subscription = Collections.singleton(tp().topic());
        shareConsumer.subscribe(subscription);
        assertEquals(subscription, shareConsumer.subscription());
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2000));
        shareConsumer.close();
        assertEquals(0, records.count());
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testSubscribePollUnsubscribe(String quorum) {
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        Set<String> subscription = Collections.singleton(tp().topic());
        shareConsumer.subscribe(subscription);
        assertEquals(subscription, shareConsumer.subscription());
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2000));
        shareConsumer.unsubscribe();
        assertEquals(Collections.emptySet(), shareConsumer.subscription());
        shareConsumer.close();
        assertEquals(0, records.count());
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testSubscribePollSubscribe(String quorum) {
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        Set<String> subscription = Collections.singleton(tp().topic());
        shareConsumer.subscribe(subscription);
        assertEquals(subscription, shareConsumer.subscription());
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2000));
        assertEquals(0, records.count());
        shareConsumer.subscribe(subscription);
        assertEquals(subscription, shareConsumer.subscription());
        records = shareConsumer.poll(Duration.ofMillis(2000));
        shareConsumer.close();
        assertEquals(0, records.count());
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testSubscribeUnsubscribePollFails(String quorum) {
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        Set<String> subscription = Collections.singleton(tp().topic());
        shareConsumer.subscribe(subscription);
        assertEquals(subscription, shareConsumer.subscription());
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2000));
        shareConsumer.unsubscribe();
        assertEquals(Collections.emptySet(), shareConsumer.subscription());
        // "Consumer is not subscribed to any topics."
        assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(2000)));
        shareConsumer.close();
        assertEquals(0, records.count());
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testSubscribeSubscribeEmptyPollFails(String quorum) {
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        Set<String> subscription = Collections.singleton(tp().topic());
        shareConsumer.subscribe(subscription);
        assertEquals(subscription, shareConsumer.subscription());
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2000));
        shareConsumer.subscribe(Collections.emptySet());
        assertEquals(Collections.emptySet(), shareConsumer.subscription());
        // "Consumer is not subscribed to any topics."
        assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(2000)));
        shareConsumer.close();
        assertEquals(0, records.count());
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testSubscriptionAndPoll(String quorum) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        shareConsumer.close();
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testSubscriptionAndPollMultiple(String quorum) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        producer.send(record);
        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        producer.send(record);
        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        shareConsumer.close();
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testHeaders(String quorum) {
        int numRecords = 1;
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());

        record.headers().add("headerKey", "headerValue".getBytes());

        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        producer.send(record);

        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));

        ArrayBuffer<ConsumerRecord<byte[], byte[]>> records = consumeRecords(shareConsumer, numRecords, Integer.MAX_VALUE);

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

        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), deserializer,
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));

        ArrayBuffer<ConsumerRecord<byte[], byte[]>> records = consumeRecords(shareConsumer, numRecords, Integer.MAX_VALUE);

        assertEquals(numRecords, records.size());
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testHeadersSerializerDeserializer(String quorum) {
        testHeadersSerializeDeserialize(new BaseConsumerTest.SerializerImpl(), new BaseConsumerTest.DeserializerImpl());
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testMaxPollRecords(String quorum) {
        int maxPollRecords = 2;
        int numRecords = 10000;

        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        long startingTimestamp = System.currentTimeMillis();
        sendRecords(producer, numRecords, tp(), startingTimestamp);

        consumerConfig().setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));
        consumeAndVerifyRecords(shareConsumer, numRecords, 0, 0, startingTimestamp,
                TimestampType.CREATE_TIME, tp(), maxPollRecords);
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testControlRecordsSkipped(String quorum) throws Exception {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());

        Properties transactionProducerProps = new Properties();
        transactionProducerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "T1");
        KafkaProducer<byte[], byte[]> transactionalProducer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), transactionProducerProps);
        transactionalProducer.initTransactions();
        transactionalProducer.beginTransaction();
        RecordMetadata transactional1 = transactionalProducer.send(record).get();

        KafkaProducer<byte[], byte[]> nonTransactionalProducer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        RecordMetadata nonTransactional1 = nonTransactionalProducer.send(record).get();

        transactionalProducer.commitTransaction();

        transactionalProducer.beginTransaction();
        RecordMetadata transactional2 = transactionalProducer.send(record).get();
        transactionalProducer.abortTransaction();

        RecordMetadata nonTransactional2 = nonTransactionalProducer.send(record).get();

        transactionalProducer.close();
        nonTransactionalProducer.close();

        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(4, records.count());
        assertEquals(transactional1.offset(), records.records(tp()).get(0).offset());
        assertEquals(nonTransactional1.offset(), records.records(tp()).get(1).offset());
        assertEquals(transactional2.offset(), records.records(tp()).get(2).offset());
        assertEquals(nonTransactional2.offset(), records.records(tp()).get(3).offset());

        // There will be control records on the topic-partition, so the offsets of the non-control records
        // are not 0, 1, 2, 3. Just assert that the offset of the final one is not 3.
        assertNotEquals(3, nonTransactional2.offset());

        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(0, records.count());
        shareConsumer.close();
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testExplicitAcknowledgeSuccess(String quorum) throws Exception {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord));
        producer.send(record);
        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        shareConsumer.close();
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testExplicitAcknowledgeReleasePollAccept(String quorum) throws Exception {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.RELEASE));
        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(0, records.count());
        shareConsumer.close();
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testExplicitAcknowledgeReleaseAccept(String quorum) throws Exception {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.RELEASE));
        records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(0, records.count());
        shareConsumer.close();
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testExplicitAcknowledgeReleaseClose(String quorum) throws Exception {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.RELEASE));
        shareConsumer.close();
    }


    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testExplicitAcknowledgeThrowsNotInBatch(String quorum) throws Exception {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        ConsumerRecord<byte[], byte[]> consumedRecord = records.records(tp()).get(0);
        shareConsumer.acknowledge(consumedRecord);
        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(0, records.count());
        assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge(consumedRecord));
        shareConsumer.close();
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testImplicitAcknowledgeFailsExplicit(String quorum) throws Exception {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        ConsumerRecord<byte[], byte[]> consumedRecord = records.records(tp()).get(0);
        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(0, records.count());
        assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge(consumedRecord));
        shareConsumer.close();
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testFetchRecordLargerThanMaxPartitionFetchBytes(String quorum) throws Exception {
        int maxPartitionFetchBytes = 10000;
        ProducerRecord<byte[], byte[]> smallRecord = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());
        ProducerRecord<byte[], byte[]> bigRecord = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), new byte[maxPartitionFetchBytes]);
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        producer.send(smallRecord).get();
        RecordMetadata rm = producer.send(bigRecord).get();

        consumerConfig().setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(maxPartitionFetchBytes));
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                new Properties(), CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        shareConsumer.close();
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testMultipleConsumersWithDifferentGroupIds(String quorum) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        Properties props1 = new Properties();
        props1.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        KafkaShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                props1, CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer1.subscribe(Collections.singleton(tp().topic()));

        Properties props2 = new Properties();
        props2.put(ConsumerConfig.GROUP_ID_CONFIG, "group2");
        KafkaShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                props2, CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer2.subscribe(Collections.singleton(tp().topic()));

        // producing 3 records to the topic
        producer.send(record);
        producer.send(record);
        producer.send(record);
        ConsumerRecords<byte[], byte[]> records1 = shareConsumer1.poll(Duration.ofMillis(5000));
        ConsumerRecords<byte[], byte[]> records2 = shareConsumer2.poll(Duration.ofMillis(5000));
        // Both the consumers should read all the messages, because they are part of different share groups (both have different group IDs)
        assertEquals(3, records1.count());
        assertEquals(3, records2.count());

        producer.send(record);
        producer.send(record);
        records1 = shareConsumer1.poll(Duration.ofMillis(5000));
        assertEquals(2, records1.count());

        producer.send(record);
        producer.send(record);
        producer.send(record);
        records1 = shareConsumer1.poll(Duration.ofMillis(5000));
        records2 = shareConsumer2.poll(Duration.ofMillis(5000));
        assertEquals(3, records1.count());
        assertEquals(5, records2.count());

        shareConsumer1.close();
        shareConsumer2.close();
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testMultipleConsumersInGroupSequentialConsumption(String quorum) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        KafkaShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                props, CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer1.subscribe(Collections.singleton(tp().topic()));
        KafkaShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                props, CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer2.subscribe(Collections.singleton(tp().topic()));

        int totalMessages = 1000;
        for (int i = 0; i < totalMessages; i++) {
            producer.send(record);
        }

        int consumer1MessageCount = 0;
        int consumer2MessageCount = 0;

        while (true) {
            ConsumerRecords<byte[], byte[]> records1 = shareConsumer1.poll(Duration.ofMillis(5000));
            consumer1MessageCount += records1.count();
            ConsumerRecords<byte[], byte[]> records2 = shareConsumer2.poll(Duration.ofMillis(5000));
            consumer2MessageCount += records2.count();
            if (records1.count() + records2.count() == 0) break;
        }

        assertEquals(totalMessages, consumer1MessageCount + consumer2MessageCount);
        shareConsumer1.close();
        shareConsumer2.close();
    }

    private void produceMessages(int messageCount) {
        try (KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties())) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());
            for (int i = 0; i < messageCount; i++) {
                producer.send(record);
            }
        }
    }

    private CompletableFuture<Integer> consumeMessages(AtomicInteger totalMessagesConsumed, int totalMessages, String groupId, int consumerNumber, int maxRetries) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                props, CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer.subscribe(Collections.singleton(tp().topic()));
        int messagesConsumed = 0;
        int retries = 0;
        try {
            while (totalMessagesConsumed.get() < totalMessages && retries < maxRetries) {
                ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
                messagesConsumed += records.count();
                totalMessagesConsumed.addAndGet(records.count()); // Add count of consumed messages
                retries++;
            }
        } catch (Exception e) {
            System.out.println("Consumer : " + consumerNumber + " failed ! with exception : " + e.getMessage());
        } finally {
            System.out.println("Message consumed, share group group : " + groupId + ", consumer : " + consumerNumber + ", total messages consumed : " + messagesConsumed);
            shareConsumer.close();
            future.complete(messagesConsumed);
        }
        return future;
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testMultipleConsumersInGroupConcurrentConsumption(String quorum) {
        AtomicInteger totalMessagesConsumed = new AtomicInteger(0);

        int consumerCount = 5;
        int producerCount = 5;
        int messagesPerProducer = 10000;

        ExecutorService consumerExecutorService = Executors.newFixedThreadPool(consumerCount);
        ExecutorService producerExecutorService = Executors.newFixedThreadPool(producerCount);

        for (int i = 0; i < producerCount; i++) {
            Runnable task = () -> {
                produceMessages(messagesPerProducer);
            };
            producerExecutorService.submit(task);
        }

        ConcurrentLinkedQueue<CompletableFuture<Integer>> futures = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < consumerCount; i++) {
            final int consumerNumber = i + 1;
            consumerExecutorService.submit(() -> {
                CompletableFuture<Integer> future = consumeMessages(totalMessagesConsumed, producerCount * messagesPerProducer, "group1", consumerNumber, 100);
                futures.add(future);
            });
        }
        producerExecutorService.shutdown();
        consumerExecutorService.shutdown();
        try {
            producerExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); // Wait for all producer threads to complete
            consumerExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); // Wait for all consumer threads to complete
            int totalResult = 0;
            for (CompletableFuture<Integer> future : futures) {
                totalResult += future.get();
            }
            assertEquals(totalMessagesConsumed.get(), producerCount * messagesPerProducer);
            assertEquals(totalResult, producerCount * messagesPerProducer);
        } catch (Exception e) {
            fail("Exception occurred : " + e.getMessage());
        }
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    public void testMultipleConsumersInMultipleGroupsConcurrentConsumption(String quorum) {
        AtomicInteger totalMessagesConsumedGroup1 = new AtomicInteger(0);
        AtomicInteger totalMessagesConsumedGroup2 = new AtomicInteger(0);
        AtomicInteger totalMessagesConsumedGroup3 = new AtomicInteger(0);

        int consumerCount = 5;
        int producerCount = 5;
        int messagesPerProducer = 10000;

        ExecutorService shareGroupExecutorService1 = Executors.newFixedThreadPool(consumerCount);
        ExecutorService shareGroupExecutorService2 = Executors.newFixedThreadPool(consumerCount);
        ExecutorService shareGroupExecutorService3 = Executors.newFixedThreadPool(consumerCount);
        ExecutorService producerExecutorService = Executors.newFixedThreadPool(producerCount);

        for (int i = 0; i < producerCount; i++) {
            Runnable task = () -> {
                produceMessages(messagesPerProducer);
            };
            producerExecutorService.submit(task);
        }

        ConcurrentLinkedQueue<CompletableFuture<Integer>> futures1 = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CompletableFuture<Integer>> futures2 = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CompletableFuture<Integer>> futures3 = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < consumerCount; i++) {
            final int consumerNumber = i + 1;
            shareGroupExecutorService1.submit(() -> {
                CompletableFuture<Integer> future = consumeMessages(totalMessagesConsumedGroup1, producerCount * messagesPerProducer, "group1", consumerNumber, 100);
                futures1.add(future);
            });
            shareGroupExecutorService2.submit(() -> {
                CompletableFuture<Integer> future = consumeMessages(totalMessagesConsumedGroup2, producerCount * messagesPerProducer, "group2", consumerNumber, 100);
                futures2.add(future);
            });
            shareGroupExecutorService3.submit(() -> {
                CompletableFuture<Integer> future = consumeMessages(totalMessagesConsumedGroup3, producerCount * messagesPerProducer, "group3", consumerNumber, 100);
                futures3.add(future);
            });
        }
        producerExecutorService.shutdown();
        shareGroupExecutorService1.shutdown();
        shareGroupExecutorService2.shutdown();
        shareGroupExecutorService3.shutdown();
        try {
            producerExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); // Wait for all producer threads to complete
            shareGroupExecutorService1.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); // Wait for all consumer threads for group 1 to complete
            shareGroupExecutorService2.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); // Wait for all consumer threads for group 2 to complete
            shareGroupExecutorService3.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); // Wait for all consumer threads for group 3 to complete

            int totalResult1 = 0;
            for (CompletableFuture<Integer> future : futures1) {
                totalResult1 += future.get();
            }

            int totalResult2 = 0;
            for (CompletableFuture<Integer> future : futures2) {
                totalResult2 += future.get();
            }

            int totalResult3 = 0;
            for (CompletableFuture<Integer> future : futures3) {
                totalResult3 += future.get();
            }

            assertEquals(totalMessagesConsumedGroup1.get(), producerCount * messagesPerProducer);
            assertEquals(totalMessagesConsumedGroup2.get(), producerCount * messagesPerProducer);
            assertEquals(totalMessagesConsumedGroup3.get(), producerCount * messagesPerProducer);
            assertEquals(totalResult1, producerCount * messagesPerProducer);
            assertEquals(totalResult2, producerCount * messagesPerProducer);
            assertEquals(totalResult3, producerCount * messagesPerProducer);
        } catch (Exception e) {
            fail("Exception occurred : " + e.getMessage());
        }
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    @Disabled // Requires some changes to handle the consumer failures
    public void testConsumerFailureInGroupSequential(String quorum) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp().topic(), tp().partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), new Properties());
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        KafkaShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                props, CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer1.subscribe(Collections.singleton(tp().topic()));
        KafkaShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                props, CollectionConverters.asScala(Collections.<String>emptyList()).toList());
        shareConsumer2.subscribe(Collections.singleton(tp().topic()));

        int totalMessages = 1000;
        for (int i = 0; i < totalMessages; i++) {
            producer.send(record);
        }

        int consumer1MessageCount = 0;
        int consumer2MessageCount = 0;

        for (int i = 0; i < 2; i++) {
            ConsumerRecords<byte[], byte[]> records1 = shareConsumer1.poll(Duration.ofMillis(5000));
            consumer1MessageCount += records1.count();
        }

        shareConsumer1.close();

        int maxRetries = 100;
        int retries = 0;
        while (consumer1MessageCount + consumer2MessageCount < totalMessages && retries < maxRetries) {
            ConsumerRecords<byte[], byte[]> records2 = shareConsumer2.poll(Duration.ofMillis(5000));
            consumer2MessageCount += records2.count();
            retries++;
        }
        shareConsumer2.close();

        producer.close();
        assertEquals(totalMessages, consumer1MessageCount + consumer2MessageCount);
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"kraft+kip932"})
    @Disabled // Requires some changes to handle the consumer failures
    public void testMultipleConsumersInGroupFailureConcurrentConsumption(String quorum) {
        AtomicInteger totalMessagesConsumed = new AtomicInteger(0);

        int consumerCount = 5;
        int producerCount = 5;
        int messagesPerProducer = 10000;

        Random random = new Random();
        int totalConsumerFailures = random.nextInt(consumerCount - 1) + 1; // Generates a random number between 1 and consumerCount, this represents the random number of consumers that will be simulated to fail
            System.out.println("number of failing consumers : " + totalConsumerFailures);
        Set<Integer> failedConsumers = new HashSet<>();

        while (failedConsumers.size() < totalConsumerFailures) {
            int randomNumber = random.nextInt(consumerCount) + 1; // Generates a random number between 1 and 5
            failedConsumers.add(randomNumber);
        }

        System.out.println("Failing consumers are :-");
        for (Integer consumer : failedConsumers) {
            System.out.println(consumer);
        }

        ExecutorService consumerExecutorService = Executors.newFixedThreadPool(consumerCount);
        ExecutorService producerExecutorService = Executors.newFixedThreadPool(producerCount);

        for (int i = 0; i < producerCount; i++) {
            Runnable task = () -> {
                produceMessages(messagesPerProducer);
            };
            producerExecutorService.submit(task);
        }

        ConcurrentLinkedQueue<CompletableFuture<Integer>> futuresSuccess = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CompletableFuture<Integer>> futuresFail = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < consumerCount; i++) {
            final int consumerNumber = i + 1;
            if (failedConsumers.contains(consumerNumber)) {
                consumerExecutorService.submit(() -> {
                    CompletableFuture<Integer> future = consumeMessages(totalMessagesConsumed, producerCount * messagesPerProducer, "group1", consumerNumber, 1);
                    futuresFail.add(future);
                });
            } else {
                consumerExecutorService.submit(() -> {
                    CompletableFuture<Integer> future = consumeMessages(totalMessagesConsumed, producerCount * messagesPerProducer, "group1", consumerNumber, 100);
                    futuresSuccess.add(future);
                });
            }
        }
        producerExecutorService.shutdown();
        consumerExecutorService.shutdown();
        try {
            producerExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); // Wait for all producer threads to complete
            consumerExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); // Wait for all consumer threads to complete
            int totalSuccessResult = 0;
            for (CompletableFuture<Integer> future : futuresSuccess) {
                totalSuccessResult += future.get();
            }
            int totalFailResult = 0;
            for (CompletableFuture<Integer> future : futuresFail) {
                totalFailResult += future.get();
            }
            assertEquals(totalMessagesConsumed.get(), producerCount * messagesPerProducer);
            assertEquals(totalSuccessResult + totalFailResult, producerCount * messagesPerProducer);
        } catch (Exception e) {
            fail("Exception occurred : " + e.getMessage());
        }
    }
}
