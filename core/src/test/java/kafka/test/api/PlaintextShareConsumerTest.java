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

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.collection.mutable.ArrayBuffer;
import scala.jdk.javaapi.CollectionConverters;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.Iterator;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}
