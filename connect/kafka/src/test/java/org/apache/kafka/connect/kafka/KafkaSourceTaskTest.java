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
package org.apache.kafka.connect.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

import static org.apache.kafka.connect.kafka.KafkaSourceTask.OFFSET_KEY;
import static org.apache.kafka.connect.kafka.KafkaSourceTask.TOPIC_PARTITION_KEY;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        KafkaSourceTask.class,
        OffsetStorageReader.class,
        SourceTaskContext.class,
        KafkaConsumer.class
})
@PowerMockIgnore("javax.management")

public class KafkaSourceTaskTest {

    private KafkaSourceTask objectUnderTest;

    private Map<String, String> opts;
    private Properties props;
    private KafkaSourceConnectorConfig config;

    private static final String MAX_SHUTDOWN_WAIT_MS_VALUE = "2000";
    private static final int POLL_LOOP_TIMEOUT_MS_VALUE = 25;
    private static final String DESTINATION_TOPIC_PREFIX_VALUE = "test.destination";
    private static final String INCLUDE_MESSAGE_HEADERS_VALUE = "false";
    private static final String CONSUMER_AUTO_OFFSET_RESET_VALUE = "0";
    private static final String SOURCE_BOOTSTRAP_SERVERS_VALUE = "localhost:6000";
    private static final String TASK_LEADER_TOPIC_PARTITION_VALUE = "0:test.topic:1";
    private static final String AUTO_OFFSET_RESET_VALUE = "latest";
    private static final String SOURCE_TOPICS_WHITELIST_VALUE = "test*";
    private static final String CONSUMER_GROUP_ID_VALUE = "test-consumer-group";

    private static final String FIRST_TOPIC = "test.topic";
    private static final int FIRST_PARTITION = 1;
    private static final long FIRST_OFFSET = 123L;
    private static final String SECOND_TOPIC = "another.test.topic";
    private static final int SECOND_PARTITION = 0;
    private static final long SECOND_OFFSET = 456L;


    private OffsetStorageReader offsetStorageReader;
    private SourceTaskContext context;
    private KafkaConsumer consumer;

    @Before
    public void setup() {

        opts = new HashMap<>();
        opts.put(KafkaSourceConnectorConfig.SOURCE_TOPIC_WHITELIST_CONFIG, SOURCE_TOPICS_WHITELIST_VALUE);
        opts.put(KafkaSourceConnectorConfig.MAX_SHUTDOWN_WAIT_MS_CONFIG, MAX_SHUTDOWN_WAIT_MS_VALUE);
        opts.put(KafkaSourceConnectorConfig.POLL_LOOP_TIMEOUT_MS_CONFIG, String.valueOf(POLL_LOOP_TIMEOUT_MS_VALUE));
        opts.put(KafkaSourceConnectorConfig.DESTINATION_TOPIC_PREFIX_CONFIG, DESTINATION_TOPIC_PREFIX_VALUE);
        opts.put(KafkaSourceConnectorConfig.INCLUDE_MESSAGE_HEADERS_CONFIG, INCLUDE_MESSAGE_HEADERS_VALUE);
        opts.put(KafkaSourceConnectorConfig.CONSUMER_AUTO_OFFSET_RESET_CONFIG, CONSUMER_AUTO_OFFSET_RESET_VALUE);
        opts.put(KafkaSourceConnectorConfig.SOURCE_BOOTSTRAP_SERVERS_CONFIG, SOURCE_BOOTSTRAP_SERVERS_VALUE);
        opts.put(KafkaSourceConnectorConfig.TASK_LEADER_TOPIC_PARTITION_CONFIG, TASK_LEADER_TOPIC_PARTITION_VALUE);
        opts.put(KafkaSourceConnectorConfig.CONSUMER_AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_VALUE);
        opts.put(KafkaSourceConnectorConfig.CONSUMER_GROUP_ID_CONFIG, CONSUMER_GROUP_ID_VALUE);

        config = new KafkaSourceConnectorConfig(opts);
        props = new Properties();
        props.putAll(config.allWithPrefix(KafkaSourceConnectorConfig.CONSUMER_PREFIX));

        objectUnderTest = new KafkaSourceTask();

        offsetStorageReader = createMock(OffsetStorageReader.class);
        context = createMock(SourceTaskContext.class);
        consumer = createMock(KafkaConsumer.class);
        objectUnderTest.initialize(context);
    }

    @After
    public void teardown() {
        objectUnderTest = null;
    }

    private ConsumerRecords createTestRecordsWithHeaders() {
        RecordHeader header = new RecordHeader("testHeader", new byte[0]);
        RecordHeaders headers = new RecordHeaders();
        headers.add(header);
        TimestampType timestampType = TimestampType.NO_TIMESTAMP_TYPE;

        byte testByte = 0;
        byte[] testKey = {testByte};
        byte[] testValue = {testByte};

        ConnectHeaders destinationHeaders = new ConnectHeaders();
        destinationHeaders.add(header.key(), header.value(), Schema.OPTIONAL_BYTES_SCHEMA);
        ConsumerRecord<byte[], byte[]> testConsumerRecord = new ConsumerRecord<byte[], byte[]>(
                FIRST_TOPIC,
                FIRST_PARTITION,
                FIRST_OFFSET,
                System.currentTimeMillis(),
                timestampType,
                0L,
                0,
                0,
                testKey,
                testValue,
                headers
        );

        TopicPartition topicPartition = new TopicPartition(FIRST_TOPIC, FIRST_PARTITION);
        List<ConsumerRecord<byte[], byte[]>> consumerRecords = new ArrayList<>();
        consumerRecords.add(testConsumerRecord);

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> consumerRecordMap = new HashMap<>(1);
        consumerRecordMap.put(topicPartition, consumerRecords);
        ConsumerRecords testRecords = new ConsumerRecords<>(consumerRecordMap);
        return testRecords;
    }

    private ConsumerRecords createTestRecords() {
        byte testByte = 0;
        byte[] testKey = {testByte};
        byte[] testValue = {testByte};
        ConsumerRecord<byte[], byte[]> testConsumerRecord = new ConsumerRecord<byte[], byte[]>(FIRST_TOPIC, FIRST_PARTITION, FIRST_OFFSET, testKey, testValue);
        TopicPartition topicPartition = new TopicPartition(FIRST_TOPIC, FIRST_PARTITION);
        List<ConsumerRecord<byte[], byte[]>> consumerRecords = new ArrayList<>();
        consumerRecords.add(testConsumerRecord);

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> consumerRecordMap = new HashMap<>(1);
        consumerRecordMap.put(topicPartition, consumerRecords);
        ConsumerRecords testRecords = new ConsumerRecords<>(consumerRecordMap);
        return testRecords;
    }

    private void mockConsumerInitialization() throws Exception {
        TopicPartition firstTopicPartition = new TopicPartition(FIRST_TOPIC, FIRST_PARTITION);
        Collection<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(firstTopicPartition);
        Map<TopicPartition, Long> endOffsets = Collections.singletonMap(firstTopicPartition, FIRST_OFFSET);

        EasyMock.expect(context.offsetStorageReader()).andReturn(offsetStorageReader);
        EasyMock.expect(offsetStorageReader.offsets(EasyMock.<List<Map<String, String>>>anyObject())).andReturn(new HashMap<>());
        PowerMock.expectNew(KafkaConsumer.class, new Class[]{Properties.class}, config.getKafkaConsumerProperties()).andReturn(consumer);
        EasyMock.expect(consumer.endOffsets(topicPartitions)).andReturn(endOffsets);
        consumer.assign(topicPartitions);
        EasyMock.expectLastCall();
        consumer.seek(firstTopicPartition, FIRST_OFFSET);
        EasyMock.expectLastCall();
    }

    @Test
    public void testStartNoStoredPartitionsStartEnd() throws Exception {
        TopicPartition firstTopicPartition = new TopicPartition(FIRST_TOPIC, FIRST_PARTITION);
        Collection<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(firstTopicPartition);
        Map<TopicPartition, Long> endOffsets = Collections.singletonMap(firstTopicPartition, FIRST_OFFSET);

        EasyMock.expect(context.offsetStorageReader()).andReturn(offsetStorageReader);
        EasyMock.expect(offsetStorageReader.offsets(EasyMock.<List<Map<String, String>>>anyObject())).andReturn(new HashMap<>());
        PowerMock.expectNew(KafkaConsumer.class, new Class[]{Properties.class}, config.getKafkaConsumerProperties()).andReturn(consumer);
        EasyMock.expect(consumer.endOffsets(topicPartitions)).andReturn(endOffsets);
        consumer.assign(topicPartitions);
        EasyMock.expectLastCall();
        consumer.seek(firstTopicPartition, FIRST_OFFSET);
        EasyMock.expectLastCall();
        replayAll();

        objectUnderTest.start(opts);

        verifyAll();
    }

    @Test
    public void testStartNoStoredPartitionsStartBeginning() throws Exception {
        opts.put(KafkaSourceConnectorConfig.CONSUMER_AUTO_OFFSET_RESET_CONFIG, "earliest");
        config = new KafkaSourceConnectorConfig(opts);
        props = new Properties();
        props.putAll(config.allWithPrefix(KafkaSourceConnectorConfig.CONSUMER_PREFIX));

        TopicPartition firstTopicPartition = new TopicPartition(FIRST_TOPIC, FIRST_PARTITION);
        Collection<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(firstTopicPartition);
        Map<TopicPartition, Long> endOffsets = Collections.singletonMap(firstTopicPartition, FIRST_OFFSET);

        EasyMock.expect(context.offsetStorageReader()).andReturn(offsetStorageReader);
        EasyMock.expect(offsetStorageReader.offsets(EasyMock.<List<Map<String, String>>>anyObject())).andReturn(new HashMap<>());
        PowerMock.expectNew(KafkaConsumer.class, new Class[]{Properties.class}, config.getKafkaConsumerProperties()).andReturn(consumer);
        EasyMock.expect(consumer.beginningOffsets(topicPartitions)).andReturn(endOffsets);
        consumer.assign(topicPartitions);
        EasyMock.expectLastCall();
        consumer.seek(firstTopicPartition, FIRST_OFFSET);
        EasyMock.expectLastCall();
        replayAll();

        objectUnderTest.start(opts);

        verifyAll();
    }

    @Test
    public void testStartAllStoredPartitions() throws Exception {
        TopicPartition firstTopicPartition = new TopicPartition(FIRST_TOPIC, FIRST_PARTITION);
        Collection<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(firstTopicPartition);
        Map<Map<String, String>, Map<String, Object>> storedOffsets = Collections.singletonMap(
                Collections.singletonMap(TOPIC_PARTITION_KEY, String.format("%s:%d", FIRST_TOPIC, FIRST_PARTITION)),
                Collections.singletonMap(OFFSET_KEY, FIRST_OFFSET)
        );

        EasyMock.expect(context.offsetStorageReader()).andReturn(offsetStorageReader);
        EasyMock.expect(offsetStorageReader.offsets(EasyMock.<List<Map<String, String>>>anyObject())).andReturn(storedOffsets);
        PowerMock.expectNew(KafkaConsumer.class, new Class[]{Properties.class}, config.getKafkaConsumerProperties()).andReturn(consumer);
        consumer.assign(topicPartitions);
        EasyMock.expectLastCall();
        consumer.seek(firstTopicPartition, FIRST_OFFSET);
        EasyMock.expectLastCall();
        replayAll();

        objectUnderTest.start(opts);

        verifyAll();
    }

    @Test
    public void testStartSomeStoredPartitions() throws Exception {
        opts.put(KafkaSourceConnectorConfig.TASK_LEADER_TOPIC_PARTITION_CONFIG, TASK_LEADER_TOPIC_PARTITION_VALUE + "," + "0:" + SECOND_TOPIC + ":" + SECOND_PARTITION);
        config = new KafkaSourceConnectorConfig(opts);
        props = new Properties();
        props.putAll(config.allWithPrefix(KafkaSourceConnectorConfig.CONSUMER_PREFIX));

        TopicPartition firstTopicPartition = new TopicPartition(FIRST_TOPIC, FIRST_PARTITION);
        TopicPartition secondTopicPartition = new TopicPartition(SECOND_TOPIC, SECOND_PARTITION);
        Collection<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(firstTopicPartition);
        topicPartitions.add(secondTopicPartition);
        Map<TopicPartition, Long> endOffsets = Collections.singletonMap(firstTopicPartition, FIRST_OFFSET);
        Map<Map<String, String>, Map<String, Object>> storedOffsets = Collections.singletonMap(
                Collections.singletonMap(TOPIC_PARTITION_KEY, String.format("%s:%d", SECOND_TOPIC, SECOND_PARTITION)),
                Collections.singletonMap(OFFSET_KEY, SECOND_OFFSET)
        );

        EasyMock.expect(context.offsetStorageReader()).andReturn(offsetStorageReader);
        EasyMock.expect(offsetStorageReader.offsets(EasyMock.<List<Map<String, String>>>anyObject())).andReturn(storedOffsets);
        PowerMock.expectNew(KafkaConsumer.class, new Class[]{Properties.class}, config.getKafkaConsumerProperties()).andReturn(consumer);
        EasyMock.expect(consumer.endOffsets(Collections.singletonList(firstTopicPartition))).andReturn(endOffsets);
        consumer.assign(topicPartitions);
        EasyMock.expectLastCall();
        consumer.seek(firstTopicPartition, FIRST_OFFSET);
        EasyMock.expectLastCall();
        consumer.seek(secondTopicPartition, SECOND_OFFSET);
        EasyMock.expectLastCall();

        replayAll();

        objectUnderTest.start(opts);

        verifyAll();
    }


    @Test
    public void testPollNoRecords() throws Exception {
        mockConsumerInitialization();
        EasyMock.expect(consumer.poll(Duration.ofMillis(POLL_LOOP_TIMEOUT_MS_VALUE))).andReturn(new ConsumerRecords<>(Collections.EMPTY_MAP));
        replayAll();

        objectUnderTest.start(opts);
        List<SourceRecord> records = objectUnderTest.poll();

        assertEquals(0, records.size());

        verifyAll();
    }


    @Test
    public void testPollRecordReturnedNoIncludeHeaders() throws Exception {
        mockConsumerInitialization();
        EasyMock.expect(consumer.poll(Duration.ofMillis(POLL_LOOP_TIMEOUT_MS_VALUE))).andReturn(createTestRecords());
        replayAll();

        objectUnderTest.start(opts);
        List<SourceRecord> records = objectUnderTest.poll();

        SourceRecord testRecord = records.get(0);
        assertEquals(String.format("%s:%d", FIRST_TOPIC, FIRST_PARTITION), testRecord.sourcePartition().get(TOPIC_PARTITION_KEY));
        assertEquals(0L, testRecord.sourceOffset().get(OFFSET_KEY));
        assertEquals(0, testRecord.headers().size());

        verifyAll();
    }

    @Test
    public void testPollRecordReturnedIncludeHeaders() throws Exception {
        opts.put(KafkaSourceConnectorConfig.INCLUDE_MESSAGE_HEADERS_CONFIG, "true");
        config = new KafkaSourceConnectorConfig(opts);
        props = new Properties();
        props.putAll(config.allWithPrefix(KafkaSourceConnectorConfig.CONSUMER_PREFIX));

        objectUnderTest = new KafkaSourceTask();
        offsetStorageReader = createMock(OffsetStorageReader.class);
        context = createMock(SourceTaskContext.class);
        consumer = createMock(KafkaConsumer.class);
        objectUnderTest.initialize(context);

        TopicPartition firstTopicPartition = new TopicPartition(FIRST_TOPIC, FIRST_PARTITION);
        Collection<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(firstTopicPartition);
        Map<TopicPartition, Long> endOffsets = Collections.singletonMap(firstTopicPartition, FIRST_OFFSET);

        EasyMock.expect(context.offsetStorageReader()).andReturn(offsetStorageReader);
        EasyMock.expect(offsetStorageReader.offsets(EasyMock.<List<Map<String, String>>>anyObject())).andReturn(new HashMap<>());
        PowerMock.expectNew(KafkaConsumer.class, new Class[]{Properties.class}, config.getKafkaConsumerProperties()).andReturn(consumer);
        EasyMock.expect(consumer.endOffsets(topicPartitions)).andReturn(endOffsets);
        consumer.assign(topicPartitions);
        EasyMock.expectLastCall();
        consumer.seek(firstTopicPartition, FIRST_OFFSET);
        EasyMock.expectLastCall();


        // expectation for poll
        EasyMock.expect(consumer.poll(Duration.ofMillis(POLL_LOOP_TIMEOUT_MS_VALUE))).andReturn(createTestRecordsWithHeaders());
        replayAll();

        objectUnderTest.start(opts);
        List<SourceRecord> records = objectUnderTest.poll();

        SourceRecord testRecord = records.get(0);
        assertEquals(String.format("%s:%d", FIRST_TOPIC, FIRST_PARTITION), testRecord.sourcePartition().get(TOPIC_PARTITION_KEY));
        assertEquals(FIRST_OFFSET, testRecord.sourceOffset().get(OFFSET_KEY));
        assertEquals(1, testRecord.headers().size());

        verifyAll();
    }


    @Test
    public void testStopClosesConsumer() throws Exception {
        mockConsumerInitialization();

        consumer.wakeup();
        EasyMock.expectLastCall();
        consumer.close(EasyMock.anyObject());
        EasyMock.expectLastCall();

        replayAll();

        objectUnderTest.start(opts);
        objectUnderTest.stop();

        verifyAll();
    }
}
