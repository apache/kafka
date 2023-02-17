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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.integration.MonitorableSourceConnector;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.TopicCreationGroup;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.api.easymock.annotation.MockStrict;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TOPIC_CREATION_GROUPS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.EXCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.INCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_CREATION_ENABLE_CONFIG;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@PowerMockIgnore({"javax.management.*",
        "org.apache.log4j.*"})
@RunWith(PowerMockRunner.class)
public class AbstractWorkerSourceTaskTest {

    private static final String TOPIC = "topic";
    private static final String OTHER_TOPIC = "other-topic";
    private static final Map<String, byte[]> PARTITION = Collections.singletonMap("key", "partition".getBytes());
    private static final Map<String, Integer> OFFSET = Collections.singletonMap("key", 12);

    // Connect-format data
    private static final Schema KEY_SCHEMA = Schema.INT32_SCHEMA;
    private static final Integer KEY = -1;
    private static final Schema RECORD_SCHEMA = Schema.INT64_SCHEMA;
    private static final Long RECORD = 12L;
    // Serialized data. The actual format of this data doesn't matter -- we just want to see that the right version
    // is used in the right place.
    private static final byte[] SERIALIZED_KEY = "converted-key".getBytes();
    private static final byte[] SERIALIZED_RECORD = "converted-record".getBytes();

    @Mock private SourceTask sourceTask;
    @Mock private TopicAdmin admin;
    @Mock private KafkaProducer<byte[], byte[]> producer;
    @Mock private Converter keyConverter;
    @Mock private Converter valueConverter;
    @Mock private HeaderConverter headerConverter;
    @Mock private TransformationChain<SourceRecord> transformationChain;
    @Mock private CloseableOffsetStorageReader offsetReader;
    @Mock private OffsetStorageWriter offsetWriter;
    @Mock private ConnectorOffsetBackingStore offsetStore;
    @Mock private StatusBackingStore statusBackingStore;
    @Mock private WorkerSourceTaskContext sourceTaskContext;
    @MockStrict private TaskStatus.Listener statusListener;

    private final ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private final ConnectorTaskId taskId1 = new ConnectorTaskId("job", 1);

    private Plugins plugins;
    private WorkerConfig config;
    private SourceConnectorConfig sourceConfig;
    private MockConnectMetrics metrics = new MockConnectMetrics();
    @Mock private ErrorHandlingMetrics errorHandlingMetrics;
    private Capture<Callback> producerCallbacks;

    private AbstractWorkerSourceTask workerTask;

    @Before
    public void setup() {
        Map<String, String> workerProps = workerProps();
        plugins = new Plugins(workerProps);
        config = new StandaloneConfig(workerProps);
        sourceConfig = new SourceConnectorConfig(plugins, sourceConnectorPropsWithGroups(TOPIC), true);
        producerCallbacks = EasyMock.newCapture();
        metrics = new MockConnectMetrics();
    }

    private Map<String, String> workerProps() {
        Map<String, String> props = new HashMap<>();
        props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("offset.storage.file.filename", "/tmp/connect.offsets");
        props.put(TOPIC_CREATION_ENABLE_CONFIG, "true");
        return props;
    }

    private Map<String, String> sourceConnectorPropsWithGroups(String topic) {
        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put("name", "foo-connector");
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(1));
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(TOPIC_CREATION_GROUPS_CONFIG, String.join(",", "foo", "bar"));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(1));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(1));
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + "foo" + "." + INCLUDE_REGEX_CONFIG, topic);
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + "bar" + "." + INCLUDE_REGEX_CONFIG, ".*");
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + "bar" + "." + EXCLUDE_REGEX_CONFIG, topic);
        return props;
    }

    @After
    public void tearDown() {
        if (metrics != null) metrics.stop();
    }

    @Test
    public void testMetricsGroup() {
        AbstractWorkerSourceTask.SourceTaskMetricsGroup group = new AbstractWorkerSourceTask.SourceTaskMetricsGroup(taskId, metrics);
        AbstractWorkerSourceTask.SourceTaskMetricsGroup group1 = new AbstractWorkerSourceTask.SourceTaskMetricsGroup(taskId1, metrics);
        for (int i = 0; i != 10; ++i) {
            group.recordPoll(100, 1000 + i * 100);
            group.recordWrite(10);
        }
        for (int i = 0; i != 20; ++i) {
            group1.recordPoll(100, 1000 + i * 100);
            group1.recordWrite(10);
        }
        assertEquals(1900.0, metrics.currentMetricValueAsDouble(group.metricGroup(), "poll-batch-max-time-ms"), 0.001d);
        assertEquals(1450.0, metrics.currentMetricValueAsDouble(group.metricGroup(), "poll-batch-avg-time-ms"), 0.001d);
        assertEquals(33.333, metrics.currentMetricValueAsDouble(group.metricGroup(), "source-record-poll-rate"), 0.001d);
        assertEquals(1000, metrics.currentMetricValueAsDouble(group.metricGroup(), "source-record-poll-total"), 0.001d);
        assertEquals(3.3333, metrics.currentMetricValueAsDouble(group.metricGroup(), "source-record-write-rate"), 0.001d);
        assertEquals(100, metrics.currentMetricValueAsDouble(group.metricGroup(), "source-record-write-total"), 0.001d);
        assertEquals(900.0, metrics.currentMetricValueAsDouble(group.metricGroup(), "source-record-active-count"), 0.001d);

        // Close the group
        group.close();

        for (MetricName metricName : group.metricGroup().metrics().metrics().keySet()) {
            // Metrics for this group should no longer exist
            assertFalse(group.metricGroup().groupId().includes(metricName));
        }
        // Sensors for this group should no longer exist
        assertNull(group.metricGroup().metrics().getSensor("sink-record-read"));
        assertNull(group.metricGroup().metrics().getSensor("sink-record-send"));
        assertNull(group.metricGroup().metrics().getSensor("sink-record-active-count"));
        assertNull(group.metricGroup().metrics().getSensor("partition-count"));
        assertNull(group.metricGroup().metrics().getSensor("offset-seq-number"));
        assertNull(group.metricGroup().metrics().getSensor("offset-commit-completion"));
        assertNull(group.metricGroup().metrics().getSensor("offset-commit-completion-skip"));
        assertNull(group.metricGroup().metrics().getSensor("put-batch-time"));

        assertEquals(2900.0, metrics.currentMetricValueAsDouble(group1.metricGroup(), "poll-batch-max-time-ms"), 0.001d);
        assertEquals(1950.0, metrics.currentMetricValueAsDouble(group1.metricGroup(), "poll-batch-avg-time-ms"), 0.001d);
        assertEquals(66.667, metrics.currentMetricValueAsDouble(group1.metricGroup(), "source-record-poll-rate"), 0.001d);
        assertEquals(2000, metrics.currentMetricValueAsDouble(group1.metricGroup(), "source-record-poll-total"), 0.001d);
        assertEquals(6.667, metrics.currentMetricValueAsDouble(group1.metricGroup(), "source-record-write-rate"), 0.001d);
        assertEquals(200, metrics.currentMetricValueAsDouble(group1.metricGroup(), "source-record-write-total"), 0.001d);
        assertEquals(1800.0, metrics.currentMetricValueAsDouble(group1.metricGroup(), "source-record-active-count"), 0.001d);
    }

    @Test
    public void testSendRecordsConvertsData() {
        createWorkerTask();

        List<SourceRecord> records = new ArrayList<>();
        // Can just use the same record for key and value
        records.add(new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD));

        Capture<ProducerRecord<byte[], byte[]>> sent = expectSendRecordAnyTimes();

        expectTopicCreation(TOPIC);

        PowerMock.replayAll();

        workerTask.toSend = records;
        workerTask.sendRecords();
        assertArrayEquals(SERIALIZED_KEY, sent.getValue().key());
        assertArrayEquals(SERIALIZED_RECORD, sent.getValue().value());

        PowerMock.verifyAll();
    }

    @Test
    public void testSendRecordsPropagatesTimestamp() {
        final Long timestamp = System.currentTimeMillis();

        createWorkerTask();

        List<SourceRecord> records = Collections.singletonList(
                new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD, timestamp)
        );

        Capture<ProducerRecord<byte[], byte[]>> sent = expectSendRecordAnyTimes();

        expectTopicCreation(TOPIC);

        PowerMock.replayAll();

        workerTask.toSend = records;
        workerTask.sendRecords();
        assertEquals(timestamp, sent.getValue().timestamp());

        PowerMock.verifyAll();
    }

    @Test
    public void testSendRecordsCorruptTimestamp() {
        final Long timestamp = -3L;
        createWorkerTask();

        List<SourceRecord> records = Collections.singletonList(
                new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD, timestamp)
        );

        Capture<ProducerRecord<byte[], byte[]>> sent = expectSendRecordAnyTimes();

        PowerMock.replayAll();

        workerTask.toSend = records;
        assertThrows(InvalidRecordException.class, workerTask::sendRecords);
        assertFalse(sent.hasCaptured());

        PowerMock.verifyAll();
    }

    @Test
    public void testSendRecordsNoTimestamp() {
        final Long timestamp = -1L;
        createWorkerTask();

        List<SourceRecord> records = Collections.singletonList(
                new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD, timestamp)
        );

        Capture<ProducerRecord<byte[], byte[]>> sent = expectSendRecordAnyTimes();

        expectTopicCreation(TOPIC);

        PowerMock.replayAll();

        workerTask.toSend = records;
        workerTask.sendRecords();
        assertNull(sent.getValue().timestamp());

        PowerMock.verifyAll();
    }

    @Test
    public void testHeaders() {
        Headers headers = new RecordHeaders();
        headers.add("header_key", "header_value".getBytes());

        org.apache.kafka.connect.header.Headers connectHeaders = new ConnectHeaders();
        connectHeaders.add("header_key", new SchemaAndValue(Schema.STRING_SCHEMA, "header_value"));

        createWorkerTask();

        List<SourceRecord> records = new ArrayList<>();
        records.add(new SourceRecord(PARTITION, OFFSET, TOPIC, null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD, null, connectHeaders));

        expectTopicCreation(TOPIC);

        Capture<ProducerRecord<byte[], byte[]>> sent = expectSendRecord(TOPIC, true, headers);

        PowerMock.replayAll();

        workerTask.toSend = records;
        workerTask.sendRecords();
        assertArrayEquals(SERIALIZED_KEY, sent.getValue().key());
        assertArrayEquals(SERIALIZED_RECORD, sent.getValue().value());
        assertEquals(headers, sent.getValue().headers());

        PowerMock.verifyAll();
    }

    @Test
    public void testHeadersWithCustomConverter() throws Exception {
        StringConverter stringConverter = new StringConverter();
        SampleConverterWithHeaders testConverter = new SampleConverterWithHeaders();

        createWorkerTask(stringConverter, testConverter, stringConverter);

        List<SourceRecord> records = new ArrayList<>();

        String stringA = "Árvíztűrő tükörfúrógép";
        org.apache.kafka.connect.header.Headers headersA = new ConnectHeaders();
        String encodingA = "latin2";
        headersA.addString("encoding", encodingA);

        records.add(new SourceRecord(PARTITION, OFFSET, "topic", null, Schema.STRING_SCHEMA, "a", Schema.STRING_SCHEMA, stringA, null, headersA));

        String stringB = "Тестовое сообщение";
        org.apache.kafka.connect.header.Headers headersB = new ConnectHeaders();
        String encodingB = "koi8_r";
        headersB.addString("encoding", encodingB);

        records.add(new SourceRecord(PARTITION, OFFSET, "topic", null, Schema.STRING_SCHEMA, "b", Schema.STRING_SCHEMA, stringB, null, headersB));

        expectTopicCreation(TOPIC);

        Capture<ProducerRecord<byte[], byte[]>> sentRecordA = expectSendRecord(TOPIC, false, null);
        Capture<ProducerRecord<byte[], byte[]>> sentRecordB = expectSendRecord(TOPIC, false, null);

        PowerMock.replayAll();

        workerTask.toSend = records;
        workerTask.sendRecords();

        assertEquals(ByteBuffer.wrap("a".getBytes()), ByteBuffer.wrap(sentRecordA.getValue().key()));
        assertEquals(
                ByteBuffer.wrap(stringA.getBytes(encodingA)),
                ByteBuffer.wrap(sentRecordA.getValue().value())
        );
        assertEquals(encodingA, new String(sentRecordA.getValue().headers().lastHeader("encoding").value()));

        assertEquals(ByteBuffer.wrap("b".getBytes()), ByteBuffer.wrap(sentRecordB.getValue().key()));
        assertEquals(
                ByteBuffer.wrap(stringB.getBytes(encodingB)),
                ByteBuffer.wrap(sentRecordB.getValue().value())
        );
        assertEquals(encodingB, new String(sentRecordB.getValue().headers().lastHeader("encoding").value()));

        PowerMock.verifyAll();
    }

    @Test
    public void testTopicCreateWhenTopicExists() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls();
        TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, null, Collections.emptyList(), Collections.emptyList());
        TopicDescription topicDesc = new TopicDescription(TOPIC, false, Collections.singletonList(topicPartitionInfo));
        EasyMock.expect(admin.describeTopics(TOPIC)).andReturn(Collections.singletonMap(TOPIC, topicDesc));

        expectSendRecord();
        expectSendRecord();

        PowerMock.replayAll();

        workerTask.toSend = Arrays.asList(record1, record2);
        workerTask.sendRecords();
    }

    @Test
    public void testSendRecordsTopicDescribeRetries() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls();
        // First round - call to describe the topic times out
        EasyMock.expect(admin.describeTopics(TOPIC))
                .andThrow(new RetriableException(new TimeoutException("timeout")));

        // Second round - calls to describe and create succeed
        expectTopicCreation(TOPIC);
        // Exactly two records are sent
        expectSendRecord();
        expectSendRecord();

        PowerMock.replayAll();

        workerTask.toSend = Arrays.asList(record1, record2);
        workerTask.sendRecords();
        assertEquals(Arrays.asList(record1, record2), workerTask.toSend);

        // Next they all succeed
        workerTask.sendRecords();
        assertNull(workerTask.toSend);
    }

    @Test
    public void testSendRecordsTopicCreateRetries() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        // First call to describe the topic times out
        expectPreliminaryCalls();
        EasyMock.expect(admin.describeTopics(TOPIC)).andReturn(Collections.emptyMap());
        Capture<NewTopic> newTopicCapture = EasyMock.newCapture();
        EasyMock.expect(admin.createOrFindTopics(EasyMock.capture(newTopicCapture)))
                .andThrow(new RetriableException(new TimeoutException("timeout")));

        // Second round
        expectTopicCreation(TOPIC);
        expectSendRecord();
        expectSendRecord();

        PowerMock.replayAll();

        workerTask.toSend = Arrays.asList(record1, record2);
        workerTask.sendRecords();
        assertEquals(Arrays.asList(record1, record2), workerTask.toSend);

        // Next they all succeed
        workerTask.sendRecords();
        assertNull(workerTask.toSend);
    }

    @Test
    public void testSendRecordsTopicDescribeRetriesMidway() {
        createWorkerTask();

        // Differentiate only by Kafka partition so we can reuse conversion expectations
        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record3 = new SourceRecord(PARTITION, OFFSET, OTHER_TOPIC, 3, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        // First round
        expectPreliminaryCalls(OTHER_TOPIC);
        expectTopicCreation(TOPIC);
        expectSendRecord();
        expectSendRecord();

        // First call to describe the topic times out
        EasyMock.expect(admin.describeTopics(OTHER_TOPIC))
                .andThrow(new RetriableException(new TimeoutException("timeout")));

        // Second round
        expectTopicCreation(OTHER_TOPIC);
        expectSendRecord(OTHER_TOPIC, false, emptyHeaders());

        PowerMock.replayAll();

        // Try to send 3, make first pass, second fail. Should save last two
        workerTask.toSend = Arrays.asList(record1, record2, record3);
        workerTask.sendRecords();
        assertEquals(Arrays.asList(record3), workerTask.toSend);

        // Next they all succeed
        workerTask.sendRecords();
        assertNull(workerTask.toSend);

        PowerMock.verifyAll();
    }

    @Test
    public void testSendRecordsTopicCreateRetriesMidway() {
        createWorkerTask();

        // Differentiate only by Kafka partition so we can reuse conversion expectations
        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record3 = new SourceRecord(PARTITION, OFFSET, OTHER_TOPIC, 3, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        // First round
        expectPreliminaryCalls(OTHER_TOPIC);
        expectTopicCreation(TOPIC);
        expectSendRecord();
        expectSendRecord();

        EasyMock.expect(admin.describeTopics(OTHER_TOPIC)).andReturn(Collections.emptyMap());
        // First call to create the topic times out
        Capture<NewTopic> newTopicCapture = EasyMock.newCapture();
        EasyMock.expect(admin.createOrFindTopics(EasyMock.capture(newTopicCapture)))
                .andThrow(new RetriableException(new TimeoutException("timeout")));

        // Second round
        expectTopicCreation(OTHER_TOPIC);
        expectSendRecord(OTHER_TOPIC, false, emptyHeaders());

        PowerMock.replayAll();

        // Try to send 3, make first pass, second fail. Should save last two
        workerTask.toSend = Arrays.asList(record1, record2, record3);
        workerTask.sendRecords();
        assertEquals(Arrays.asList(record3), workerTask.toSend);

        // Next they all succeed
        workerTask.sendRecords();
        assertNull(workerTask.toSend);

        PowerMock.verifyAll();
    }

    @Test
    public void testTopicDescribeFails() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls();
        EasyMock.expect(admin.describeTopics(TOPIC))
                .andThrow(new ConnectException(new TopicAuthorizationException("unauthorized")));

        PowerMock.replayAll();

        workerTask.toSend = Arrays.asList(record1, record2);
        assertThrows(ConnectException.class, workerTask::sendRecords);
    }

    @Test
    public void testTopicCreateFails() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls();
        EasyMock.expect(admin.describeTopics(TOPIC)).andReturn(Collections.emptyMap());

        Capture<NewTopic> newTopicCapture = EasyMock.newCapture();
        EasyMock.expect(admin.createOrFindTopics(EasyMock.capture(newTopicCapture)))
                .andThrow(new ConnectException(new TopicAuthorizationException("unauthorized")));

        PowerMock.replayAll();

        workerTask.toSend = Arrays.asList(record1, record2);
        assertThrows(ConnectException.class, workerTask::sendRecords);
        assertTrue(newTopicCapture.hasCaptured());
    }

    @Test
    public void testTopicCreateFailsWithExceptionWhenCreateReturnsTopicNotCreatedOrFound() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls();
        EasyMock.expect(admin.describeTopics(TOPIC)).andReturn(Collections.emptyMap());

        Capture<NewTopic> newTopicCapture = EasyMock.newCapture();
        EasyMock.expect(admin.createOrFindTopics(EasyMock.capture(newTopicCapture))).andReturn(TopicAdmin.EMPTY_CREATION);

        PowerMock.replayAll();

        workerTask.toSend = Arrays.asList(record1, record2);
        assertThrows(ConnectException.class, workerTask::sendRecords);
        assertTrue(newTopicCapture.hasCaptured());
    }

    @Test
    public void testTopicCreateSucceedsWhenCreateReturnsExistingTopicFound() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls();
        EasyMock.expect(admin.describeTopics(TOPIC)).andReturn(Collections.emptyMap());

        Capture<NewTopic> newTopicCapture = EasyMock.newCapture();
        EasyMock.expect(admin.createOrFindTopics(EasyMock.capture(newTopicCapture))).andReturn(foundTopic(TOPIC));

        expectSendRecord();
        expectSendRecord();

        PowerMock.replayAll();

        workerTask.toSend = Arrays.asList(record1, record2);
        workerTask.sendRecords();
    }

    @Test
    public void testTopicCreateSucceedsWhenCreateReturnsNewTopicFound() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls();
        EasyMock.expect(admin.describeTopics(TOPIC)).andReturn(Collections.emptyMap());

        Capture<NewTopic> newTopicCapture = EasyMock.newCapture();
        EasyMock.expect(admin.createOrFindTopics(EasyMock.capture(newTopicCapture))).andReturn(createdTopic(TOPIC));

        expectSendRecord();
        expectSendRecord();

        PowerMock.replayAll();

        workerTask.toSend = Arrays.asList(record1, record2);
        workerTask.sendRecords();
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecord(
            String topic,
            boolean anyTimes,
            Headers headers
    ) {
        if (headers != null)
            expectConvertHeadersAndKeyValue(topic, anyTimes, headers);

        expectApplyTransformationChain(anyTimes);

        Capture<ProducerRecord<byte[], byte[]>> sent = EasyMock.newCapture();

        IExpectationSetters<Future<RecordMetadata>> expect = EasyMock.expect(
                producer.send(EasyMock.capture(sent), EasyMock.capture(producerCallbacks)));

        IAnswer<Future<RecordMetadata>> expectResponse = () -> {
            synchronized (producerCallbacks) {
                for (Callback cb : producerCallbacks.getValues()) {
                    cb.onCompletion(new RecordMetadata(new TopicPartition("foo", 0), 0, 0, 0L, 0, 0), null);
                }
                producerCallbacks.reset();
            }
            return null;
        };

        if (anyTimes)
            expect.andStubAnswer(expectResponse);
        else
            expect.andAnswer(expectResponse);

        expectTaskGetTopic(anyTimes);

        return sent;
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordAnyTimes() {
        return expectSendRecord(TOPIC, true, emptyHeaders());
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecord() {
        return expectSendRecord(TOPIC, false, emptyHeaders());
    }

    private void expectTaskGetTopic(boolean anyTimes) {
        final Capture<String> connectorCapture = EasyMock.newCapture();
        final Capture<String> topicCapture = EasyMock.newCapture();
        IExpectationSetters<TopicStatus> expect = EasyMock.expect(statusBackingStore.getTopic(
                EasyMock.capture(connectorCapture),
                EasyMock.capture(topicCapture)));
        if (anyTimes) {
            expect.andStubAnswer(() -> new TopicStatus(
                    topicCapture.getValue(),
                    new ConnectorTaskId(connectorCapture.getValue(), 0),
                    Time.SYSTEM.milliseconds()));
        } else {
            expect.andAnswer(() -> new TopicStatus(
                    topicCapture.getValue(),
                    new ConnectorTaskId(connectorCapture.getValue(), 0),
                    Time.SYSTEM.milliseconds()));
        }
        if (connectorCapture.hasCaptured() && topicCapture.hasCaptured()) {
            assertEquals("job", connectorCapture.getValue());
            assertEquals(TOPIC, topicCapture.getValue());
        }
    }

    private void expectTopicCreation(String topic) {
        if (config.topicCreationEnable()) {
            EasyMock.expect(admin.describeTopics(topic)).andReturn(Collections.emptyMap());
            Capture<NewTopic> newTopicCapture = EasyMock.newCapture();
            EasyMock.expect(admin.createOrFindTopics(EasyMock.capture(newTopicCapture))).andReturn(createdTopic(topic));
        }
    }

    private TopicAdmin.TopicCreationResponse createdTopic(String topic) {
        Set<String> created = Collections.singleton(topic);
        Set<String> existing = Collections.emptySet();
        return new TopicAdmin.TopicCreationResponse(created, existing);
    }

    private TopicAdmin.TopicCreationResponse foundTopic(String topic) {
        Set<String> created = Collections.emptySet();
        Set<String> existing = Collections.singleton(topic);
        return new TopicAdmin.TopicCreationResponse(created, existing);
    }

    private void expectPreliminaryCalls() {
        expectPreliminaryCalls(TOPIC);
    }

    private void expectPreliminaryCalls(String topic) {
        expectConvertHeadersAndKeyValue(topic, true, emptyHeaders());
        expectApplyTransformationChain(false);
        PowerMock.expectLastCall();
    }

    private void expectConvertHeadersAndKeyValue(String topic, boolean anyTimes, Headers headers) {
        for (Header header : headers) {
            IExpectationSetters<byte[]> convertHeaderExpect = EasyMock.expect(headerConverter.fromConnectHeader(topic, header.key(), Schema.STRING_SCHEMA, new String(header.value())));
            if (anyTimes)
                convertHeaderExpect.andStubReturn(header.value());
            else
                convertHeaderExpect.andReturn(header.value());
        }
        IExpectationSetters<byte[]> convertKeyExpect = EasyMock.expect(keyConverter.fromConnectData(topic, headers, KEY_SCHEMA, KEY));
        if (anyTimes)
            convertKeyExpect.andStubReturn(SERIALIZED_KEY);
        else
            convertKeyExpect.andReturn(SERIALIZED_KEY);
        IExpectationSetters<byte[]> convertValueExpect = EasyMock.expect(valueConverter.fromConnectData(topic, headers, RECORD_SCHEMA, RECORD));
        if (anyTimes)
            convertValueExpect.andStubReturn(SERIALIZED_RECORD);
        else
            convertValueExpect.andReturn(SERIALIZED_RECORD);
    }

    private void expectApplyTransformationChain(boolean anyTimes) {
        final Capture<SourceRecord> recordCapture = EasyMock.newCapture();
        IExpectationSetters<SourceRecord> convertKeyExpect = EasyMock.expect(transformationChain.apply(EasyMock.capture(recordCapture)));
        if (anyTimes)
            convertKeyExpect.andStubAnswer(recordCapture::getValue);
        else
            convertKeyExpect.andAnswer(recordCapture::getValue);
    }

    private RecordHeaders emptyHeaders() {
        return new RecordHeaders();
    }

    private void createWorkerTask() {
        createWorkerTask(keyConverter, valueConverter, headerConverter);
    }

    private void createWorkerTask(Converter keyConverter, Converter valueConverter, HeaderConverter headerConverter) {
        workerTask = new AbstractWorkerSourceTask(
                taskId, sourceTask, statusListener, TargetState.STARTED, keyConverter, valueConverter, headerConverter, transformationChain,
                sourceTaskContext, producer, admin, TopicCreationGroup.configuredGroups(sourceConfig), offsetReader, offsetWriter, offsetStore,
                config, metrics, errorHandlingMetrics,  plugins.delegatingLoader(), Time.SYSTEM, RetryWithToleranceOperatorTest.NOOP_OPERATOR,
                statusBackingStore, Runnable::run) {
            @Override
            protected void prepareToInitializeTask() {
            }

            @Override
            protected void prepareToEnterSendLoop() {
            }

            @Override
            protected void beginSendIteration() {
            }

            @Override
            protected void prepareToPollTask() {
            }

            @Override
            protected void recordDropped(SourceRecord record) {
            }

            @Override
            protected Optional<SubmittedRecords.SubmittedRecord> prepareToSendRecord(SourceRecord sourceRecord, ProducerRecord<byte[], byte[]> producerRecord) {
                return Optional.empty();
            }

            @Override
            protected void recordDispatched(SourceRecord record) {
            }

            @Override
            protected void batchDispatched() {
            }

            @Override
            protected void recordSent(SourceRecord sourceRecord, ProducerRecord<byte[], byte[]> producerRecord, RecordMetadata recordMetadata) {
            }

            @Override
            protected void producerSendFailed(boolean synchronous, ProducerRecord<byte[], byte[]> producerRecord, SourceRecord preTransformRecord, Exception e) {
            }

            @Override
            protected void finalOffsetCommit(boolean failed) {
            }
        };

    }

}
