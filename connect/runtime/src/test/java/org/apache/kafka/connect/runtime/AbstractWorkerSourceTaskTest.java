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
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.StrictStubs.class)
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

    @Mock
    private SourceTask sourceTask;
    @Mock private TopicAdmin admin;
    @Mock private KafkaProducer<byte[], byte[]> producer;
    @Mock private Converter keyConverter;
    @Mock private Converter valueConverter;
    @Mock private HeaderConverter headerConverter;
    @Mock private TransformationChain<SourceRecord, SourceRecord> transformationChain;
    @Mock private CloseableOffsetStorageReader offsetReader;
    @Mock private OffsetStorageWriter offsetWriter;
    @Mock private ConnectorOffsetBackingStore offsetStore;
    @Mock private StatusBackingStore statusBackingStore;
    @Mock private WorkerSourceTaskContext sourceTaskContext;
    @Mock private TaskStatus.Listener statusListener;

    private final ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private final ConnectorTaskId taskId1 = new ConnectorTaskId("job", 1);

    private Plugins plugins;
    private WorkerConfig config;
    private SourceConnectorConfig sourceConfig;
    private MockConnectMetrics metrics;
    @Mock private ErrorHandlingMetrics errorHandlingMetrics;

    private AbstractWorkerSourceTask workerTask;

    @Before
    public void setup() {
        Map<String, String> workerProps = workerProps();
        plugins = new Plugins(workerProps);
        config = new StandaloneConfig(workerProps);
        sourceConfig = new SourceConnectorConfig(plugins, sourceConnectorPropsWithGroups(), true);
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

    private Map<String, String> sourceConnectorPropsWithGroups() {
        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put("name", "foo-connector");
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(1));
        props.put(TOPIC_CONFIG, TOPIC);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(TOPIC_CREATION_GROUPS_CONFIG, String.join(",", "foo", "bar"));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(1));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(1));
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + "foo" + "." + INCLUDE_REGEX_CONFIG, TOPIC);
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + "bar" + "." + INCLUDE_REGEX_CONFIG, ".*");
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + "bar" + "." + EXCLUDE_REGEX_CONFIG, TOPIC);
        return props;
    }

    @After
    public void tearDown() {
        if (metrics != null) metrics.stop();
        verifyNoMoreInteractions(statusListener);
    }

    @Test
    public void testMetricsGroup() {
        AbstractWorkerSourceTask.SourceTaskMetricsGroup group = new AbstractWorkerSourceTask.SourceTaskMetricsGroup(taskId, metrics);
        AbstractWorkerSourceTask.SourceTaskMetricsGroup group1 = new AbstractWorkerSourceTask.SourceTaskMetricsGroup(taskId1, metrics);
        for (int i = 0; i != 10; ++i) {
            group.recordPoll(100, 1000 + i * 100);
            group.recordWrite(10, 2);
        }
        for (int i = 0; i != 20; ++i) {
            group1.recordPoll(100, 1000 + i * 100);
            group1.recordWrite(10, 4);
        }
        assertEquals(1900.0, metrics.currentMetricValueAsDouble(group.metricGroup(), "poll-batch-max-time-ms"), 0.001d);
        assertEquals(1450.0, metrics.currentMetricValueAsDouble(group.metricGroup(), "poll-batch-avg-time-ms"), 0.001d);
        assertEquals(33.333, metrics.currentMetricValueAsDouble(group.metricGroup(), "source-record-poll-rate"), 0.001d);
        assertEquals(1000, metrics.currentMetricValueAsDouble(group.metricGroup(), "source-record-poll-total"), 0.001d);
        assertEquals(2.666, metrics.currentMetricValueAsDouble(group.metricGroup(), "source-record-write-rate"), 0.001d);
        assertEquals(80, metrics.currentMetricValueAsDouble(group.metricGroup(), "source-record-write-total"), 0.001d);
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
        assertEquals(4.0, metrics.currentMetricValueAsDouble(group1.metricGroup(), "source-record-write-rate"), 0.001d);
        assertEquals(120, metrics.currentMetricValueAsDouble(group1.metricGroup(), "source-record-write-total"), 0.001d);
        assertEquals(1800.0, metrics.currentMetricValueAsDouble(group1.metricGroup(), "source-record-active-count"), 0.001d);
    }

    @Test
    public void testSendRecordsConvertsData() {
        createWorkerTask();

        // Can just use the same record for key and value
        List<SourceRecord> records = Collections.singletonList(
            new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD)
        );

        expectSendRecord(emptyHeaders());
        expectTopicCreation(TOPIC);

        workerTask.toSend = records;
        workerTask.sendRecords();

        ArgumentCaptor<ProducerRecord<byte[], byte[]>> sent = verifySendRecord();

        assertArrayEquals(SERIALIZED_KEY, sent.getValue().key());
        assertArrayEquals(SERIALIZED_RECORD, sent.getValue().value());
        
        verifyTaskGetTopic();
        verifyTopicCreation();
    }

    @Test
    public void testSendRecordsPropagatesTimestamp() {
        final Long timestamp = System.currentTimeMillis();
        createWorkerTask();

        expectSendRecord(emptyHeaders());
        expectTopicCreation(TOPIC);

        workerTask.toSend = Collections.singletonList(
                new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD, timestamp)
        );
        workerTask.sendRecords();

        ArgumentCaptor<ProducerRecord<byte[], byte[]>> sent = verifySendRecord();
        assertEquals(timestamp, sent.getValue().timestamp());

        verifyTaskGetTopic();
        verifyTopicCreation();
    }

    @Test
    public void testSendRecordsCorruptTimestamp() {
        final Long timestamp = -3L;
        createWorkerTask();

        expectSendRecord(emptyHeaders());

        workerTask.toSend = Collections.singletonList(
                new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD, timestamp)
        );
        assertThrows(InvalidRecordException.class, workerTask::sendRecords);
        verifyNoInteractions(producer);
        verifyNoInteractions(admin);
    }

    @Test
    public void testSendRecordsNoTimestamp() {
        final Long timestamp = -1L;
        createWorkerTask();

        expectSendRecord(emptyHeaders());
        expectTopicCreation(TOPIC);

        workerTask.toSend = Collections.singletonList(
                new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD, timestamp)
        );
        workerTask.sendRecords();

        ArgumentCaptor<ProducerRecord<byte[], byte[]>> sent = verifySendRecord();
        assertNull(sent.getValue().timestamp());

        verifyTaskGetTopic();
        verifyTopicCreation();
    }

    @Test
    public void testHeaders() {
        Headers headers = new RecordHeaders()
                .add("header_key", "header_value".getBytes());

        org.apache.kafka.connect.header.Headers connectHeaders = new ConnectHeaders()
                .add("header_key", new SchemaAndValue(Schema.STRING_SCHEMA, "header_value"));

        createWorkerTask();

        expectSendRecord(headers);
        expectTopicCreation(TOPIC);

        workerTask.toSend = Collections.singletonList(
            new SourceRecord(PARTITION, OFFSET, TOPIC, null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD,
                null, connectHeaders)
        );
        workerTask.sendRecords();

        ArgumentCaptor<ProducerRecord<byte[], byte[]>> sent = verifySendRecord();

        assertArrayEquals(SERIALIZED_KEY, sent.getValue().key());
        assertArrayEquals(SERIALIZED_RECORD, sent.getValue().value());
        assertEquals(headers, sent.getValue().headers());

        verifyTaskGetTopic();
        verifyTopicCreation();
    }

    @Test
    public void testHeadersWithCustomConverter() throws Exception {
        StringConverter stringConverter = new StringConverter();
        SampleConverterWithHeaders testConverter = new SampleConverterWithHeaders();

        createWorkerTask(stringConverter, testConverter, stringConverter, RetryWithToleranceOperatorTest.noopOperator(),
                Collections::emptyList);

        expectSendRecord(null);
        expectTopicCreation(TOPIC);

        String stringA = "Árvíztűrő tükörfúrógép";
        String encodingA = "latin2";
        String stringB = "Тестовое сообщение";
        String encodingB = "koi8_r";

        org.apache.kafka.connect.header.Headers headersA = new ConnectHeaders()
            .addString("encoding", encodingA);
        org.apache.kafka.connect.header.Headers headersB = new ConnectHeaders()
            .addString("encoding", encodingB);

        workerTask.toSend = Arrays.asList(
            new SourceRecord(PARTITION, OFFSET, "topic", null, Schema.STRING_SCHEMA, "a",
                Schema.STRING_SCHEMA, stringA, null, headersA),
            new SourceRecord(PARTITION, OFFSET, "topic", null, Schema.STRING_SCHEMA, "b",
                Schema.STRING_SCHEMA, stringB, null, headersB)
        );
        workerTask.sendRecords();

        ArgumentCaptor<ProducerRecord<byte[], byte[]>> sent = verifySendRecord(2);

        List<ProducerRecord<byte[], byte[]>> capturedValues = sent.getAllValues();
        assertEquals(2, capturedValues.size());

        ProducerRecord<byte[], byte[]> sentRecordA = capturedValues.get(0);
        ProducerRecord<byte[], byte[]> sentRecordB = capturedValues.get(1);

        assertEquals(ByteBuffer.wrap("a".getBytes()), ByteBuffer.wrap(sentRecordA.key()));
        assertEquals(
            ByteBuffer.wrap(stringA.getBytes(encodingA)),
            ByteBuffer.wrap(sentRecordA.value())
        );
        assertEquals(encodingA, new String(sentRecordA.headers().lastHeader("encoding").value()));

        assertEquals(ByteBuffer.wrap("b".getBytes()), ByteBuffer.wrap(sentRecordB.key()));
        assertEquals(
            ByteBuffer.wrap(stringB.getBytes(encodingB)),
            ByteBuffer.wrap(sentRecordB.value())
        );
        assertEquals(encodingB, new String(sentRecordB.headers().lastHeader("encoding").value()));

        verifyTaskGetTopic(2);
        verifyTopicCreation();
    }

    @Test
    public void testTopicCreateWhenTopicExists() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls(TOPIC);

        TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, null, Collections.emptyList(), Collections.emptyList());
        TopicDescription topicDesc = new TopicDescription(TOPIC, false, Collections.singletonList(topicPartitionInfo));
        when(admin.describeTopics(TOPIC)).thenReturn(Collections.singletonMap(TOPIC, topicDesc));

        expectSendRecord(emptyHeaders());

        workerTask.toSend = Arrays.asList(record1, record2);
        workerTask.sendRecords();

        verifySendRecord(2);
        verify(admin, never()).createOrFindTopics(any(NewTopic.class));
        // Make sure we didn't try to create the topic after finding out it already existed
        verifyNoMoreInteractions(admin);
    }

    @Test
    public void testSendRecordsTopicDescribeRetries() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls(TOPIC);

        when(admin.describeTopics(TOPIC))
                .thenThrow(new RetriableException(new TimeoutException("timeout")))
                .thenReturn(Collections.emptyMap());

        workerTask.toSend = Arrays.asList(record1, record2);
        workerTask.sendRecords();
        assertEquals(Arrays.asList(record1, record2), workerTask.toSend);
        verify(admin, never()).createOrFindTopics(any(NewTopic.class));
        verifyNoMoreInteractions(admin);

        // Second round - calls to describe and create succeed
        expectTopicCreation(TOPIC);
        workerTask.sendRecords();
        assertNull(workerTask.toSend);

        verifyTopicCreation();
    }

    @Test
    public void testSendRecordsTopicCreateRetries() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls(TOPIC);

        when(admin.describeTopics(TOPIC)).thenReturn(Collections.emptyMap());
        when(admin.createOrFindTopics(any(NewTopic.class)))
                // First call to create the topic times out
                .thenThrow(new RetriableException(new TimeoutException("timeout")))
                // Next attempt succeeds
                .thenReturn(createdTopic(TOPIC));

        workerTask.toSend = Arrays.asList(record1, record2);
        workerTask.sendRecords();
        assertEquals(Arrays.asList(record1, record2), workerTask.toSend);

        // Next they all succeed
        workerTask.sendRecords();
        assertNull(workerTask.toSend);

        // First attempt failed, second succeeded
        verifyTopicCreation(2, TOPIC, TOPIC);
    }

    @Test
    public void testSendRecordsTopicDescribeRetriesMidway() {
        createWorkerTask();

        // Differentiate only by Kafka partition so we can reuse conversion expectations
        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record3 = new SourceRecord(PARTITION, OFFSET, OTHER_TOPIC, 3, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls(TOPIC);
        expectPreliminaryCalls(OTHER_TOPIC);

        when(admin.describeTopics(anyString()))
                .thenReturn(Collections.emptyMap())
                .thenThrow(new RetriableException(new TimeoutException("timeout")))
                .thenReturn(Collections.emptyMap());
        when(admin.createOrFindTopics(any(NewTopic.class))).thenAnswer(
            (Answer<TopicAdmin.TopicCreationResponse>) invocation -> {
                NewTopic newTopic = invocation.getArgument(0);
                return createdTopic(newTopic.name());
            });

        // Try to send 3, make first pass, second fail. Should save last record
        workerTask.toSend = Arrays.asList(record1, record2, record3);
        workerTask.sendRecords();
        assertEquals(Collections.singletonList(record3), workerTask.toSend);

        // Next they all succeed
        workerTask.sendRecords();
        assertNull(workerTask.toSend);

        verify(admin, times(3)).describeTopics(anyString());

        ArgumentCaptor<NewTopic> newTopicCaptor = ArgumentCaptor.forClass(NewTopic.class);
        verify(admin, times(2)).createOrFindTopics(newTopicCaptor.capture());

        assertEquals(Arrays.asList(TOPIC, OTHER_TOPIC), newTopicCaptor.getAllValues()
            .stream()
            .map(NewTopic::name)
            .collect(Collectors.toList()));
    }

    @Test
    public void testSendRecordsTopicCreateRetriesMidway() {
        createWorkerTask();

        // Differentiate only by Kafka partition so we can reuse conversion expectations
        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record3 = new SourceRecord(PARTITION, OFFSET, OTHER_TOPIC, 3, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls(TOPIC);
        expectPreliminaryCalls(OTHER_TOPIC);

        when(admin.describeTopics(anyString())).thenReturn(Collections.emptyMap());
        when(admin.createOrFindTopics(any(NewTopic.class)))
                .thenReturn(createdTopic(TOPIC))
                .thenThrow(new RetriableException(new TimeoutException("timeout")))
                .thenReturn(createdTopic(OTHER_TOPIC));

        // Try to send 3, make first pass, second fail. Should save last record
        workerTask.toSend = Arrays.asList(record1, record2, record3);
        workerTask.sendRecords();
        assertEquals(Collections.singletonList(record3), workerTask.toSend);
        verifyTopicCreation(2,  TOPIC, OTHER_TOPIC); // Second call to createOrFindTopics will throw

        // Next they all succeed
        workerTask.sendRecords();
        assertNull(workerTask.toSend);

        verifyTopicCreation(3,  TOPIC, OTHER_TOPIC, OTHER_TOPIC);
    }

    @Test
    public void testTopicDescribeFails() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls(TOPIC);
        when(admin.describeTopics(TOPIC)).thenThrow(
            new ConnectException(new TopicAuthorizationException("unauthorized"))
        );

        workerTask.toSend = Arrays.asList(record1, record2);
        assertThrows(ConnectException.class, workerTask::sendRecords);
    }

    @Test
    public void testTopicCreateFails() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls(TOPIC);
        when(admin.describeTopics(TOPIC)).thenReturn(Collections.emptyMap());
        when(admin.createOrFindTopics(any(NewTopic.class))).thenThrow(
            new ConnectException(new TopicAuthorizationException("unauthorized"))
        );

        workerTask.toSend = Arrays.asList(record1, record2);
        assertThrows(ConnectException.class, workerTask::sendRecords);
        verify(admin).createOrFindTopics(any());

        verifyTopicCreation();
    }

    @Test
    public void testTopicCreateFailsWithExceptionWhenCreateReturnsTopicNotCreatedOrFound() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls(TOPIC);

        when(admin.describeTopics(TOPIC)).thenReturn(Collections.emptyMap());
        when(admin.createOrFindTopics(any(NewTopic.class))).thenReturn(TopicAdmin.EMPTY_CREATION);

        workerTask.toSend = Arrays.asList(record1, record2);
        assertThrows(ConnectException.class, workerTask::sendRecords);
        verify(admin).createOrFindTopics(any());

        verifyTopicCreation();
    }

    @Test
    public void testTopicCreateSucceedsWhenCreateReturnsExistingTopicFound() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectSendRecord(emptyHeaders());

        when(admin.describeTopics(TOPIC)).thenReturn(Collections.emptyMap());
        when(admin.createOrFindTopics(any(NewTopic.class))).thenReturn(foundTopic(TOPIC));

        workerTask.toSend = Arrays.asList(record1, record2);
        workerTask.sendRecords();

        ArgumentCaptor<ProducerRecord<byte[], byte[]>> sent = verifySendRecord(2);

        List<ProducerRecord<byte[], byte[]>> capturedValues = sent.getAllValues();
        assertEquals(2, capturedValues.size());

        verifyTaskGetTopic(2);
        verifyTopicCreation();
    }

    @Test
    public void testTopicCreateSucceedsWhenCreateReturnsNewTopicFound() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectSendRecord(emptyHeaders());

        when(admin.describeTopics(TOPIC)).thenReturn(Collections.emptyMap());
        when(admin.createOrFindTopics(any(NewTopic.class))).thenReturn(createdTopic(TOPIC));

        workerTask.toSend = Arrays.asList(record1, record2);
        workerTask.sendRecords();

        ArgumentCaptor<ProducerRecord<byte[], byte[]>> sent = verifySendRecord(2);

        List<ProducerRecord<byte[], byte[]>> capturedValues = sent.getAllValues();
        assertEquals(2, capturedValues.size());

        verifyTaskGetTopic(2);
        verifyTopicCreation();
    }

    @Test
    public void testSendRecordsRetriableException() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record3 = new SourceRecord(PARTITION, OFFSET, TOPIC, 3, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectConvertHeadersAndKeyValue(emptyHeaders(), TOPIC);
        expectTaskGetTopic();

        when(transformationChain.apply(any(), eq(record1))).thenReturn(null);
        when(transformationChain.apply(any(), eq(record2))).thenReturn(null);
        when(transformationChain.apply(any(), eq(record3))).thenReturn(record3);

        TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, null, Collections.emptyList(), Collections.emptyList());
        TopicDescription topicDesc = new TopicDescription(TOPIC, false, Collections.singletonList(topicPartitionInfo));
        when(admin.describeTopics(TOPIC)).thenReturn(Collections.singletonMap(TOPIC, topicDesc));

        when(producer.send(any(), any())).thenThrow(new RetriableException("Retriable exception")).thenReturn(null);

        workerTask.toSend = Arrays.asList(record1, record2, record3);

        // The first two records are filtered out / dropped by the transformation chain; only the third record will be attempted to be sent.
        // The producer throws a RetriableException the first time we try to send the third record
        assertFalse(workerTask.sendRecords());

        // The next attempt to send the third record should succeed
        assertTrue(workerTask.sendRecords());

        // Ensure that the first two records that were filtered out by the transformation chain
        // aren't re-processed when we retry the call to sendRecords()
        verify(transformationChain, times(1)).apply(any(), eq(record1));
        verify(transformationChain, times(1)).apply(any(), eq(record2));
        verify(transformationChain, times(2)).apply(any(), eq(record3));
    }

    private void expectSendRecord(Headers headers) {
        if (headers != null)
            expectConvertHeadersAndKeyValue(headers, TOPIC);

        expectApplyTransformationChain();

        expectTaskGetTopic();
    }

    private ArgumentCaptor<ProducerRecord<byte[], byte[]>> verifySendRecord() {
        return verifySendRecord(1);
    }

    private ArgumentCaptor<ProducerRecord<byte[], byte[]>> verifySendRecord(int times) {
        ArgumentCaptor<ProducerRecord<byte[], byte[]>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        ArgumentCaptor<Callback> producerCallbacks = ArgumentCaptor.forClass(Callback.class);
        verify(producer, times(times)).send(sent.capture(), producerCallbacks.capture());

        for (Callback cb : producerCallbacks.getAllValues()) {
            cb.onCompletion(new RecordMetadata(new TopicPartition("foo", 0), 0, 0, 0L, 0, 0),
                    null);
        }

        return sent;
    }

    private void expectTaskGetTopic() {
        when(statusBackingStore.getTopic(anyString(), anyString())).thenAnswer((Answer<TopicStatus>) invocation -> {
            String connector = invocation.getArgument(0, String.class);
            String topic = invocation.getArgument(1, String.class);
            return new TopicStatus(topic, new ConnectorTaskId(connector, 0), Time.SYSTEM.milliseconds());
        });
    }

    private void verifyTaskGetTopic() {
        verifyTaskGetTopic(1);
    }
    private void verifyTaskGetTopic(int times) {
        ArgumentCaptor<String> connectorCapture = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> topicCapture = ArgumentCaptor.forClass(String.class);
        verify(statusBackingStore, times(times)).getTopic(connectorCapture.capture(), topicCapture.capture());

        assertEquals("job", connectorCapture.getValue());
        assertEquals(TOPIC, topicCapture.getValue());
    }

    @SuppressWarnings("SameParameterValue")
    private void expectTopicCreation(String topic) {
        when(admin.createOrFindTopics(any(NewTopic.class))).thenReturn(createdTopic(topic));
    }

    private void verifyTopicCreation() {
        verifyTopicCreation(1, TOPIC);
    }
    private void verifyTopicCreation(int times, String... topics) {
        ArgumentCaptor<NewTopic> newTopicCapture = ArgumentCaptor.forClass(NewTopic.class);

        verify(admin, times(times)).createOrFindTopics(newTopicCapture.capture());
        assertArrayEquals(topics, newTopicCapture.getAllValues()
                .stream()
                .map(NewTopic::name)
                .toArray(String[]::new));
    }

    @SuppressWarnings("SameParameterValue")
    private TopicAdmin.TopicCreationResponse createdTopic(String topic) {
        Set<String> created = Collections.singleton(topic);
        Set<String> existing = Collections.emptySet();
        return new TopicAdmin.TopicCreationResponse(created, existing);
    }

    @SuppressWarnings("SameParameterValue")
    private TopicAdmin.TopicCreationResponse foundTopic(String topic) {
        Set<String> created = Collections.emptySet();
        Set<String> existing = Collections.singleton(topic);
        return new TopicAdmin.TopicCreationResponse(created, existing);
    }

    private void expectPreliminaryCalls(String topic) {
        expectConvertHeadersAndKeyValue(emptyHeaders(), topic);
        expectApplyTransformationChain();
    }

    private void expectConvertHeadersAndKeyValue(Headers headers, String topic) {
        if (headers.iterator().hasNext()) {
            when(headerConverter.fromConnectHeader(anyString(), anyString(), eq(Schema.STRING_SCHEMA),
                    anyString()))
                    .thenAnswer((Answer<byte[]>) invocation -> {
                        String headerValue = invocation.getArgument(3, String.class);
                        return headerValue.getBytes(StandardCharsets.UTF_8);
                    });
        }

        when(keyConverter.fromConnectData(eq(topic), any(Headers.class), eq(KEY_SCHEMA), eq(KEY)))
                .thenReturn(SERIALIZED_KEY);
        when(valueConverter.fromConnectData(eq(topic), any(Headers.class), eq(RECORD_SCHEMA),
                eq(RECORD)))
                .thenReturn(SERIALIZED_RECORD);
    }

    private void expectApplyTransformationChain() {
        when(transformationChain.apply(any(), any(SourceRecord.class)))
                .thenAnswer(AdditionalAnswers.returnsSecondArg());
    }

    private RecordHeaders emptyHeaders() {
        return new RecordHeaders();
    }

    private void createWorkerTask() {
        createWorkerTask(keyConverter, valueConverter, headerConverter, RetryWithToleranceOperatorTest.noopOperator(), Collections::emptyList);
    }

    private void createWorkerTask(Converter keyConverter, Converter valueConverter, HeaderConverter headerConverter,
                                  RetryWithToleranceOperator<SourceRecord> retryWithToleranceOperator, Supplier<List<ErrorReporter<SourceRecord>>> errorReportersSupplier) {
        workerTask = new AbstractWorkerSourceTask(
                taskId, sourceTask, statusListener, TargetState.STARTED, keyConverter, valueConverter, headerConverter, transformationChain,
                sourceTaskContext, producer, admin, TopicCreationGroup.configuredGroups(sourceConfig), offsetReader, offsetWriter, offsetStore,
                config, metrics, errorHandlingMetrics,  plugins.delegatingLoader(), Time.SYSTEM, retryWithToleranceOperator,
                statusBackingStore, Runnable::run, errorReportersSupplier) {
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
            protected void producerSendFailed(ProcessingContext<SourceRecord> context, boolean synchronous, ProducerRecord<byte[], byte[]> producerRecord, SourceRecord preTransformRecord, Exception e) {
            }

            @Override
            protected void finalOffsetCommit(boolean failed) {
            }
        };
    }
}
