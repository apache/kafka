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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.integration.MonitorableSourceConnector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.apache.kafka.connect.runtime.errors.LogReporter;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.runtime.errors.WorkerErrantRecordReporter;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.isolation.PluginUtils;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.TopicCreationGroup;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Time.SYSTEM;
import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TOPIC_CREATION_GROUPS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.INCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_CREATION_ENABLE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ErrorHandlingTaskTest {

    private static final String TOPIC = "test";
    private static final int PARTITION1 = 12;
    private static final int PARTITION2 = 13;
    private static final long FIRST_OFFSET = 45;

    @Mock
    Plugins plugins;

    private static final Map<String, String> TASK_PROPS = new HashMap<>();

    static {
        TASK_PROPS.put(SinkConnector.TOPICS_CONFIG, TOPIC);
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, TestSinkTask.class.getName());
    }

    public static final long OPERATOR_RETRY_TIMEOUT_MILLIS = 60000;
    public static final long OPERATOR_RETRY_MAX_DELAY_MILLIS = 5000;
    public static final ToleranceType OPERATOR_TOLERANCE_TYPE = ToleranceType.ALL;

    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    private final ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private final TargetState initialState = TargetState.STARTED;
    private Time time;
    private MockConnectMetrics metrics;
    @SuppressWarnings("unused")
    @Mock
    private SinkTask sinkTask;
    @SuppressWarnings("unused")
    @Mock
    private SourceTask sourceTask;
    private WorkerConfig workerConfig;
    private SourceConnectorConfig sourceConfig;
    @Mock
    private PluginClassLoader pluginLoader;
    @SuppressWarnings("unused")
    @Mock
    private HeaderConverter headerConverter;
    private WorkerSinkTask workerSinkTask;
    private WorkerSourceTask workerSourceTask;
    @SuppressWarnings("unused")
    @Mock
    private KafkaConsumer<byte[], byte[]> consumer;
    @SuppressWarnings("unused")
    @Mock
    private KafkaProducer<byte[], byte[]> producer;
    @SuppressWarnings("unused")
    @Mock private TopicAdmin admin;

    @Mock
    OffsetStorageReaderImpl offsetReader;
    @Mock
    OffsetStorageWriter offsetWriter;
    @Mock
    private ConnectorOffsetBackingStore offsetStore;
    @SuppressWarnings("unused")
    @Mock
    private TaskStatus.Listener statusListener;
    @SuppressWarnings("unused")
    @Mock private StatusBackingStore statusBackingStore;

    @Mock
    private WorkerErrantRecordReporter workerErrantRecordReporter;

    private ErrorHandlingMetrics errorHandlingMetrics;

    private boolean enableTopicCreation;


    public void setup(boolean enableTopicCreation) {
        this.enableTopicCreation = enableTopicCreation;
        time = new MockTime(0, 0, 0);
        metrics = new MockConnectMetrics();
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerProps.put(TOPIC_CREATION_ENABLE_CONFIG, String.valueOf(enableTopicCreation));
        workerConfig = new StandaloneConfig(workerProps);
        sourceConfig = new SourceConnectorConfig(plugins, sourceConnectorProps(TOPIC), true);
        errorHandlingMetrics = new ErrorHandlingMetrics(taskId, metrics);
    }

    private Map<String, String> sourceConnectorProps(String topic) {
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
        return props;
    }

    @AfterEach
    public void tearDown() {
        if (metrics != null) {
            metrics.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testErrorHandlingInSinkTasks(boolean enableTopicCreation) {
        setup(enableTopicCreation);
        Map<String, String> reportProps = new HashMap<>();
        reportProps.put(ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, "true");
        reportProps.put(ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG, "true");
        LogReporter<ConsumerRecord<byte[], byte[]>> reporter = new LogReporter.Sink(taskId, connConfig(reportProps), errorHandlingMetrics);

        RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> retryWithToleranceOperator = operator();
        createSinkTask(initialState, retryWithToleranceOperator, singletonList(reporter));

        // valid json
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>(
                TOPIC, PARTITION1, FIRST_OFFSET,
                null, "{\"a\": 10}".getBytes());
        // bad json
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>(
                TOPIC, PARTITION2, FIRST_OFFSET,
                null, "{\"a\" 10}".getBytes());

        when(consumer.poll(any()))
                .thenReturn(records(record1))
                .thenReturn(records(record2));

        workerSinkTask.initialize(TASK_CONFIG);
        workerSinkTask.initializeAndStart();
        workerSinkTask.iteration();

        workerSinkTask.iteration();

        verifyInitializeSink();
        verify(sinkTask, times(2)).put(any());

        // two records were consumed from Kafka
        assertSinkMetricValue("sink-record-read-total", 2.0);
        // only one was written to the task
        assertSinkMetricValue("sink-record-send-total", 1.0);
        // one record completely failed (converter issues)
        assertErrorHandlingMetricValue("total-record-errors", 1.0);
        // 2 failures in the transformation, and 1 in the converter
        assertErrorHandlingMetricValue("total-record-failures", 3.0);
        // one record completely failed (converter issues), and thus was skipped
        assertErrorHandlingMetricValue("total-records-skipped", 1.0);
    }

    private <T> RetryWithToleranceOperator<T> operator() {
        return new RetryWithToleranceOperator<>(OPERATOR_RETRY_TIMEOUT_MILLIS,
                OPERATOR_RETRY_MAX_DELAY_MILLIS, OPERATOR_TOLERANCE_TYPE,
                SYSTEM, errorHandlingMetrics);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testErrorHandlingInSourceTasks(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        Map<String, String> reportProps = new HashMap<>();
        reportProps.put(ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, "true");
        reportProps.put(ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG, "true");
        LogReporter<SourceRecord> reporter = new LogReporter.Source(taskId, connConfig(reportProps), errorHandlingMetrics);

        RetryWithToleranceOperator<SourceRecord> retryWithToleranceOperator = operator();
        createSourceTask(initialState, retryWithToleranceOperator, singletonList(reporter));

        // valid json
        Schema valSchema = SchemaBuilder.struct().field("val", Schema.INT32_SCHEMA).build();
        Struct struct1 = new Struct(valSchema).put("val", 1234);
        SourceRecord record1 = new SourceRecord(emptyMap(), emptyMap(), TOPIC, PARTITION1, valSchema, struct1);
        Struct struct2 = new Struct(valSchema).put("val", 6789);
        SourceRecord record2 = new SourceRecord(emptyMap(), emptyMap(), TOPIC, PARTITION1, valSchema, struct2);

        when(workerSourceTask.isStopping())
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true);

        doReturn(true).when(workerSourceTask).commitOffsets();

        when(sourceTask.poll())
                .thenReturn(singletonList(record1))
                .thenReturn(singletonList(record2));

        expectTopicCreation(TOPIC);

        workerSourceTask.initialize(TASK_CONFIG);
        workerSourceTask.initializeAndStart();
        workerSourceTask.execute();
        verify(workerSourceTask, times(3)).isStopping();
        verify(workerSourceTask).commitOffsets();
        verify(offsetStore).start();
        verify(sourceTask).initialize(any());
        verify(sourceTask).start(any());
        verify(sourceTask, times(2)).poll();
        verify(producer, times(2)).send(any(), any());
        // two records were consumed from Kafka
        assertSourceMetricValue("source-record-poll-total", 2.0);
        // only one was written to the task
        assertSourceMetricValue("source-record-write-total", 0.0);
        // one record completely failed (converter issues)
        assertErrorHandlingMetricValue("total-record-errors", 0.0);
        // 2 failures in the transformation, and 1 in the converter
        assertErrorHandlingMetricValue("total-record-failures", 4.0);
        // one record completely failed (converter issues), and thus was skipped
        assertErrorHandlingMetricValue("total-records-skipped", 0.0);
    }

    private ConnectorConfig connConfig(Map<String, String> connProps) {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.NAME_CONFIG, "test");
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SinkTask.class.getName());
        props.putAll(connProps);
        return new ConnectorConfig(plugins, props);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testErrorHandlingInSourceTasksWithBadConverter(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        Map<String, String> reportProps = new HashMap<>();
        reportProps.put(ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, "true");
        reportProps.put(ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG, "true");
        LogReporter<SourceRecord> reporter = new LogReporter.Source(taskId, connConfig(reportProps), errorHandlingMetrics);

        RetryWithToleranceOperator<SourceRecord> retryWithToleranceOperator = operator();
        createSourceTask(initialState, retryWithToleranceOperator, singletonList(reporter), badConverter());

        // valid json
        Schema valSchema = SchemaBuilder.struct().field("val", Schema.INT32_SCHEMA).build();
        Struct struct1 = new Struct(valSchema).put("val", 1234);
        SourceRecord record1 = new SourceRecord(emptyMap(), emptyMap(), TOPIC, PARTITION1, valSchema, struct1);
        Struct struct2 = new Struct(valSchema).put("val", 6789);
        SourceRecord record2 = new SourceRecord(emptyMap(), emptyMap(), TOPIC, PARTITION1, valSchema, struct2);

        when(workerSourceTask.isStopping())
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true);

        doReturn(true).when(workerSourceTask).commitOffsets();

        when(sourceTask.poll())
                .thenReturn(singletonList(record1))
                .thenReturn(singletonList(record2));
        expectTopicCreation(TOPIC);
        workerSourceTask.initialize(TASK_CONFIG);
        workerSourceTask.initializeAndStart();
        workerSourceTask.execute();
        // two records were consumed from Kafka
        assertSourceMetricValue("source-record-poll-total", 2.0);
        // only one was written to the task
        assertSourceMetricValue("source-record-write-total", 0.0);
        // one record completely failed (converter issues)
        assertErrorHandlingMetricValue("total-record-errors", 0.0);
        // 2 failures in the transformation, and 1 in the converter
        assertErrorHandlingMetricValue("total-record-failures", 8.0);
        // one record completely failed (converter issues), and thus was skipped
        assertErrorHandlingMetricValue("total-records-skipped", 0.0);

        verify(workerSourceTask, times(3)).isStopping();
        verify(workerSourceTask).commitOffsets();
        verify(offsetStore).start();
        verify(sourceTask).initialize(any());
        verify(sourceTask).start(any());
        verify(sourceTask, times(2)).poll();
        verify(producer, times(2)).send(any(), any());
    }

    private void assertSinkMetricValue(String name, double expected) {
        ConnectMetrics.MetricGroup sinkTaskGroup = workerSinkTask.sinkTaskMetricsGroup().metricGroup();
        double measured = metrics.currentMetricValueAsDouble(sinkTaskGroup, name);
        assertEquals(expected, measured, 0.001d);
    }

    private void verifyInitializeSink() {
        verify(sinkTask).start(TASK_PROPS);
        verify(sinkTask).initialize(any(WorkerSinkTaskContext.class));
        verify(consumer).subscribe(eq(singletonList(TOPIC)),
                any(ConsumerRebalanceListener.class));
    }

    private void assertSourceMetricValue(String name, double expected) {
        ConnectMetrics.MetricGroup sinkTaskGroup = workerSourceTask.sourceTaskMetricsGroup().metricGroup();
        double measured = metrics.currentMetricValueAsDouble(sinkTaskGroup, name);
        assertEquals(expected, measured, 0.001d);
    }

    private void assertErrorHandlingMetricValue(String name, double expected) {
        ConnectMetrics.MetricGroup sinkTaskGroup = errorHandlingMetrics.metricGroup();
        double measured = metrics.currentMetricValueAsDouble(sinkTaskGroup, name);
        assertEquals(expected, measured, 0.001d);
    }

    private void expectTopicCreation(String topic) {
        if (enableTopicCreation) {
            when(admin.describeTopics(topic)).thenReturn(Collections.emptyMap());
            Set<String> created = Collections.singleton(topic);
            Set<String> existing = Collections.emptySet();
            TopicAdmin.TopicCreationResponse response = new TopicAdmin.TopicCreationResponse(created, existing);
            when(admin.createOrFindTopics(any(NewTopic.class))).thenReturn(response);
        }
    }

    private void createSinkTask(TargetState initialState, RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> retryWithToleranceOperator,
                                List<ErrorReporter<ConsumerRecord<byte[], byte[]>>> errorReporters) {
        JsonConverter converter = new JsonConverter();
        Map<String, Object> oo = workerConfig.originalsWithPrefix("value.converter.");
        oo.put("converter.type", "value");
        oo.put("schemas.enable", "false");
        converter.configure(oo);

        TransformationChain<ConsumerRecord<byte[], byte[]>, SinkRecord> sinkTransforms =
                new TransformationChain<>(singletonList(new TransformationStage<>(new FaultyPassthrough<SinkRecord>())), retryWithToleranceOperator);

        workerSinkTask = new WorkerSinkTask(
            taskId, sinkTask, statusListener, initialState, workerConfig,
            ClusterConfigState.EMPTY, metrics, converter, converter, errorHandlingMetrics,
            headerConverter, sinkTransforms, consumer, pluginLoader, time,
            retryWithToleranceOperator, workerErrantRecordReporter,
                statusBackingStore, () -> errorReporters, PluginUtils.noOpLoaderSwap());
    }

    private void createSourceTask(TargetState initialState, RetryWithToleranceOperator<SourceRecord> retryWithToleranceOperator, List<ErrorReporter<SourceRecord>> errorReporters) {
        JsonConverter converter = new JsonConverter();
        Map<String, Object> oo = workerConfig.originalsWithPrefix("value.converter.");
        oo.put("converter.type", "value");
        oo.put("schemas.enable", "false");
        converter.configure(oo);

        createSourceTask(initialState, retryWithToleranceOperator, errorReporters, converter);
    }

    private Converter badConverter() {
        FaultyConverter converter = new FaultyConverter();
        Map<String, Object> oo = workerConfig.originalsWithPrefix("value.converter.");
        oo.put("converter.type", "value");
        oo.put("schemas.enable", "false");
        converter.configure(oo);
        return converter;
    }

    private void createSourceTask(TargetState initialState, RetryWithToleranceOperator<SourceRecord> retryWithToleranceOperator,
                                  List<ErrorReporter<SourceRecord>> errorReporters, Converter converter) {
        TransformationChain<SourceRecord, SourceRecord> sourceTransforms = new TransformationChain<>(singletonList(
                new TransformationStage<>(new FaultyPassthrough<SourceRecord>())), retryWithToleranceOperator);

        workerSourceTask = spy(new WorkerSourceTask(
            taskId, sourceTask, statusListener, initialState, converter,
                converter, errorHandlingMetrics, headerConverter,
                sourceTransforms, producer, admin,
                TopicCreationGroup.configuredGroups(sourceConfig),
                offsetReader, offsetWriter, offsetStore, workerConfig,
                ClusterConfigState.EMPTY, metrics, pluginLoader, time,
                retryWithToleranceOperator,
                statusBackingStore, Runnable::run, () -> errorReporters, PluginUtils.noOpLoaderSwap()));

    }

    private ConsumerRecords<byte[], byte[]> records(ConsumerRecord<byte[], byte[]> record) {
        final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        return new ConsumerRecords<>(Map.of(tp, List.of(record)), Map.of(tp, new OffsetAndMetadata(record.offset() + 1, record.leaderEpoch(), "")));
    }

    private abstract static class TestSinkTask extends SinkTask {
    }

    // Public to allow plugin discovery to complete without errors
    public static class FaultyConverter extends JsonConverter implements Versioned {
        private static final Logger log = LoggerFactory.getLogger(FaultyConverter.class);
        private int invocations = 0;

        public byte[] fromConnectData(String topic, Schema schema, Object value) {
            if (value == null) {
                return super.fromConnectData(topic, schema, null);
            }
            invocations++;
            if (invocations % 3 == 0) {
                log.debug("Succeeding record: {} where invocations={}", value, invocations);
                return super.fromConnectData(topic, schema, value);
            } else {
                log.debug("Failing record: {} at invocations={}", value, invocations);
                throw new RetriableException("Bad invocations " + invocations + " for mod 3");
            }
        }

        @Override
        public String version() {
            return "1.0";
        }
    }

    // Public to allow plugin discovery to complete without errors
    public static class FaultyPassthrough<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

        private static final Logger log = LoggerFactory.getLogger(FaultyPassthrough.class);

        private static final String MOD_CONFIG = "mod";
        private static final int MOD_CONFIG_DEFAULT = 3;

        public static final ConfigDef CONFIG_DEF = new ConfigDef()
                .define(MOD_CONFIG, ConfigDef.Type.INT, MOD_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, "Pass records without failure only if timestamp % mod == 0");

        private int mod = MOD_CONFIG_DEFAULT;

        private int invocations = 0;

        @Override
        public R apply(R record) {
            invocations++;
            if (invocations % mod == 0) {
                log.debug("Succeeding record: {} where invocations={}", record, invocations);
                return record;
            } else {
                log.debug("Failing record: {} at invocations={}", record, invocations);
                throw new RetriableException("Bad invocations " + invocations + " for mod " + mod);
            }
        }

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public ConfigDef config() {
            return CONFIG_DEF;
        }

        @Override
        public void close() {
            log.info("Shutting down transform");
        }

        @Override
        public void configure(Map<String, ?> configs) {
            final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
            mod = Math.max(config.getInt(MOD_CONFIG), 2);
            log.info("Configuring {}. Setting mod to {}", this.getClass(), mod);
        }
    }
}
