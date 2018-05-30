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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.LogReporter;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({WorkerSinkTask.class, WorkerSourceTask.class})
@PowerMockIgnore("javax.management.*")
public class ErrorHandlingTaskTest {

    private static final String TOPIC = "test";
    private static final int PARTITION1 = 12;
    private static final int PARTITION2 = 13;
    private static final long FIRST_OFFSET = 45;

    private static final Map<String, String> TASK_PROPS = new HashMap<>();

    static {
        TASK_PROPS.put(SinkConnector.TOPICS_CONFIG, TOPIC);
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, TestSinkTask.class.getName());
    }

    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    private static final Map<String, String> OPERATION_EXECUTOR_PROPS = new HashMap<>();

    static {
        OPERATION_EXECUTOR_PROPS.put(RetryWithToleranceOperator.TOLERANCE_LIMIT, "all");
        // wait up to 1 minute for an operation
        OPERATION_EXECUTOR_PROPS.put(RetryWithToleranceOperator.RETRY_TIMEOUT, "60000");
        // wait up 5 seconds between subsequent retries
        OPERATION_EXECUTOR_PROPS.put(RetryWithToleranceOperator.RETRY_DELAY_MAX_MS, "5000");
    }

    private ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private TargetState initialState = TargetState.STARTED;
    private Time time;
    private MockConnectMetrics metrics;
    @SuppressWarnings("unused")
    @Mock
    private SinkTask sinkTask;
    @SuppressWarnings("unused")
    @Mock
    private SourceTask sourceTask;
    private Capture<WorkerSinkTaskContext> sinkTaskContext = EasyMock.newCapture();
    private WorkerConfig workerConfig;
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

    @Mock
    OffsetStorageReader offsetReader;
    @Mock
    OffsetStorageWriter offsetWriter;

    private Capture<ConsumerRebalanceListener> rebalanceListener = EasyMock.newCapture();
    @SuppressWarnings("unused")
    @Mock
    private TaskStatus.Listener statusListener;

    private ErrorHandlingMetrics errorHandlingMetrics;

    @Before
    public void setup() {
        time = new MockTime(0, 0, 0);
        metrics = new MockConnectMetrics();
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put("internal.value.converter.schemas.enable", "false");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        pluginLoader = PowerMock.createMock(PluginClassLoader.class);
        workerConfig = new StandaloneConfig(workerProps);
        errorHandlingMetrics = new ErrorHandlingMetrics(taskId, metrics);
    }

    @After
    public void tearDown() {
        if (metrics != null) {
            metrics.stop();
        }
    }

    @Test
    public void testErrorHandlingInSinkTasks() throws Exception {
        LogReporter reporter = new LogReporter(taskId);
        Map<String, Object> reportProps = new HashMap<>();
        reportProps.put(LogReporter.LOG_ENABLE, "true");
        reportProps.put(LogReporter.LOG_INCLUDE_MESSAGES, "true");
        reporter.configure(reportProps);
        reporter.metrics(errorHandlingMetrics);

        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(time);
        retryWithToleranceOperator.configure(OPERATION_EXECUTOR_PROPS);
        retryWithToleranceOperator.metrics(errorHandlingMetrics);
        retryWithToleranceOperator.reporters(singletonList(reporter));
        createSinkTask(initialState, retryWithToleranceOperator);

        expectInitializeTask();

        // valid json
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>(TOPIC, PARTITION1, FIRST_OFFSET, null, "{\"a\": 10}".getBytes());
        // bad json
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>(TOPIC, PARTITION2, FIRST_OFFSET, null, "{\"a\" 10}".getBytes());

        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andReturn(records(record1));
        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andReturn(records(record2));

        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall().times(2);

        PowerMock.replayAll();

        workerSinkTask.initialize(TASK_CONFIG);
        workerSinkTask.initializeAndStart();
        workerSinkTask.iteration();

        workerSinkTask.iteration();

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

        PowerMock.verifyAll();
    }

    @Test
    public void testErrorHandlingInSourceTasks() throws Exception {
        LogReporter reporter = new LogReporter(taskId);
        Map<String, Object> reportProps = new HashMap<>();
        reportProps.put(LogReporter.LOG_ENABLE, "true");
        reportProps.put(LogReporter.LOG_INCLUDE_MESSAGES, "true");
        reporter.configure(reportProps);
        reporter.metrics(errorHandlingMetrics);

        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(time);
        retryWithToleranceOperator.configure(OPERATION_EXECUTOR_PROPS);
        retryWithToleranceOperator.metrics(errorHandlingMetrics);
        retryWithToleranceOperator.reporters(singletonList(reporter));
        createSourceTask(initialState, retryWithToleranceOperator);

        // valid json
        Schema valSchema = SchemaBuilder.struct().field("val", Schema.INT32_SCHEMA).build();
        Struct struct1 = new Struct(valSchema).put("val", 1234);
        SourceRecord record1 = new SourceRecord(emptyMap(), emptyMap(), TOPIC, PARTITION1, valSchema, struct1);
        Struct struct2 = new Struct(valSchema).put("val", 6789);
        SourceRecord record2 = new SourceRecord(emptyMap(), emptyMap(), TOPIC, PARTITION1, valSchema, struct2);

        EasyMock.expect(workerSourceTask.isStopping()).andReturn(false);
        EasyMock.expect(workerSourceTask.isStopping()).andReturn(false);
        EasyMock.expect(workerSourceTask.isStopping()).andReturn(true);

        EasyMock.expect(workerSourceTask.commitOffsets()).andReturn(true);

        offsetWriter.offset(EasyMock.anyObject(), EasyMock.anyObject());
        EasyMock.expectLastCall().times(2);
        sourceTask.initialize(EasyMock.anyObject());
        EasyMock.expectLastCall();

        sourceTask.start(EasyMock.anyObject());
        EasyMock.expectLastCall();

        EasyMock.expect(sourceTask.poll()).andReturn(singletonList(record1));
        EasyMock.expect(sourceTask.poll()).andReturn(singletonList(record2));
        EasyMock.expect(producer.send(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(null).times(2);

        PowerMock.replayAll();

        workerSourceTask.initialize(TASK_CONFIG);
        workerSourceTask.execute();

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

        PowerMock.verifyAll();
    }

    @Test
    public void testErrorHandlingInSourceTasksWthBadConverter() throws Exception {
        LogReporter reporter = new LogReporter(taskId);
        Map<String, Object> reportProps = new HashMap<>();
        reportProps.put(LogReporter.LOG_ENABLE, "true");
        reportProps.put(LogReporter.LOG_INCLUDE_MESSAGES, "true");
        reporter.configure(reportProps);
        reporter.metrics(errorHandlingMetrics);

        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(time);
        retryWithToleranceOperator.configure(OPERATION_EXECUTOR_PROPS);
        retryWithToleranceOperator.metrics(errorHandlingMetrics);
        retryWithToleranceOperator.reporters(singletonList(reporter));
        createSourceTask(initialState, retryWithToleranceOperator, badConverter());

        // valid json
        Schema valSchema = SchemaBuilder.struct().field("val", Schema.INT32_SCHEMA).build();
        Struct struct1 = new Struct(valSchema).put("val", 1234);
        SourceRecord record1 = new SourceRecord(emptyMap(), emptyMap(), TOPIC, PARTITION1, valSchema, struct1);
        Struct struct2 = new Struct(valSchema).put("val", 6789);
        SourceRecord record2 = new SourceRecord(emptyMap(), emptyMap(), TOPIC, PARTITION1, valSchema, struct2);

        EasyMock.expect(workerSourceTask.isStopping()).andReturn(false);
        EasyMock.expect(workerSourceTask.isStopping()).andReturn(false);
        EasyMock.expect(workerSourceTask.isStopping()).andReturn(true);

        EasyMock.expect(workerSourceTask.commitOffsets()).andReturn(true);

        offsetWriter.offset(EasyMock.anyObject(), EasyMock.anyObject());
        EasyMock.expectLastCall().times(2);
        sourceTask.initialize(EasyMock.anyObject());
        EasyMock.expectLastCall();

        sourceTask.start(EasyMock.anyObject());
        EasyMock.expectLastCall();

        EasyMock.expect(sourceTask.poll()).andReturn(singletonList(record1));
        EasyMock.expect(sourceTask.poll()).andReturn(singletonList(record2));
        EasyMock.expect(producer.send(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(null).times(2);

        PowerMock.replayAll();

        workerSourceTask.initialize(TASK_CONFIG);
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

        PowerMock.verifyAll();
    }

    private void assertSinkMetricValue(String name, double expected) {
        ConnectMetrics.MetricGroup sinkTaskGroup = workerSinkTask.sinkTaskMetricsGroup().metricGroup();
        double measured = metrics.currentMetricValueAsDouble(sinkTaskGroup, name);
        assertEquals(expected, measured, 0.001d);
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

    private void expectInitializeTask() throws Exception {
        PowerMock.expectPrivate(workerSinkTask, "createConsumer").andReturn(consumer);
        consumer.subscribe(EasyMock.eq(singletonList(TOPIC)), EasyMock.capture(rebalanceListener));
        PowerMock.expectLastCall();

        sinkTask.initialize(EasyMock.capture(sinkTaskContext));
        PowerMock.expectLastCall();
        sinkTask.start(TASK_PROPS);
        PowerMock.expectLastCall();
    }

    private void createSinkTask(TargetState initialState, RetryWithToleranceOperator retryWithToleranceOperator) {
        JsonConverter converter = new JsonConverter();
        Map<String, Object> oo = workerConfig.originalsWithPrefix("value.converter.");
        oo.put("converter.type", "value");
        oo.put("schemas.enable", "false");
        converter.configure(oo);

        TransformationChain<SinkRecord> sinkTransforms = new TransformationChain<>(singletonList(new FaultyPassthrough<SinkRecord>()), retryWithToleranceOperator);

        workerSinkTask = PowerMock.createPartialMock(
                WorkerSinkTask.class, new String[]{"createConsumer"},
                taskId, sinkTask, statusListener, initialState, workerConfig, metrics, converter, converter,
                headerConverter, sinkTransforms, pluginLoader, time, retryWithToleranceOperator);
    }

    private void createSourceTask(TargetState initialState, RetryWithToleranceOperator retryWithToleranceOperator) {
        JsonConverter converter = new JsonConverter();
        Map<String, Object> oo = workerConfig.originalsWithPrefix("value.converter.");
        oo.put("converter.type", "value");
        oo.put("schemas.enable", "false");
        converter.configure(oo);

        createSourceTask(initialState, retryWithToleranceOperator, converter);
    }

    private Converter badConverter() {
        FaultyConverter converter = new FaultyConverter();
        Map<String, Object> oo = workerConfig.originalsWithPrefix("value.converter.");
        oo.put("converter.type", "value");
        oo.put("schemas.enable", "false");
        converter.configure(oo);
        return converter;
    }

    private void createSourceTask(TargetState initialState, RetryWithToleranceOperator retryWithToleranceOperator, Converter converter) {
        TransformationChain<SourceRecord> sourceTransforms = new TransformationChain<>(singletonList(new FaultyPassthrough<SourceRecord>()), retryWithToleranceOperator);

        workerSourceTask = PowerMock.createPartialMock(
                WorkerSourceTask.class, new String[]{"commitOffsets", "isStopping"},
                taskId, sourceTask, statusListener, initialState, converter, converter, headerConverter, sourceTransforms,
                producer, offsetReader, offsetWriter, workerConfig, metrics, pluginLoader, time, retryWithToleranceOperator);
    }

    private ConsumerRecords<byte[], byte[]> records(ConsumerRecord<byte[], byte[]> record) {
        return new ConsumerRecords<>(Collections.singletonMap(
                new TopicPartition(record.topic(), record.partition()), singletonList(record)));
    }

    private abstract static class TestSinkTask extends SinkTask {
    }

    static class FaultyConverter extends JsonConverter {
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
    }

    static class FaultyPassthrough<R extends ConnectRecord<R>> implements Transformation<R> {

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
