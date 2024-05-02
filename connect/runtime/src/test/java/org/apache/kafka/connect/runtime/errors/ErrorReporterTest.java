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
package org.apache.kafka.connect.runtime.errors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectMetrics;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.MockConnectMetrics;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_CONNECTOR_NAME;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_EXCEPTION;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_EXCEPTION_MESSAGE;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_EXCEPTION_STACK_TRACE;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_EXECUTING_CLASS;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_ORIG_OFFSET;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_ORIG_PARTITION;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_ORIG_TOPIC;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_STAGE;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_TASK_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class ErrorReporterTest {

    private static final String TOPIC = "test-topic";
    private static final String DLQ_TOPIC = "test-topic-errors";
    private static final ConnectorTaskId TASK_ID = new ConnectorTaskId("job", 0);

    @Mock
    KafkaProducer<byte[], byte[]> producer;

    @Mock
    Future<RecordMetadata> metadata;

    @Mock
    Plugins plugins;

    private ErrorHandlingMetrics errorHandlingMetrics;
    private MockConnectMetrics metrics;

    @Before
    public void setup() {
        metrics = new MockConnectMetrics();
        errorHandlingMetrics = new ErrorHandlingMetrics(new ConnectorTaskId("connector-", 1), metrics);
    }

    @After
    public void tearDown() {
        if (metrics != null) {
            metrics.stop();
        }
    }

    @Test
    public void initializeDLQWithNullMetrics() {
        assertThrows(NullPointerException.class, () -> new DeadLetterQueueReporter(producer, config(emptyMap()), TASK_ID, null));
    }

    @Test
    public void testDLQConfigWithEmptyTopicName() {
        DeadLetterQueueReporter deadLetterQueueReporter = new DeadLetterQueueReporter(
                producer, config(emptyMap()), TASK_ID, errorHandlingMetrics);

        ProcessingContext<ConsumerRecord<byte[], byte[]>> context = processingContext();

        // since topic name is empty, this method should be a NOOP and producer.send() should
        // not be called.
        deadLetterQueueReporter.report(context);
        verifyNoMoreInteractions(producer);
    }

    @Test
    public void testDLQConfigWithValidTopicName() {
        DeadLetterQueueReporter deadLetterQueueReporter = new DeadLetterQueueReporter(
                producer, config(singletonMap(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC)), TASK_ID, errorHandlingMetrics);

        ProcessingContext<ConsumerRecord<byte[], byte[]>> context = processingContext();

        when(producer.send(any(), any())).thenReturn(metadata);

        deadLetterQueueReporter.report(context);

        verify(producer, times(1)).send(any(), any());
    }

    @Test
    public void testReportDLQTwice() {
        DeadLetterQueueReporter deadLetterQueueReporter = new DeadLetterQueueReporter(
                producer, config(singletonMap(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC)), TASK_ID, errorHandlingMetrics);

        ProcessingContext<ConsumerRecord<byte[], byte[]>> context = processingContext();

        when(producer.send(any(), any())).thenReturn(metadata);

        deadLetterQueueReporter.report(context);
        deadLetterQueueReporter.report(context);

        verify(producer, times(2)).send(any(), any());
    }

    @Test
    public void testCloseDLQ() {
        DeadLetterQueueReporter deadLetterQueueReporter = new DeadLetterQueueReporter(
            producer, config(singletonMap(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC)), TASK_ID, errorHandlingMetrics);

        deadLetterQueueReporter.close();
        verify(producer).close();
    }

    @Test
    public void testLogOnDisabledLogReporter() {
        LogReporter<ConsumerRecord<byte[], byte[]>> logReporter = new LogReporter.Sink(TASK_ID, config(emptyMap()), errorHandlingMetrics);

        ProcessingContext<ConsumerRecord<byte[], byte[]>> context = processingContext();
        context.error(new RuntimeException());

        // reporting a context without an error should not cause any errors.
        logReporter.report(context);
        assertErrorHandlingMetricValue("total-errors-logged", 0.0);
    }

    @Test
    public void testLogOnEnabledLogReporter() {
        LogReporter<ConsumerRecord<byte[], byte[]>> logReporter = new LogReporter.Sink(TASK_ID, config(singletonMap(ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, "true")), errorHandlingMetrics);

        ProcessingContext<ConsumerRecord<byte[], byte[]>> context = processingContext();
        context.error(new RuntimeException());

        // reporting a context without an error should not cause any errors.
        logReporter.report(context);
        assertErrorHandlingMetricValue("total-errors-logged", 1.0);
    }

    @Test
    public void testLogMessageWithNoRecords() {
        LogReporter<ConsumerRecord<byte[], byte[]>> logReporter = new LogReporter.Sink(TASK_ID, config(singletonMap(ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, "true")), errorHandlingMetrics);

        ProcessingContext<ConsumerRecord<byte[], byte[]>> context = processingContext();

        String msg = logReporter.message(context);
        assertEquals("Error encountered in task job-0. Executing stage 'KEY_CONVERTER' with class " +
                "'org.apache.kafka.connect.json.JsonConverter'.", msg);
    }

    @Test
    public void testLogMessageWithSinkRecords() {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, "true");
        props.put(ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG, "true");

        LogReporter<ConsumerRecord<byte[], byte[]>> logReporter = new LogReporter.Sink(TASK_ID, config(props), errorHandlingMetrics);

        ProcessingContext<ConsumerRecord<byte[], byte[]>> context = processingContext();

        String msg = logReporter.message(context);
        assertEquals("Error encountered in task job-0. Executing stage 'KEY_CONVERTER' with class " +
                "'org.apache.kafka.connect.json.JsonConverter', where consumed record is {topic='test-topic', " +
                "partition=5, offset=100}.", msg);
    }

    @Test
    public void testLogReportAndReturnFuture() {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, "true");
        props.put(ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG, "true");

        LogReporter<ConsumerRecord<byte[], byte[]>> logReporter = new LogReporter.Sink(TASK_ID, config(props), errorHandlingMetrics);

        ProcessingContext<ConsumerRecord<byte[], byte[]>> context = processingContext();

        String msg = logReporter.message(context);
        assertEquals("Error encountered in task job-0. Executing stage 'KEY_CONVERTER' with class " +
            "'org.apache.kafka.connect.json.JsonConverter', where consumed record is {topic='test-topic', " +
            "partition=5, offset=100}.", msg);

        Future<RecordMetadata> future = logReporter.report(context);
        assertInstanceOf(CompletableFuture.class, future);
    }

    @Test
    public void testSetDLQConfigs() {
        SinkConnectorConfig configuration = config(singletonMap(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC));
        assertEquals(configuration.dlqTopicName(), DLQ_TOPIC);

        configuration = config(singletonMap(SinkConnectorConfig.DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "7"));
        assertEquals(configuration.dlqTopicReplicationFactor(), 7);
    }

    @Test
    public void testDlqHeaderConsumerRecord() {
        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC);
        props.put(SinkConnectorConfig.DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, "true");
        DeadLetterQueueReporter deadLetterQueueReporter = new DeadLetterQueueReporter(producer, config(props), TASK_ID, errorHandlingMetrics);

        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("source-topic", 7, 10, "source-key".getBytes(), "source-value".getBytes());
        ProcessingContext<ConsumerRecord<byte[], byte[]>> context = new ProcessingContext<>(consumerRecord);
        context.currentContext(Stage.TRANSFORMATION, Transformation.class);
        context.error(new ConnectException("Test Exception"));

        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(DLQ_TOPIC, "source-key".getBytes(), "source-value".getBytes());

        deadLetterQueueReporter.populateContextHeaders(producerRecord, context);
        assertEquals("source-topic", headerValue(producerRecord, ERROR_HEADER_ORIG_TOPIC));
        assertEquals("7", headerValue(producerRecord, ERROR_HEADER_ORIG_PARTITION));
        assertEquals("10", headerValue(producerRecord, ERROR_HEADER_ORIG_OFFSET));
        assertEquals(TASK_ID.connector(), headerValue(producerRecord, ERROR_HEADER_CONNECTOR_NAME));
        assertEquals(String.valueOf(TASK_ID.task()), headerValue(producerRecord, ERROR_HEADER_TASK_ID));
        assertEquals(Stage.TRANSFORMATION.name(), headerValue(producerRecord, ERROR_HEADER_STAGE));
        assertEquals(Transformation.class.getName(), headerValue(producerRecord, ERROR_HEADER_EXECUTING_CLASS));
        assertEquals(ConnectException.class.getName(), headerValue(producerRecord, ERROR_HEADER_EXCEPTION));
        assertEquals("Test Exception", headerValue(producerRecord, ERROR_HEADER_EXCEPTION_MESSAGE));
        assertFalse(headerValue(producerRecord, ERROR_HEADER_EXCEPTION_STACK_TRACE).isEmpty());
        assertTrue(headerValue(producerRecord, ERROR_HEADER_EXCEPTION_STACK_TRACE).startsWith("org.apache.kafka.connect.errors.ConnectException: Test Exception"));
    }

    @Test
    public void testDlqHeaderOnNullExceptionMessage() {
        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC);
        props.put(SinkConnectorConfig.DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, "true");
        DeadLetterQueueReporter deadLetterQueueReporter = new DeadLetterQueueReporter(producer, config(props), TASK_ID, errorHandlingMetrics);

        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("source-topic", 7, 10, "source-key".getBytes(), "source-value".getBytes());
        ProcessingContext<ConsumerRecord<byte[], byte[]>> context = new ProcessingContext<>(consumerRecord);
        context.currentContext(Stage.TRANSFORMATION, Transformation.class);
        context.error(new NullPointerException());

        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(DLQ_TOPIC, "source-key".getBytes(), "source-value".getBytes());

        deadLetterQueueReporter.populateContextHeaders(producerRecord, context);
        assertEquals("source-topic", headerValue(producerRecord, ERROR_HEADER_ORIG_TOPIC));
        assertEquals("7", headerValue(producerRecord, ERROR_HEADER_ORIG_PARTITION));
        assertEquals("10", headerValue(producerRecord, ERROR_HEADER_ORIG_OFFSET));
        assertEquals(TASK_ID.connector(), headerValue(producerRecord, ERROR_HEADER_CONNECTOR_NAME));
        assertEquals(String.valueOf(TASK_ID.task()), headerValue(producerRecord, ERROR_HEADER_TASK_ID));
        assertEquals(Stage.TRANSFORMATION.name(), headerValue(producerRecord, ERROR_HEADER_STAGE));
        assertEquals(Transformation.class.getName(), headerValue(producerRecord, ERROR_HEADER_EXECUTING_CLASS));
        assertEquals(NullPointerException.class.getName(), headerValue(producerRecord, ERROR_HEADER_EXCEPTION));
        assertNull(producerRecord.headers().lastHeader(ERROR_HEADER_EXCEPTION_MESSAGE).value());
        assertFalse(headerValue(producerRecord, ERROR_HEADER_EXCEPTION_STACK_TRACE).isEmpty());
        assertTrue(headerValue(producerRecord, ERROR_HEADER_EXCEPTION_STACK_TRACE).startsWith("java.lang.NullPointerException"));
    }

    @Test
    public void testDlqHeaderIsAppended() {
        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC);
        props.put(SinkConnectorConfig.DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, "true");
        DeadLetterQueueReporter deadLetterQueueReporter = new DeadLetterQueueReporter(producer, config(props), TASK_ID, errorHandlingMetrics);

        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("source-topic", 7, 10, "source-key".getBytes(), "source-value".getBytes());
        ProcessingContext<ConsumerRecord<byte[], byte[]>> context = new ProcessingContext<>(consumerRecord);
        context.currentContext(Stage.TRANSFORMATION, Transformation.class);
        context.error(new ConnectException("Test Exception"));

        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(DLQ_TOPIC, "source-key".getBytes(), "source-value".getBytes());
        producerRecord.headers().add(ERROR_HEADER_ORIG_TOPIC, "dummy".getBytes());

        deadLetterQueueReporter.populateContextHeaders(producerRecord, context);
        int appearances = 0;
        for (Header header: producerRecord.headers()) {
            if (ERROR_HEADER_ORIG_TOPIC.equalsIgnoreCase(header.key())) {
                appearances++;
            }
        }

        assertEquals("source-topic", headerValue(producerRecord, ERROR_HEADER_ORIG_TOPIC));
        assertEquals(2, appearances);
    }

    private String headerValue(ProducerRecord<byte[], byte[]> producerRecord, String headerSuffix) {
        return new String(producerRecord.headers().lastHeader(headerSuffix).value());
    }

    private ProcessingContext<ConsumerRecord<byte[], byte[]>> processingContext() {
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(TOPIC, 5, 100, new byte[]{'a', 'b'}, new byte[]{'x'});
        ProcessingContext<ConsumerRecord<byte[], byte[]>> context = new ProcessingContext<>(consumerRecord);
        context.currentContext(Stage.KEY_CONVERTER, JsonConverter.class);
        return context;
    }

    private SinkConnectorConfig config(Map<String, String> configProps) {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.NAME_CONFIG, "test");
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SinkTask.class.getName());
        props.putAll(configProps);
        return new SinkConnectorConfig(plugins, props);
    }

    private void assertErrorHandlingMetricValue(String name, double expected) {
        ConnectMetrics.MetricGroup sinkTaskGroup = errorHandlingMetrics.metricGroup();
        double measured = metrics.currentMetricValueAsDouble(sinkTaskGroup, name);
        assertEquals(expected, measured, 0.001d);
    }

}
