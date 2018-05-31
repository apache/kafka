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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectMetrics;
import org.apache.kafka.connect.runtime.MockConnectMetrics;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class ErrorReporterTest {

    private static final String TOPIC = "test-topic";
    private static final String DLQ_TOPIC = "test-topic-errors";
    private static final ConnectorTaskId TASK_ID = new ConnectorTaskId("job", 0);

    @Mock
    KafkaProducer<byte[], byte[]> producer;

    @Mock
    Future<RecordMetadata> metadata;

    private HashMap<String, Object> config;
    private ErrorHandlingMetrics errorHandlingMetrics;
    private MockConnectMetrics metrics;

    @Before
    public void setup() {
        config = new HashMap<>();
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
    public void testDLQConfigWithEmptyTopicName() {
        DeadLetterQueueReporter deadLetterQueueReporter = new DeadLetterQueueReporter(producer);
        deadLetterQueueReporter.configure(config);
        deadLetterQueueReporter.metrics(errorHandlingMetrics);

        ProcessingContext context = processingContext();

        EasyMock.expect(producer.send(EasyMock.anyObject(), EasyMock.anyObject())).andThrow(new RuntimeException());
        replay(producer);

        // since topic name is empty, this method should be a NOOP.
        // if it attempts to log to the DLQ via the producer, the send mock will throw a RuntimeException.
        deadLetterQueueReporter.report(context);
    }

    @Test
    public void testDLQConfigWithValidTopicName() {
        DeadLetterQueueReporter deadLetterQueueReporter = new DeadLetterQueueReporter(producer);
        deadLetterQueueReporter.configure(config(DeadLetterQueueReporter.DLQ_TOPIC_NAME, DLQ_TOPIC));
        deadLetterQueueReporter.metrics(errorHandlingMetrics);

        ProcessingContext context = processingContext();

        EasyMock.expect(producer.send(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(metadata);
        replay(producer);

        deadLetterQueueReporter.report(context);

        PowerMock.verifyAll();
    }

    @Test
    public void testReportDLQTwice() {
        DeadLetterQueueReporter deadLetterQueueReporter = new DeadLetterQueueReporter(producer);
        deadLetterQueueReporter.configure(config(DeadLetterQueueReporter.DLQ_TOPIC_NAME, DLQ_TOPIC));
        deadLetterQueueReporter.metrics(errorHandlingMetrics);

        ProcessingContext context = processingContext();

        EasyMock.expect(producer.send(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(metadata).times(2);
        replay(producer);

        deadLetterQueueReporter.report(context);
        deadLetterQueueReporter.report(context);

        PowerMock.verifyAll();
    }

    @Test
    public void testLogOnDisabledLogReporter() {
        LogReporter logReporter = new LogReporter(TASK_ID);
        logReporter.configure(config);
        logReporter.metrics(errorHandlingMetrics);

        ProcessingContext context = processingContext();
        context.error(new RuntimeException());

        // reporting a context without an error should not cause any errors.
        logReporter.report(context);
        assertErrorHandlingMetricValue("total-errors-logged", 0.0);
    }

    @Test
    public void testLogOnEnabledLogReporter() {
        LogReporter logReporter = new LogReporter(TASK_ID);
        logReporter.configure(config(LogReporter.LOG_ENABLE, "true"));
        logReporter.metrics(errorHandlingMetrics);

        ProcessingContext context = processingContext();
        context.error(new RuntimeException());

        // reporting a context without an error should not cause any errors.
        logReporter.report(context);
        assertErrorHandlingMetricValue("total-errors-logged", 1.0);
    }

    @Test
    public void testLogMessageWithNoRecords() {
        LogReporter logReporter = new LogReporter(TASK_ID);
        logReporter.configure(config(LogReporter.LOG_ENABLE, "true"));
        logReporter.metrics(errorHandlingMetrics);

        ProcessingContext context = processingContext();

        String msg = logReporter.message(context);
        assertEquals("Error encountered in task job-0. Executing stage 'KEY_CONVERTER' with class " +
                "'org.apache.kafka.connect.json.JsonConverter'.", msg);
    }

    @Test
    public void testLogMessageWithSinkRecords() {
        LogReporter logReporter = new LogReporter(TASK_ID);
        logReporter.configure(config(LogReporter.LOG_ENABLE, "true"));
        logReporter.configure(config(LogReporter.LOG_INCLUDE_MESSAGES, "true"));
        logReporter.metrics(errorHandlingMetrics);

        ProcessingContext context = processingContext();

        String msg = logReporter.message(context);
        assertEquals("Error encountered in task job-0. Executing stage 'KEY_CONVERTER' with class " +
                "'org.apache.kafka.connect.json.JsonConverter', where consumed record is {topic='test-topic', " +
                "partition=5, offset=100}.", msg);
    }

    private ProcessingContext processingContext() {
        ProcessingContext context = new ProcessingContext();
        context.consumerRecord(new ConsumerRecord<>(TOPIC, 5, 100, new byte[]{'a', 'b'}, new byte[]{'x'}));
        context.currentContext(Stage.KEY_CONVERTER, JsonConverter.class);
        return context;
    }

    private Map<String, Object> config(String key, Object val) {
        config.put(key, val);
        return config;
    }

    private void assertErrorHandlingMetricValue(String name, double expected) {
        ConnectMetrics.MetricGroup sinkTaskGroup = errorHandlingMetrics.metricGroup();
        double measured = metrics.currentMetricValueAsDouble(sinkTaskGroup, name);
        assertEquals(expected, measured, 0.001d);
    }

}
