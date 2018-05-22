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
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata;
import org.apache.kafka.connect.transforms.Transformation;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import static org.easymock.EasyMock.replay;

@RunWith(PowerMockRunner.class)
public class ErrorReporterTest {

    private static final int PARTITIONS = 10;
    private static final String TOPIC = "test-topic";
    private static final String DLQ_TOPIC = "test-topic-errors";

    private final HashMap<String, Object> config = new HashMap<>();

    @Mock
    KafkaProducer<byte[], byte[]> producer;

    @Mock
    Future<RecordMetadata> metadata;

    @Test
    public void testDLQConfigWithEmptyTopicName() {
        DLQReporter dlqReporter = new DLQReporter(producer, PARTITIONS);
        dlqReporter.configure(config);
        ProcessingContext context = processingContext();

        EasyMock.expect(producer.send(EasyMock.anyObject(), EasyMock.anyObject())).andThrow(new RuntimeException());
        replay(producer);

        // since topic name is empty, this method should be a NOOP.
        // if it attempts to log to the DLQ via the producer, the send mock will throw a RuntimeException.
        dlqReporter.report(context);
    }

    @Test
    public void testDLQConfigWithValidTopicName() {
        DLQReporter dlqReporter = new DLQReporter(producer, PARTITIONS);
        dlqReporter.configure(config(DLQReporter.DLQ_TOPIC_NAME, DLQ_TOPIC));
        ProcessingContext context = processingContext();

        EasyMock.expect(producer.send(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(metadata);
        replay(producer);

        dlqReporter.report(context);

        PowerMock.verifyAll();
    }

    @Test
    public void testReportTwice() {
        DLQReporter dlqReporter = new DLQReporter(producer, PARTITIONS);
        dlqReporter.configure(config(DLQReporter.DLQ_TOPIC_NAME, DLQ_TOPIC));
        ProcessingContext context = processingContext();

        EasyMock.expect(producer.send(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(metadata).times(2);
        replay(producer);

        dlqReporter.report(context);
        dlqReporter.report(context);

        PowerMock.verifyAll();
    }

    private ProcessingContext processingContext() {
        ProcessingContext context = new ProcessingContext();
        context.sinkRecord(new ConsumerRecord<>(TOPIC, 5, 100, new byte[]{}, new byte[]{}));
        context.setStage(Stage.TRANSFORMATION, Transformation.class);
        return context;
    }

    private Map<String, Object> config(String key, Object val) {
        config.put(key, val);
        return config;
    }

}
