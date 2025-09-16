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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class InternalSinkRecordTest {

    private static final String TOPIC = "test-topic";

    @Test
    public void testNewRecordHeaders() {
        SinkRecord sinkRecord = new SinkRecord(TOPIC, 0, null, null, null, null, 10);
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("test-topic", 0, 10, null, null);
        ProcessingContext<ConsumerRecord<byte[], byte[]>> context = new ProcessingContext<>(consumerRecord);
        InternalSinkRecord internalSinkRecord = new InternalSinkRecord(context, sinkRecord);
        assertTrue(internalSinkRecord.headers().isEmpty());
        assertTrue(sinkRecord.headers().isEmpty());

        SinkRecord newRecord = internalSinkRecord.newRecord(TOPIC, 0, null, null, null,
                null, null, List.of(mock(Header.class)));
        assertEquals(1, newRecord.headers().size());
    }

    @Test
    public void shouldRetainOriginalTopicPartition() {
        String transformedTopic = "transformed-test-topic";
        SinkRecord sinkRecord = new SinkRecord(transformedTopic, 0, null, null, null, null, 10);
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(TOPIC, 0, 10, null, null);
        ProcessingContext<ConsumerRecord<byte[], byte[]>> context = new ProcessingContext<>(consumerRecord);
        InternalSinkRecord internalSinkRecord = new InternalSinkRecord(context, sinkRecord);

        assertEquals(TOPIC, internalSinkRecord.originalTopic());
        assertEquals(0, internalSinkRecord.originalKafkaPartition().intValue());

        SinkRecord transformedSinkRecord = internalSinkRecord.newRecord(transformedTopic, 1, null, null, null, null, null);
        assertEquals(TOPIC, transformedSinkRecord.originalTopic());
        assertEquals(0, transformedSinkRecord.originalKafkaPartition().intValue());
    }
}
