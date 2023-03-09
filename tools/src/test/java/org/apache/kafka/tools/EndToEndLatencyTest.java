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
package org.apache.kafka.tools;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class EndToEndLatencyTest {

    @Mock
    KafkaConsumer<byte[], byte[]> consumer;

    @Mock
    ConsumerRecords<byte[], byte[]> records;

    @Test
    public void shouldFailWhenSuppliedUnexpectedArgs() {
        String[] args = new String[] {"localhost:9092", "test", "10000", "1", "200", "propsfile.properties", "random"};
        assertThrows(TerseException.class, () -> EndToEndLatency.execute(args));
    }

    @Test
    public void shouldFailWhenProducerAcksAreNotSynchronised() {
        String[] args = new String[] {"localhost:9092", "test", "10000", "0", "200"};
        assertThrows(IllegalArgumentException.class, () -> EndToEndLatency.execute(args));
    }

    @Test
    public void shouldFailWhenConsumerRecordsIsEmpty() {
        when(records.isEmpty()).thenReturn(true);
        assertThrows(RuntimeException.class, () -> EndToEndLatency.validate(consumer, new byte[0], records));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFailWhenSentIsNotEqualToReceived() {
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = mock(Iterator.class);
        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        when(records.isEmpty()).thenReturn(false);
        when(records.iterator()).thenReturn(iterator);
        when(iterator.next()).thenReturn(record);
        when(record.value()).thenReturn("kafkab".getBytes(StandardCharsets.UTF_8));
        assertThrows(RuntimeException.class, () -> EndToEndLatency.validate(consumer, "kafkaa".getBytes(StandardCharsets.UTF_8), records));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFailWhenReceivedMoreThanOneRecord() {
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = mock(Iterator.class);
        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        when(records.isEmpty()).thenReturn(false);
        when(records.iterator()).thenReturn(iterator);
        when(iterator.next()).thenReturn(record);
        when(record.value()).thenReturn("kafkaa".getBytes(StandardCharsets.UTF_8));
        when(records.count()).thenReturn(2);
        assertThrows(RuntimeException.class, () -> EndToEndLatency.validate(consumer, "kafkaa".getBytes(StandardCharsets.UTF_8), records));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldPassInValidation() {
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = mock(Iterator.class);
        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        when(records.isEmpty()).thenReturn(false);
        when(records.iterator()).thenReturn(iterator);
        when(iterator.next()).thenReturn(record);
        when(record.value()).thenReturn("kafkaa".getBytes(StandardCharsets.UTF_8));
        when(records.count()).thenReturn(1);
        assertDoesNotThrow(() -> EndToEndLatency.validate(consumer, "kafkaa".getBytes(StandardCharsets.UTF_8), records));
    }

}
