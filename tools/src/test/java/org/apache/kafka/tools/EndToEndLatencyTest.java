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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.utils.Exit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class EndToEndLatencyTest {

    private static final byte[] RECORD_VALUE = "record-sent".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RECORD_VALUE_DIFFERENT = "record-received".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RECORD_KEY = "key-sent".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RECORD_KEY_DIFFERENT = "key-received".getBytes(StandardCharsets.UTF_8);
    private static final String HEADER_KEY = "header-key-sent";
    private static final String HEADER_KEY_DIFFERENT = "header-key-received";
    private static final byte[] HEADER_VALUE = "header-value-sent".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HEADER_VALUE_DIFFERENT = "header-value-received".getBytes(StandardCharsets.UTF_8);

    // legacy format test arguments
    private static final String[] LEGACY_INVALID_ARGS_UNEXPECTED = {
        "localhost:9092", "test", "10000", "1", "200", "propsfile.properties", "random"
    };

    private static class ArgsBuilder {
        private final Map<String, String> params = new LinkedHashMap<>();
        
        private ArgsBuilder() {
            params.put("--bootstrap-server", "localhost:9092");
            params.put("--topic", "test-topic");
            params.put("--num-records", "100");
            params.put("--producer-acks", "1");
            params.put("--record-size", "200");
        }
        
        public static ArgsBuilder defaults() {
            return new ArgsBuilder();
        }
        
        public ArgsBuilder with(String param, String value) {
            params.put(param, value);
            return this;
        }
        
        public String[] build() {
            return params.entrySet().stream()
                    .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
                    .toArray(String[]::new);
        }

        public ArgsBuilder withNegative(String param) {
            return with(param, "-1");
        }
        
        public ArgsBuilder withZero(String param) {
            return with(param, "0");
        }
    }

    @Mock
    KafkaConsumer<byte[], byte[]> consumer;

    @Mock
    ConsumerRecords<byte[], byte[]> records;

    @Test
    public void testInvalidArgs() {
        testUnexpectedArgsWithLegacyFormat();
        testInvalidProducerAcks();
        testInvalidNumRecords();
        testInvalidRecordSize();
        testInvalidRecordKey();
        testInvalidNumHeaders();
        testInvalidRecordHeaderKey();
        testInvalidRecordHeaderValue();
    }

    private void testUnexpectedArgsWithLegacyFormat() {
        String expectedMsg = "Invalid number of arguments. Expected 5 or 6 positional arguments, but got 7.";
        TerseException terseException = assertThrows(TerseException.class, () -> EndToEndLatency.execute(LEGACY_INVALID_ARGS_UNEXPECTED));
        assertTrue(terseException.getMessage().contains(expectedMsg));
    }

    private void testInvalidNumRecords() {
        String expectedMsg = "Value for --num-records must be a positive integer.";
        assertInitializeInvalidOptionsExitCodeAndMsg(
            ArgsBuilder.defaults().withNegative("--num-records").build(), expectedMsg);
    }

    private void testInvalidRecordSize() {
        String expectedMsg = "Value for --record-size must be a non-negative integer.";
        assertInitializeInvalidOptionsExitCodeAndMsg(
            ArgsBuilder.defaults().withNegative("--record-size").build(), expectedMsg);
    }

    private void testInvalidRecordKey() {
        String expectedMsg = "Value for --record-key-size must be a non-negative integer.";
        assertInitializeInvalidOptionsExitCodeAndMsg(
            ArgsBuilder.defaults().withNegative("--record-key-size").build(), expectedMsg);
    }

    private void testInvalidNumHeaders() {
        String expectedMsg = "Value for --num-headers must be a non-negative integer.";
        assertInitializeInvalidOptionsExitCodeAndMsg(
                ArgsBuilder.defaults().withNegative("--num-headers").build(), expectedMsg);
    }

    private void testInvalidRecordHeaderKey() {
        String expectedMsg = "Value for --record-header-key-size must be a non-negative integer.";
        assertInitializeInvalidOptionsExitCodeAndMsg(
            ArgsBuilder.defaults().withNegative("--record-header-key-size").build(), expectedMsg);
    }

    private void testInvalidRecordHeaderValue() {
        String expectedMsg = "Value for --record-header-size must be a non-negative integer.";
        assertInitializeInvalidOptionsExitCodeAndMsg(
            ArgsBuilder.defaults().withNegative("--record-header-size").build(), expectedMsg);
    }

    private void testInvalidProducerAcks() {
        String expectedMsg = "Invalid value for --producer-acks. Latency testing requires synchronous acknowledgement. Please use '1' or 'all'.";
        assertInitializeInvalidOptionsExitCodeAndMsg(
                ArgsBuilder.defaults().withZero("--producer-acks").build(), expectedMsg);
    }

    private void assertInitializeInvalidOptionsExitCodeAndMsg(String[] args, String expectedMsg) {
        Exit.setExitProcedure((exitCode, message) -> {
            assertEquals(1, exitCode);
            assertTrue(message.contains(expectedMsg));
            throw new RuntimeException();
        });
        try {
            assertThrows(RuntimeException.class, () -> EndToEndLatency.execute(args));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    @SuppressWarnings("removal")
    public void testConvertLegacyArgs() throws Exception {
        String[] legacyArgs = {"localhost:9092", "test", "100", "1", "200"};
        String[] convertedArgs = EndToEndLatency.convertLegacyArgsIfNeeded(legacyArgs);
        String[] expectedArgs = {
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--num-records", "100",
            "--producer-acks", "1",
            "--record-size", "200"
        };
        assertArrayEquals(expectedArgs, convertedArgs);
    }

    @Test
    public void shouldFailWhenConsumerRecordsIsEmpty() {
        when(records.isEmpty()).thenReturn(true);
        assertThrows(RuntimeException.class, () -> EndToEndLatency.validate(consumer, new byte[0], records, null, null));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFailWhenSentRecordIsNotEqualToReceived() {
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = mock(Iterator.class);
        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        when(records.isEmpty()).thenReturn(false);
        when(records.iterator()).thenReturn(iterator);
        when(iterator.next()).thenReturn(record);
        when(record.value()).thenReturn(RECORD_VALUE_DIFFERENT);
        assertThrows(RuntimeException.class, () -> EndToEndLatency.validate(consumer, RECORD_VALUE, records, null, null));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFailWhenSentRecordKeyIsNotEqualToReceived() {
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = mock(Iterator.class);
        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        when(records.isEmpty()).thenReturn(false);
        when(records.iterator()).thenReturn(iterator);
        when(iterator.next()).thenReturn(record);
        when(record.value()).thenReturn(RECORD_VALUE);
        when(record.key()).thenReturn(RECORD_KEY_DIFFERENT);

        assertThrows(RuntimeException.class, () ->
                EndToEndLatency.validate(consumer, RECORD_VALUE, records,
                        RECORD_KEY, null));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFailWhenSentHeaderKeyIsNotEqualToReceived() {
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = mock(Iterator.class);
        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        Headers headers = mock(Headers.class);
        Iterator<Header> headerIterator = mock(Iterator.class);
        Header receivedHeader = new RecordHeader(HEADER_KEY_DIFFERENT, HEADER_VALUE);

        when(records.isEmpty()).thenReturn(false);
        when(records.iterator()).thenReturn(iterator);
        when(iterator.next()).thenReturn(record);
        when(record.value()).thenReturn(RECORD_VALUE);
        when(record.key()).thenReturn(null);
        when(record.headers()).thenReturn(headers);
        when(headers.iterator()).thenReturn(headerIterator);
        when(headerIterator.hasNext()).thenReturn(true);
        when(headerIterator.next()).thenReturn(receivedHeader);

        Header sentHeader = new RecordHeader(HEADER_KEY, HEADER_VALUE);
        List<Header> sentHeaders = List.of(sentHeader);

        assertThrows(RuntimeException.class, () ->
                EndToEndLatency.validate(consumer, RECORD_VALUE, records, null, sentHeaders));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFailWhenSentHeaderValueIsNotEqualToReceived() {
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = mock(Iterator.class);
        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        Headers headers = mock(Headers.class);
        Iterator<Header> headerIterator = mock(Iterator.class);
        Header receivedHeader = new RecordHeader(HEADER_KEY, HEADER_VALUE_DIFFERENT);
        Header sentHeader = new RecordHeader(HEADER_KEY, HEADER_VALUE);
        List<Header> sentHeaders = List.of(sentHeader);

        when(records.isEmpty()).thenReturn(false);
        when(records.iterator()).thenReturn(iterator);
        when(iterator.next()).thenReturn(record);
        when(record.value()).thenReturn(RECORD_VALUE);
        when(record.key()).thenReturn(null);
        when(record.headers()).thenReturn(headers);
        when(headers.iterator()).thenReturn(headerIterator);
        when(headerIterator.hasNext()).thenReturn(true);
        when(headerIterator.next()).thenReturn(receivedHeader);

        assertThrows(RuntimeException.class, () ->
                EndToEndLatency.validate(consumer, RECORD_VALUE, records, null, sentHeaders));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFailWhenReceivedMoreThanOneRecord() {
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = mock(Iterator.class);
        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        when(records.isEmpty()).thenReturn(false);
        when(records.iterator()).thenReturn(iterator);
        when(iterator.next()).thenReturn(record);
        when(record.value()).thenReturn(RECORD_VALUE);
        when(records.count()).thenReturn(2);
        assertThrows(RuntimeException.class, () -> EndToEndLatency.validate(consumer, RECORD_VALUE, records, null, null));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldPassInValidation() {
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = mock(Iterator.class);
        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        Headers headers = mock(Headers.class);
        Iterator<Header> headerIterator = mock(Iterator.class);
        Header receivedHeader = new RecordHeader(HEADER_KEY, HEADER_VALUE);
        Header sentHeader = new RecordHeader(HEADER_KEY, HEADER_VALUE);
        List<Header> sentHeaders = List.of(sentHeader);

        when(records.isEmpty()).thenReturn(false);
        when(records.iterator()).thenReturn(iterator);
        when(iterator.next()).thenReturn(record);
        when(record.value()).thenReturn(RECORD_VALUE);
        byte[] recordKey = RECORD_KEY;
        when(record.key()).thenReturn(recordKey);
        when(records.count()).thenReturn(1);
        when(record.headers()).thenReturn(headers);
        when(headers.iterator()).thenReturn(headerIterator);
        when(headerIterator.hasNext()).thenReturn(true, true, false);
        when(headerIterator.next()).thenReturn(receivedHeader);

        assertDoesNotThrow(() -> EndToEndLatency.validate(consumer, RECORD_VALUE, records, recordKey, sentHeaders));
    }

    @Test
    public void shouldPassWithNamedArgs() {
        AtomicReference<Integer> exitStatus = new AtomicReference<>();
        Exit.setExitProcedure((status, __) -> {
            exitStatus.set(status);
            throw new RuntimeException();
        });
        try {
            assertDoesNotThrow(() -> new EndToEndLatency.EndToEndLatencyCommandOptions(ArgsBuilder.defaults().build()));
            assertNull(exitStatus.get());
        } finally {
            Exit.resetExitProcedure();
        }
    }

}
