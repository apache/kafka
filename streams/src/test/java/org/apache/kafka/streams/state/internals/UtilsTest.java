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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.apache.kafka.streams.state.internals.Utils.rawTimestampedValue;
import static org.apache.kafka.streams.state.internals.Utils.readBytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UtilsTest {
    private static final String TOPIC = "test-topic";
    private static final String VALUE = "test-value";

    @Test
    public void testRawTimestampedValue() {
        long timestamp = 123456789L;

        Headers headers = new RecordHeaders().add("key1", "value1".getBytes(StandardCharsets.UTF_8));
        ValueTimestampHeaders<String> input = ValueTimestampHeaders.make(VALUE, timestamp, headers);
        try (
                ValueTimestampHeadersSerializer<String> serializer = new ValueTimestampHeadersSerializer<>(Serdes.String().serializer());
                ValueAndTimestampSerde<String> stringSerde = new ValueAndTimestampSerde<>(Serdes.String())
            ) {
            byte[] inputBytes = serializer.serialize(TOPIC, input);
            byte[] outputBytes = rawTimestampedValue(inputBytes);
            ValueAndTimestamp<String> output = stringSerde.deserializer().deserialize(TOPIC, outputBytes);

            assertEquals(timestamp, output.timestamp());
            assertEquals(VALUE, output.value());
        }
    }

    @Test
    public void testReadBytes() {
        byte[] valueBytes = VALUE.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.wrap(valueBytes);

        assertThrows(SerializationException.class, () -> readBytes(buf, -1));
        assertThrows(SerializationException.class, () -> readBytes(buf, valueBytes.length + 1));
        
        assertEquals('t', readBytes(buf, 1)[0]);
        assertEquals('e', readBytes(buf, 1)[0]);

        byte[] nextTwo = readBytes(buf, 2);
        assertEquals(2, nextTwo.length);
        assertEquals('s', nextTwo[0]);
        assertEquals('t', nextTwo[1]);

        byte[] tail = readBytes(buf, buf.remaining());
        assertEquals(6, tail.length);
        assertArrayEquals("-value".getBytes(StandardCharsets.UTF_8), tail);

        assertThrows(SerializationException.class, () -> readBytes(buf, 1));
    }

    @Test
    public void testKeyBytes() {
        StateSerdes<String, String> serdes = StateSerdes.withBuiltinTypes(TOPIC, String.class, String.class);
        
    }
}
