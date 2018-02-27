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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.WindowedSerdes.SessionWindowedSerde;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SessionWindowedSerdesTest {

    final private String key = "key";
    final private String topic = "topic";
    final private long startTime = 50L;
    final private long endTime = 100L;
    final private Serde<String> serde = Serdes.String();

    final private Window window = new SessionWindow(startTime, endTime);
    final private Windowed<String> windowedKey = new Windowed<>(key, window);
    final private Serde<Windowed<String>> keySerde = new SessionWindowedSerde<>(serde);

    @Test
    public void shouldSerializeDeserialize() {
        final byte[] bytes = keySerde.serializer().serialize(topic, windowedKey);
        final Windowed<String> result = keySerde.deserializer().deserialize(topic, bytes);
        assertEquals(windowedKey, result);
    }

    @Test
    public void shouldSerializeNullToNull() {
        assertNull(keySerde.serializer().serialize(topic, null));
    }

    @Test
    public void shouldDeSerializeEmtpyByteArrayToNull() {
        assertNull(keySerde.deserializer().deserialize(topic, new byte[0]));
    }

    @Test
    public void shouldDeSerializeNullToNull() {
        assertNull(keySerde.deserializer().deserialize(topic, null));
    }

    @Test
    public void shouldConvertToBinaryAndBack() {
        final byte[] serialized = SessionWindowedSerde.toBinary(windowedKey, serde.serializer(), "dummy");
        final Windowed<String> result = SessionWindowedSerde.from(serialized, Serdes.String().deserializer(), "dummy");
        assertEquals(windowedKey, result);
    }

    @Test
    public void shouldExtractEndTimeFromBinary() {
        final byte[] serialized = SessionWindowedSerde.toBinary(windowedKey, serde.serializer(), "dummy");
        assertEquals(endTime, SessionWindowedSerde.extractEndTimestamp(serialized));
    }

    @Test
    public void shouldExtractStartTimeFromBinary() {
        final byte[] serialized = SessionWindowedSerde.toBinary(windowedKey, serde.serializer(), "dummy");
        assertEquals(startTime, SessionWindowedSerde.extractStartTimestamp(serialized));
    }

    @Test
    public void shouldExtractWindowFromBindary() {
        final byte[] serialized = SessionWindowedSerde.toBinary(windowedKey, serde.serializer(), "dummy");
        assertEquals(window, SessionWindowedSerde.extractWindow(serialized));
    }

    @Test
    public void shouldExtractKeyBytesFromBinary() {
        final byte[] serialized = SessionWindowedSerde.toBinary(windowedKey, serde.serializer(), "dummy");
        assertArrayEquals(key.getBytes(), SessionWindowedSerde.extractKeyBytes(serialized));
    }

    @Test
    public void shouldExtractKeyFromBinary() {
        final byte[] serialized = SessionWindowedSerde.toBinary(windowedKey, serde.serializer(), "dummy");
        assertEquals(windowedKey, SessionWindowedSerde.from(serialized, serde.deserializer(), "dummy"));
    }

    @Test
    public void shouldExtractBytesKeyFromBinary() {
        final Bytes bytesKey = Bytes.wrap(key.getBytes());
        final Windowed<Bytes> windowedBytesKey = new Windowed<>(bytesKey, window);
        final Bytes serialized = Bytes.wrap(SessionWindowedSerde.toBinary(windowedBytesKey));
        assertEquals(windowedBytesKey, SessionWindowedSerde.from(serialized));
    }
}
