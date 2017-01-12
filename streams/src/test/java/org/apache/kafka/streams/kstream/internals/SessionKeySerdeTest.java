/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SessionKeySerdeTest {

    @Test
    public void shouldSerializeDeserialize() throws Exception {
        final Windowed<Long> key = new Windowed<>(1L, new SessionWindow(10, 100));
        final SessionKeySerde<Long> serde = new SessionKeySerde<>(Serdes.Long());
        final byte[] bytes = serde.serializer().serialize("t", key);
        final Windowed<Long> result = serde.deserializer().deserialize("t", bytes);
        assertEquals(key, result);
    }

    @Test
    public void shouldSerializeNullToNull() throws Exception {
        final SessionKeySerde<String> serde = new SessionKeySerde<>(Serdes.String());
        assertNull(serde.serializer().serialize("t", null));
    }

    @Test
    public void shouldDeSerializeEmtpyByteArrayToNull() throws Exception {
        final SessionKeySerde<String> serde = new SessionKeySerde<>(Serdes.String());
        assertNull(serde.deserializer().deserialize("t", new byte[0]));
    }

    @Test
    public void shouldDeSerializeNullToNull() throws Exception {
        final SessionKeySerde<String> serde = new SessionKeySerde<>(Serdes.String());
        assertNull(serde.deserializer().deserialize("t", null));
    }

    @Test
    public void shouldConvertToBinaryAndBack() throws Exception {
        final Windowed<String> key = new Windowed<>("key", new SessionWindow(10, 20));
        final Bytes serialized = SessionKeySerde.toBinary(key, Serdes.String().serializer());
        final Windowed<String> result = SessionKeySerde.from(serialized.get(), Serdes.String().deserializer());
        assertEquals(key, result);
    }

    @Test
    public void shouldExtractEndTimeFromBinary() throws Exception {
        final Windowed<String> key = new Windowed<>("key", new SessionWindow(10, 100));
        final Bytes serialized = SessionKeySerde.toBinary(key, Serdes.String().serializer());
        assertEquals(100, SessionKeySerde.extractEnd(serialized.get()));
    }

    @Test
    public void shouldExtractStartTimeFromBinary() throws Exception {
        final Windowed<String> key = new Windowed<>("key", new SessionWindow(50, 100));
        final Bytes serialized = SessionKeySerde.toBinary(key, Serdes.String().serializer());
        assertEquals(50, SessionKeySerde.extractStart(serialized.get()));
    }

    @Test
    public void shouldExtractKeyBytesFromBinary() throws Exception {
        final Windowed<String> key = new Windowed<>("blah", new SessionWindow(50, 100));
        final Bytes serialized = SessionKeySerde.toBinary(key, Serdes.String().serializer());
        assertArrayEquals("blah".getBytes(), SessionKeySerde.extractKeyBytes(serialized.get()));
    }

}