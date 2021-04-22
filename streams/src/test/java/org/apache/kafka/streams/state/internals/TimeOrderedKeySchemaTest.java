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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.StateSerdes;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TimeOrderedKeySchemaTest {
    final private String key = "key";
    final private long startTime = 50L;
    final private long endTime = 100L;
    final private Serde<String> serde = Serdes.String();

    final private Window window = new TimeWindow(startTime, endTime);
    final private Windowed<String> windowedKey = new Windowed<>(key, window);
    final private StateSerdes<String, byte[]> stateSerdes = new StateSerdes<>("dummy", serde, Serdes.ByteArray());

    @Test
    public void shouldConvertToBinaryAndBack() {
        final Bytes serialized = TimeOrderedKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        final Windowed<String> result = TimeOrderedKeySchema.fromStoreKey(serialized.get(), endTime - startTime, stateSerdes.keyDeserializer(), stateSerdes.topic());
        assertEquals(windowedKey, result);
    }

    @Test
    public void shouldExtractSequenceFromBinary() {
        final Bytes serialized = TimeOrderedKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertEquals(0, TimeOrderedKeySchema.extractStoreSequence(serialized.get()));
    }

    @Test
    public void shouldExtractStartTimeFromBinary() {
        final Bytes serialized = TimeOrderedKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertEquals(startTime, TimeOrderedKeySchema.extractStoreTimestamp(serialized.get()));
    }

    @Test
    public void shouldExtractWindowFromBinary() {
        final Bytes serialized = TimeOrderedKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertEquals(window, TimeOrderedKeySchema.extractStoreWindow(serialized.get(), endTime - startTime));
    }

    @Test
    public void shouldExtractKeyBytesFromBinary() {
        final Bytes serialized = TimeOrderedKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertArrayEquals(key.getBytes(), TimeOrderedKeySchema.extractStoreKeyBytes(serialized.get()));
    }

    @Test
    public void shouldExtractKeyFromBinary() {
        final Bytes serialized = TimeOrderedKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertEquals(windowedKey, TimeOrderedKeySchema.fromStoreKey(serialized.get(), endTime - startTime, stateSerdes.keyDeserializer(), stateSerdes.topic()));
    }

    @Test
    public void shouldExtractBytesKeyFromBinary() {
        final Windowed<Bytes> windowedBytesKey = new Windowed<>(Bytes.wrap(key.getBytes()), window);
        final Bytes serialized = TimeOrderedKeySchema.toStoreKeyBinary(windowedBytesKey, 0);
        assertEquals(windowedBytesKey, TimeOrderedKeySchema.fromStoreBytesKey(serialized.get(), endTime - startTime));
    }
}
