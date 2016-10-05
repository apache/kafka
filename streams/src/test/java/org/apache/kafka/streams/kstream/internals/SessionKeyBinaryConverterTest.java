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

public class SessionKeyBinaryConverterTest {

    @Test
    public void shouldConvertToBinaryAndBack() throws Exception {
        final Windowed<String> key = new Windowed<>("key", new TimeWindow(10, 20));
        final Bytes serialized = SessionKeyBinaryConverter.toBinary(key, Serdes.String().serializer());
        final Windowed<String> result = SessionKeyBinaryConverter.from(serialized.get(), Serdes.String().deserializer());
        assertEquals(key, result);
    }

    @Test
    public void shouldExtractEndTimeFromBinary() throws Exception {
        final Windowed<String> key = new Windowed<>("key", new TimeWindow(10, 100));
        final Bytes serialized = SessionKeyBinaryConverter.toBinary(key, Serdes.String().serializer());
        assertEquals(100, SessionKeyBinaryConverter.extractEnd(serialized.get()));
    }

    @Test
    public void shouldExtractStartTimeFromBinary() throws Exception {
        final Windowed<String> key = new Windowed<>("key", new TimeWindow(50, 100));
        final Bytes serialized = SessionKeyBinaryConverter.toBinary(key, Serdes.String().serializer());
        assertEquals(50, SessionKeyBinaryConverter.extractStart(serialized.get()));
    }

    @Test
    public void shouldExtractKeyBytesFromBinary() throws Exception {
        final Windowed<String> key = new Windowed<>("blah", new TimeWindow(50, 100));
        final Bytes serialized = SessionKeyBinaryConverter.toBinary(key, Serdes.String().serializer());
        assertArrayEquals("blah".getBytes(), SessionKeyBinaryConverter.extractKeyBytes(serialized.get()));
    }
}