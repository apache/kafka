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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.apache.kafka.streams.state.internals.Utils.rawPlainValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class UtilsTest {

    @Test
    public void shouldExtractRawPlainValue() {
        // Format: [headersSize(varint)][headers][timestamp(8)][value]
        // Create a value with headers size=0, timestamp=-1, value="test"
        final byte[] value = "test".getBytes();
        final ByteBuffer buffer = ByteBuffer.allocate(1 + 8 + value.length);
        buffer.put((byte) 0x00); // headers size = 0
        buffer.putLong(-1L); // timestamp = -1
        buffer.put(value);

        final byte[] result = rawPlainValue(buffer.array());

        assertArrayEquals(value, result);
    }

    @Test
    public void shouldReturnNullForNullRawPlainValue() {
        assertNull(rawPlainValue(null));
    }

}