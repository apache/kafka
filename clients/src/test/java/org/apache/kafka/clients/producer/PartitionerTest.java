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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.Cluster;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

public class PartitionerTest {

    private static final Cluster CLUSTER = mock(Cluster.class);

    private static Partitioner capturingPartitioner(byte[][] capture) {
        return new Partitioner() {
            @Override
            public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
                capture[0] = keyBytes;
                capture[1] = valueBytes;
                return 0;
            }

            @Override
            public void close() {
            }

            @Override
            public void configure(Map<String, ?> configs) {
            }
        };
    }

    @Test
    public void testByteBufferPartitionPassesBackingArrayDirectly() {
        final byte[][] captured = new byte[2][];

        try (Partitioner partitioner = capturingPartitioner(captured)) {
            byte[] keyArray = "key".getBytes();
            byte[] valueArray = "value".getBytes();

            partitioner.partition("test", null, ByteBuffer.wrap(keyArray), null, ByteBuffer.wrap(valueArray), CLUSTER);
            assertSame(keyArray, captured[0],
                    "When key ByteBuffer wraps an exact array, the backing array should be passed directly without copying");
            assertSame(valueArray, captured[1],
                    "When value ByteBuffer wraps an exact array, the backing array should be passed directly without copying");
        }
    }

    @Test
    public void testByteBufferPartitionCopiesWhenNotExactArray() {
        final byte[][] captured = new byte[2][];

        try (Partitioner partitioner = capturingPartitioner(captured)) {
            // A slice of a larger buffer - hasArray() is true but arrayOffset/length don't match
            byte[] keyBackingArray = "prefixkey".getBytes();
            ByteBuffer keySlice = ByteBuffer.wrap(keyBackingArray, 6, 3).slice();
            byte[] valueBackingArray = "prefixvalue".getBytes();
            ByteBuffer valueSlice = ByteBuffer.wrap(valueBackingArray, 6, 5).slice();

            partitioner.partition("test", null, keySlice, null, valueSlice, CLUSTER);
            assertNotSame(keyBackingArray, captured[0],
                    "When key ByteBuffer is a slice, a new array should be allocated");
            assertArrayEquals("key".getBytes(), captured[0],
                    "The copied key array should contain the correct bytes");
            assertNotSame(valueBackingArray, captured[1],
                    "When value ByteBuffer is a slice, a new array should be allocated");
            assertArrayEquals("value".getBytes(), captured[1],
                    "The copied value array should contain the correct bytes");
        }
    }
}
