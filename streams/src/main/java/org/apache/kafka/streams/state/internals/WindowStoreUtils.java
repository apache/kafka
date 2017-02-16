/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.StateSerdes;

import java.nio.ByteBuffer;

public class WindowStoreUtils {

    private static final int SEQNUM_SIZE = 4;
    private static final int TIMESTAMP_SIZE = 8;

    /** Inner byte array serde used for segments */
    static final Serde<Bytes> INNER_KEY_SERDE = Serdes.Bytes();
    static final Serde<byte[]> INNER_VALUE_SERDE = Serdes.ByteArray();
    static final StateSerdes<Bytes, byte[]> INNER_SERDES = new StateSerdes<>("rocksDB-inner", INNER_KEY_SERDE, INNER_VALUE_SERDE);

    static <K> Bytes toBinaryKey(K key, final long timestamp, final int seqnum, StateSerdes<K, ?> serdes) {
        byte[] serializedKey = serdes.rawKey(key);
        return toBinaryKey(serializedKey, timestamp, seqnum);
    }

    static Bytes toBinaryKey(byte[] serializedKey, final long timestamp, final int seqnum) {
        ByteBuffer buf = ByteBuffer.allocate(serializedKey.length + TIMESTAMP_SIZE + SEQNUM_SIZE);
        buf.put(serializedKey);
        buf.putLong(timestamp);
        buf.putInt(seqnum);

        return Bytes.wrap(buf.array());
    }

    static <K> K keyFromBinaryKey(byte[] binaryKey, StateSerdes<K, ?> serdes) {
        byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE];

        System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);

        return serdes.keyFrom(bytes);
    }

    static Bytes bytesKeyFromBinaryKey(byte[] binaryKey) {
        byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE];

        System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);

        return Bytes.wrap(bytes);
    }

    static long timestampFromBinaryKey(byte[] binaryKey) {
        return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE);
    }

    static int sequenceNumberFromBinaryKey(byte[] binaryKey) {
        return ByteBuffer.wrap(binaryKey).getInt(binaryKey.length - SEQNUM_SIZE);
    }
}
