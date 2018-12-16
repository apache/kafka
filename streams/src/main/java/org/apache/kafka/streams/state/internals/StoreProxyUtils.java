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

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;

class StoreProxyUtils {
    private static final byte[] UNKNOWN_TIMESTAMP_BYTE_ARRAY = new LongSerializer().serialize(null, -1L);

    static byte[] getValue(final byte[] rawValueWithTimestamp) {
        if (rawValueWithTimestamp == null) {
            return null;
        }
        final byte[] rawValue = new byte[rawValueWithTimestamp.length - 8];
        // TODO: should we use `ByteBuffer` instead of `System.arraycopy` ?
        System.arraycopy(rawValueWithTimestamp, 8, rawValue, 0, rawValue.length);
        return rawValue;
    }

    static byte[] getValueWithUnknownTimestamp(final byte[] rawValue) {
        if (rawValue == null) {
            return null;
        }
        final byte[] rawValueWithUnknownTimestamp = new byte[8 + rawValue.length];
        // TODO: should we use `ByteBuffer` instead of `System.arraycopy` ?
        System.arraycopy(UNKNOWN_TIMESTAMP_BYTE_ARRAY, 0, rawValueWithUnknownTimestamp, 0, 8);
        System.arraycopy(rawValue, 0, rawValueWithUnknownTimestamp, 8, rawValue.length);
        return rawValueWithUnknownTimestamp;
    }

    static class KeyValueIteratorProxy<K> implements KeyValueIterator<K, byte[]> {
        private final KeyValueIterator<K, byte[]> innerIterator;

        KeyValueIteratorProxy(final KeyValueIterator<K, byte[]> innerIterator) {
            this.innerIterator = innerIterator;
        }

        @Override
        public void close() {
            innerIterator.close();
        }

        @Override
        public K peekNextKey() {
            return innerIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public KeyValue<K, byte[]> next() {
            final KeyValue<K, byte[]> plainKeyValue = innerIterator.next();
            return KeyValue.pair(plainKeyValue.key, getValueWithUnknownTimestamp(plainKeyValue.value));
        }
    }

    static class WindowIteratorProxy extends KeyValueIteratorProxy<Long> implements WindowStoreIterator<byte[]> {

        WindowIteratorProxy(final KeyValueIterator<Long, byte[]> innerIterator) {
            super(innerIterator);
        }
    }
}
