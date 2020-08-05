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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.rocksdb.RocksIterator;

import java.util.Set;

class RocksDBPrefixIterator extends RocksDbIterator {
    private byte[] rawPrefix;

    RocksDBPrefixIterator(final String name,
                          final RocksIterator newIterator,
                          final Set<KeyValueIterator<Bytes, byte[]>> openIterators,
                          final Bytes prefix) {
        super(name, newIterator, openIterators, false);
        rawPrefix = prefix.get();
        newIterator.seek(rawPrefix);
    }

    @Override
    public synchronized boolean hasNext() {
        if (!super.hasNext()) {
            return false;
        }

        final byte[] rawNextKey = super.peekNextKey().get();
        for (int i = 0; i < rawPrefix.length; i++) {
            if (i == rawNextKey.length) {
                throw new IllegalStateException("Unexpected RocksDB Key Value. Should have been skipped with seek.");
            }
            if (rawNextKey[i] != rawPrefix[i]) {
                return false;
            }
        }
        return true;
    }
}