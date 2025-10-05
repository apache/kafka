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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.VersionedBytesStore;

/**
 * A {@link VersionedBytesStore} wrapper for writing changelog records on calls to
 * {@link VersionedBytesStore#put(Object, Object)} and {@link VersionedBytesStore#delete(Bytes, long)}.
 */
public class ChangeLoggingVersionedKeyValueBytesStore extends ChangeLoggingKeyValueBytesStore implements VersionedBytesStore {

    private final VersionedBytesStore inner;

    ChangeLoggingVersionedKeyValueBytesStore(final KeyValueStore<Bytes, byte[]> inner) {
        super(inner);
        if (!(inner instanceof VersionedBytesStore)) {
            throw new IllegalArgumentException("inner store must be versioned");
        }
        this.inner = (VersionedBytesStore) inner;
    }

    @Override
    public long put(final Bytes key, final byte[] value, final long timestamp) {
        final long validTo = inner.put(key, value, timestamp);
        log(key, value, timestamp);
        return validTo;
    }

    @Override
    public byte[] get(final Bytes key, final long asOfTimestamp) {
        return inner.get(key, asOfTimestamp);
    }

    @Override
    public byte[] delete(final Bytes key, final long timestamp) {
        final byte[] oldValue = inner.delete(key, timestamp);
        log(key, null, timestamp);
        return oldValue;
    }

    @Override
    public void log(final Bytes key, final byte[] value, final long timestamp) {
        internalContext.logChange(
            name(),
            key,
            value,
            timestamp,
            wrapped().getPosition()
        );
    }
}
