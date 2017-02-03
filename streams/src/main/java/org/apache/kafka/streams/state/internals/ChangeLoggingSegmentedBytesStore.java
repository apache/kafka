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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;

/**
 * Simple wrapper around a {@link SegmentedBytesStore} to support writing
 * updates to a changelog
 */
class ChangeLoggingSegmentedBytesStore extends WrappedStateStore.AbstractStateStore implements SegmentedBytesStore {

    private final SegmentedBytesStore bytesStore;
    private StoreChangeLogger<Bytes, byte[]> changeLogger;

    ChangeLoggingSegmentedBytesStore(final SegmentedBytesStore bytesStore) {
        super(bytesStore);
        this.bytesStore = bytesStore;
    }

    @Override
    @SuppressWarnings("unchecked")
    public KeyValueIterator<Bytes, byte[]> fetch(final Bytes key, final long from, final long to) {
        return bytesStore.fetch(key, from, to);
    }

    @Override
    public void remove(final Bytes key) {
        bytesStore.remove(key);
        changeLogger.logChange(key, null);
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        if (key != null) {
            bytesStore.put(key, value);
            changeLogger.logChange(key, value);
        }
    }

    @Override
    public byte[] get(final Bytes key) {
        return bytesStore.get(key);
    }


    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context, final StateStore root) {
        bytesStore.init(context, root);
        changeLogger = new StoreChangeLogger<>(name(), context, WindowStoreUtils.INNER_SERDES);
    }
}
