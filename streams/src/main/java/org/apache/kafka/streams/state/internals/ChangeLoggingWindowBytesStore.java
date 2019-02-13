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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Simple wrapper around a {@link WindowStore} to support writing
 * updates to a changelog
 */
class ChangeLoggingWindowBytesStore extends WrappedStateStore<WindowStore<Bytes, byte[]>> implements WindowStore<Bytes, byte[]> {

    private final boolean retainDuplicates;
    private StoreChangeLogger<Bytes, byte[]> changeLogger;
    private ProcessorContext context;
    private int seqnum = 0;

    ChangeLoggingWindowBytesStore(final WindowStore<Bytes, byte[]> bytesStore,
                                  final boolean retainDuplicates) {
        super(bytesStore);
        this.retainDuplicates = retainDuplicates;
    }

    @Override
    public byte[] fetch(final Bytes key, final long timestamp) {
        return wrapped().fetch(key, timestamp);
    }

    @SuppressWarnings("deprecation")
    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final long from, final long to) {
        return wrapped().fetch(key, from, to);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo, final long from, final long to) {
        return wrapped().fetch(keyFrom, keyTo, from, to);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        return wrapped().all();
    }

    @SuppressWarnings("deprecation")
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        return wrapped().fetchAll(timeFrom, timeTo);
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        put(key, value, context.timestamp());
    }

    @Override
    public void put(final Bytes key, final byte[] value, final long windowStartTimestamp) {
        wrapped().put(key, value, windowStartTimestamp);
        changeLogger.logChange(WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, maybeUpdateSeqnumForDups()), value);
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        this.context = context;
        super.init(context, root);
        final String topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), name());
        changeLogger = new StoreChangeLogger<>(
            name(),
            context,
            new StateSerdes<>(topic, Serdes.Bytes(), Serdes.ByteArray()));
    }

    private int maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
        return seqnum;
    }
}
