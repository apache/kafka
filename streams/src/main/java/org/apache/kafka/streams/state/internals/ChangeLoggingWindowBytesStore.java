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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Simple wrapper around a {@link SegmentedBytesStore} to support writing
 * updates to a changelog
 */
class ChangeLoggingWindowBytesStore extends WrappedStateStore.AbstractStateStore implements WindowStore<Bytes, byte[]> {

    private final WindowStore<Bytes, byte[]> bytesStore;
    private final boolean retainDuplicates;
    private StoreChangeLogger<Bytes, byte[]> changeLogger;
    private ProcessorContext context;
    private StateSerdes<Bytes, byte[]> innerStateSerde;
    private int seqnum = 0;

    ChangeLoggingWindowBytesStore(final WindowStore<Bytes, byte[]> bytesStore,
                                  final boolean retainDuplicates) {
        super(bytesStore);
        this.bytesStore = bytesStore;
        this.retainDuplicates = retainDuplicates;
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final long from, final long to) {
        return bytesStore.fetch(key, from, to);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes keyFrom, Bytes keyTo, long from, long to) {
        return bytesStore.fetch(keyFrom, keyTo, from, to);
    }


    @Override
    public void put(final Bytes key, final byte[] value) {
        put(key, value, context.timestamp());
    }

    @Override
    public void put(final Bytes key, final byte[] value, final long timestamp) {
        bytesStore.put(key, value, timestamp);
        changeLogger.logChange(WindowStoreUtils.toBinaryKey(key, timestamp, maybeUpdateSeqnumForDups(), innerStateSerde), value);
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        this.context = context;
        bytesStore.init(context, root);
        innerStateSerde = WindowStoreUtils.getInnerStateSerde(ProcessorStateManager.storeChangelogTopic(context.applicationId(), bytesStore.name()));
        changeLogger = new StoreChangeLogger<>(
            name(),
            context,
            innerStateSerde);
    }

    private int maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
        return seqnum;
    }
}
