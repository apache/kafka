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
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;


import static java.util.Objects.requireNonNull;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

/**
 * Simple wrapper around a {@link WindowStore} to support writing
 * updates to a changelog
 */
class ChangeLoggingWindowBytesStore
        extends WrappedStateStore<WindowStore<Bytes, byte[]>, byte[], byte[]>
        implements WindowStore<Bytes, byte[]> {

    interface ChangeLoggingKeySerializer {
        Bytes serialize(final Bytes key, final long timestamp, final int seqnum);
    }

    private final boolean retainDuplicates;
    InternalProcessorContext context;
    private int seqnum = 0;
    private final ChangeLoggingKeySerializer keySerializer;

    ChangeLoggingWindowBytesStore(final WindowStore<Bytes, byte[]> bytesStore,
                                  final boolean retainDuplicates,
                                  final ChangeLoggingKeySerializer keySerializer) {
        super(bytesStore);
        this.retainDuplicates = retainDuplicates;
        this.keySerializer = requireNonNull(keySerializer, "keySerializer");
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        this.context = asInternalProcessorContext(context);
        super.init(context, root);
    }

    @Override
    public void init(final StateStoreContext context,
                     final StateStore root) {
        this.context = asInternalProcessorContext(context);
        super.init(context, root);
    }

    @Override
    public byte[] fetch(final Bytes key,
                        final long timestamp) {
        return wrapped().fetch(key, timestamp);
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key,
                                             final long from,
                                             final long to) {
        return wrapped().fetch(key, from, to);
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key,
                                                     final long timeFrom,
                                                     final long timeTo) {
        return wrapped().backwardFetch(key, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom,
                                                           final Bytes keyTo,
                                                           final long timeFrom,
                                                           final long to) {
        return wrapped().fetch(keyFrom, keyTo, timeFrom, to);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom,
                                                                   final Bytes keyTo,
                                                                   final long timeFrom,
                                                                   final long timeTo) {
        return wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        return wrapped().all();
    }


    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        return wrapped().backwardAll();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom,
                                                              final long timeTo) {
        return wrapped().fetchAll(timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom,
                                                                      final long timeTo) {
        return wrapped().backwardFetchAll(timeFrom, timeTo);
    }

    @Override
    public void put(final Bytes key,
                    final byte[] value,
                    final long windowStartTimestamp) {
        wrapped().put(key, value, windowStartTimestamp);

        log(keySerializer.serialize(key, windowStartTimestamp, maybeUpdateSeqnumForDups()), value);
    }

    void log(final Bytes key, final byte[] value) {
        context.logChange(name(), key, value, context.timestamp(), wrapped().getPosition());
    }

    private int maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
        return seqnum;
    }
}
