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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestampWithHeaders;
import org.apache.kafka.streams.state.WindowStoreIterator;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

/**
 * A ChangeLogging wrapper for a timestamped window store that supports headers.
 * Headers are stored in the state store but NOT logged to the changelog topic,
 * per the KIP design.
 */
class ChangeLoggingTimestampedWindowStoreWithHeaders
    extends WrappedStateStore<TimestampedWindowStoreWithHeaders<Bytes, byte[]>, byte[], byte[]>
    implements TimestampedWindowStoreWithHeaders<Bytes, byte[]> {

    private final boolean retainDuplicates;
    private InternalProcessorContext<?, ?> internalContext;
    private int seqnum = 0;

    ChangeLoggingTimestampedWindowStoreWithHeaders(final TimestampedWindowStoreWithHeaders<Bytes, byte[]> bytesStore,
                                                   final boolean retainDuplicates) {
        super(bytesStore);
        this.retainDuplicates = retainDuplicates;
    }

    @Override
    public void init(final StateStoreContext stateStoreContext,
                     final StateStore root) {
        internalContext = asInternalProcessorContext(stateStoreContext);
        super.init(stateStoreContext, root);
    }

    @Override
    public void put(final Bytes key,
                    final ValueAndTimestampWithHeaders<byte[]> value,
                    final long windowStartTimestamp) {
        wrapped().put(key, value, windowStartTimestamp);

        final Bytes keyBytes = WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, maybeUpdateSeqnumForDups());

        if (value != null) {
            // Per KIP: headers are stored in state store but NOT in changelog
            internalContext.logChange(
                name(),
                keyBytes,
                value.value(),
                value.timestamp(),
                wrapped().getPosition()
            );
        } else {
            internalContext.logChange(
                name(),
                keyBytes,
                null,
                internalContext.recordContext().timestamp(),
                wrapped().getPosition()
            );
        }
    }

    @Override
    public void put(final Bytes key,
                    final byte[] value,
                    final long windowStartTimestamp,
                    final long timestamp,
                    final Headers headers) {
        wrapped().put(key, value, windowStartTimestamp, timestamp, headers);

        final Bytes keyBytes = WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, maybeUpdateSeqnumForDups());

        // Per KIP: headers are stored in state store but NOT in changelog
        internalContext.logChange(
            name(),
            keyBytes,
            value,
            timestamp,
            wrapped().getPosition()
        );
    }

    @Override
    public ValueAndTimestampWithHeaders<byte[]> fetch(final Bytes key,
                                                      final long timestamp) {
        return wrapped().fetch(key, timestamp);
    }

    @Override
    public WindowStoreIterator<ValueAndTimestampWithHeaders<byte[]>> fetch(final Bytes key,
                                                                           final long from,
                                                                           final long to) {
        return wrapped().fetch(key, from, to);
    }

    @Override
    public WindowStoreIterator<ValueAndTimestampWithHeaders<byte[]>> backwardFetch(final Bytes key,
                                                                                    final long timeFrom,
                                                                                    final long timeTo) {
        return wrapped().backwardFetch(key, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueAndTimestampWithHeaders<byte[]>> fetch(final Bytes keyFrom,
                                                                                          final Bytes keyTo,
                                                                                          final long timeFrom,
                                                                                          final long timeTo) {
        return wrapped().fetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueAndTimestampWithHeaders<byte[]>> backwardFetch(final Bytes keyFrom,
                                                                                                  final Bytes keyTo,
                                                                                                  final long timeFrom,
                                                                                                  final long timeTo) {
        return wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueAndTimestampWithHeaders<byte[]>> all() {
        return wrapped().all();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueAndTimestampWithHeaders<byte[]>> backwardAll() {
        return wrapped().backwardAll();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueAndTimestampWithHeaders<byte[]>> fetchAll(final long timeFrom,
                                                                                             final long timeTo) {
        return wrapped().fetchAll(timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueAndTimestampWithHeaders<byte[]>> backwardFetchAll(final long timeFrom,
                                                                                                     final long timeTo) {
        return wrapped().backwardFetchAll(timeFrom, timeTo);
    }

    @Override
    public WindowStoreIterator<ValueAndTimestampWithHeaders<byte[]>> fetchWithHeaders(final Bytes key,
                                                                                       final long timeFrom,
                                                                                       final long timeTo) {
        return wrapped().fetchWithHeaders(key, timeFrom, timeTo);
    }

    @Override
    public WindowStoreIterator<ValueAndTimestampWithHeaders<byte[]>> backwardFetchWithHeaders(final Bytes key,
                                                                                                final long timeFrom,
                                                                                                final long timeTo) {
        return wrapped().backwardFetchWithHeaders(key, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueAndTimestampWithHeaders<byte[]>> fetchWithHeaders(final Bytes keyFrom,
                                                                                                     final Bytes keyTo,
                                                                                                     final long timeFrom,
                                                                                                     final long timeTo) {
        return wrapped().fetchWithHeaders(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueAndTimestampWithHeaders<byte[]>> backwardFetchWithHeaders(final Bytes keyFrom,
                                                                                                             final Bytes keyTo,
                                                                                                             final long timeFrom,
                                                                                                             final long timeTo) {
        return wrapped().backwardFetchWithHeaders(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueAndTimestampWithHeaders<byte[]>> fetchAllWithHeaders(final long timeFrom,
                                                                                                        final long timeTo) {
        return wrapped().fetchAllWithHeaders(timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueAndTimestampWithHeaders<byte[]>> backwardFetchAllWithHeaders(final long timeFrom,
                                                                                                                final long timeTo) {
        return wrapped().backwardFetchAllWithHeaders(timeFrom, timeTo);
    }

    private int maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
        return seqnum;
    }
}
