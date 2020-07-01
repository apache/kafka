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
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

public class RocksDBWindowStore
    extends WrappedStateStore<SegmentedBytesStore, Object, Object>
    implements WindowStore<Bytes, byte[]> {

    private final boolean retainDuplicates;
    private final long windowSize;

    private ProcessorContext context;
    private int seqnum = 0;

    RocksDBWindowStore(final SegmentedBytesStore bytesStore,
                       final boolean retainDuplicates,
                       final long windowSize) {
        super(bytesStore);
        this.retainDuplicates = retainDuplicates;
        this.windowSize = windowSize;
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        this.context = context;
        super.init(context, root);
    }

    @Deprecated
    @Override
    public void put(final Bytes key, final byte[] value) {
        put(key, value, context.timestamp());
    }

    @Override
    public void put(final Bytes key, final byte[] value, final long windowStartTimestamp) {
        // Skip if value is null and duplicates are allowed since this delete is a no-op
        if (!(value == null && retainDuplicates)) {
            maybeUpdateSeqnumForDups();
            wrapped().put(WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, seqnum), value);
        }
    }

    @Override
    public byte[] fetch(final Bytes key, final long timestamp) {
        final byte[] bytesValue = wrapped().get(WindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum));
        return bytesValue;
    }

    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetch(...) is removed
    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(key, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).valuesIterator();
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(Bytes key, Instant from, Instant to) throws IllegalArgumentException {
        final long timeFrom = ApiUtils.validateMillisecondInstant(from, prepareMillisCheckFailMsgPrefix(from, "from"));
        final long timeTo = ApiUtils.validateMillisecondInstant(to, prepareMillisCheckFailMsgPrefix(to, "to"));
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetch(key, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).valuesIterator();
    }

    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetch(...) is removed
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from,
                                                           final Bytes to,
                                                           final long timeFrom,
                                                           final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(from, to, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(Bytes from, Bytes to, Instant fromTime, Instant toTime) {
        final long timeFrom = ApiUtils.validateMillisecondInstant(fromTime, prepareMillisCheckFailMsgPrefix(fromTime, "from"));
        final long timeTo = ApiUtils.validateMillisecondInstant(toTime, prepareMillisCheckFailMsgPrefix(toTime, "to"));
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetch(from, to, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().all();
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardAll();
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetchAll(...) is removed
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetchAll(timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final Instant from, final Instant to) {
        final long timeFrom = ApiUtils.validateMillisecondInstant(from, prepareMillisCheckFailMsgPrefix(from, "from"));
        final long timeTo = ApiUtils.validateMillisecondInstant(to, prepareMillisCheckFailMsgPrefix(to, "to"));
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetchAll(timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    private void maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
    }
}
