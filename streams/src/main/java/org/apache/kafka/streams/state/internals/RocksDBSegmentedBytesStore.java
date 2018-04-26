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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.List;
import java.util.Objects;

class RocksDBSegmentedBytesStore implements SegmentedBytesStore {

    private final String name;
    private final Segments segments;
    private final KeySchema keySchema;
    private ProcessorContext context;
    private volatile boolean open;

    RocksDBSegmentedBytesStore(final String name,
                               final long retention,
                               final int numSegments,
                               final KeySchema keySchema) {
        this.name = name;
        this.keySchema = keySchema;
        this.segments = new Segments(name, retention, numSegments, keySchema.bytesComparator());
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetch(final Bytes key, final long from, final long to) {
        Objects.requireNonNull(key, "key cannot be null");

        final List<Segment> searchSpace = keySchema.segmentsToSearch(segments, from, to);
        final Bytes binaryFrom = keySchema.lowerRange(key, from);
        final Bytes binaryTo = keySchema.upperRange(key, to);

        return new SegmentIterator(searchSpace.iterator(),
                                   keySchema.hasNextCondition(key, key, from, to),
                                   binaryFrom, binaryTo);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetch(final Bytes keyFrom, Bytes keyTo, final long from, final long to) {
        Objects.requireNonNull(keyFrom, "keyFrom cannot be null");
        Objects.requireNonNull(keyTo, "keyTo cannot be null");

        final List<Segment> searchSpace = keySchema.segmentsToSearch(segments, from, to);
        final Bytes binaryFrom = keySchema.lowerRange(keyFrom, from);
        final Bytes binaryTo = keySchema.upperRange(keyTo, to);

        return new SegmentIterator(searchSpace.iterator(),
                                   keySchema.hasNextCondition(keyFrom, keyTo, from, to),
                                   binaryFrom, binaryTo);
    }
    
    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        
        final List<Segment> searchSpace = segments.allSegments();
        
        return new SegmentIterator(searchSpace.iterator(),
                                   keySchema.hasNextCondition(null, null, 0, Long.MAX_VALUE),
                                   null, null);
    }
    
    @Override
    public KeyValueIterator<Bytes, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        final List<Segment> searchSpace = segments.segments(timeFrom, timeTo);
        
        return new SegmentIterator(searchSpace.iterator(),
                                   keySchema.hasNextCondition(null, null, timeFrom, timeTo),
                                   null, null);
    }
    
    @Override
    public void remove(final Bytes key) {
        final Segment segment = segments.getSegmentForTimestamp(keySchema.segmentTimestamp(key));
        if (segment == null) {
            return;
        }
        segment.delete(key);
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");

        final long segmentId = segments.segmentId(keySchema.segmentTimestamp(key));
        final Segment segment = segments.getOrCreateSegment(segmentId, context);
        if (segment != null) {
            segment.put(key, value);
        }
    }

    @Override
    public byte[] get(final Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");

        final Segment segment = segments.getSegmentForTimestamp(keySchema.segmentTimestamp(key));
        if (segment == null) {
            return null;
        }
        return segment.get(key);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        this.context = context;

        keySchema.init(ProcessorStateManager.storeChangelogTopic(context.applicationId(), root.name()));

        segments.openExisting(context);

        // register and possibly restore the state from the logs
        context.register(root, false, new StateRestoreCallback() {
            @Override
            public void restore(byte[] key, byte[] value) {
                put(Bytes.wrap(key), value);
            }
        });

        flush();
        open = true;
    }

    @Override
    public void flush() {
        segments.flush();
    }

    @Override
    public void close() {
        open = false;
        segments.close();
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

}
