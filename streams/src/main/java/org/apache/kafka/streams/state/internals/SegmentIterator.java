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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterate over multiple KeyValueSegments
 */
class SegmentIterator<S extends Segment> implements KeyValueIterator<Bytes, byte[]> {

    private final Bytes from;
    private final Bytes to;
    private final boolean forward;
    protected final Iterator<S> segments;
    protected final HasNextCondition hasNextCondition;

    private S currentSegment;
    KeyValueIterator<Bytes, byte[]> currentIterator;

    SegmentIterator(final Iterator<S> segments,
                    final HasNextCondition hasNextCondition,
                    final Bytes from,
                    final Bytes to,
                    final boolean forward) {
        this.segments = segments;
        this.hasNextCondition = hasNextCondition;
        this.from = from;
        this.to = to;
        this.forward = forward;
    }

    @Override
    public void close() {
        if (currentIterator != null) {
            currentIterator.close();
            currentIterator = null;
        }
    }

    @Override
    public Bytes peekNextKey() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return currentIterator.peekNextKey();
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = false;
        while ((currentIterator == null || !(hasNext = hasNextConditionHasNext()) || !currentSegment.isOpen())
            && segments.hasNext()) {
            close();
            currentSegment = segments.next();
            try {
                if (from == null || to == null) {
                    if (forward) {
                        currentIterator = currentSegment.all();
                    } else {
                        currentIterator = currentSegment.reverseAll();
                    }
                } else {
                    if (forward) {
                        currentIterator = currentSegment.range(from, to);
                    } else {
                        currentIterator = currentSegment.reverseRange(from, to);
                    }
                }
            } catch (final InvalidStateStoreException e) {
                // segment may have been closed so we ignore it.
            }
        }
        return currentIterator != null && hasNext;
    }

    private boolean hasNextConditionHasNext() {
        boolean hasNext = false;
        try {
            hasNext = hasNextCondition.hasNext(currentIterator);
        } catch (final InvalidStateStoreException e) {
            //already closed so ignore
        }
        return hasNext;
    }

    @Override
    public KeyValue<Bytes, byte[]> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return currentIterator.next();
    }
}
