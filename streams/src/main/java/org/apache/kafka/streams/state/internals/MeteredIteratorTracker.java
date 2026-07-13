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

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;

final class MeteredIteratorTracker {

    private final LongAdder numOpenIterators;
    private final NavigableSet<MeteredIterator> openIterators;

    MeteredIteratorTracker() {
        this(
            new LongAdder(),
            new ConcurrentSkipListSet<>(Comparator.comparingLong(MeteredIterator::startTimestamp))
        );
    }

    MeteredIteratorTracker(final LongAdder numOpenIterators,
                           final NavigableSet<MeteredIterator> openIterators) {
        this.numOpenIterators = numOpenIterators;
        this.openIterators = openIterators;
    }

    void add(final MeteredIterator iterator) {
        numOpenIterators.increment();
        openIterators.add(iterator);
    }

    void remove(final MeteredIterator iterator) {
        numOpenIterators.decrement();
        openIterators.remove(iterator);
    }

    long numOpenIterators() {
        return numOpenIterators.sum();
    }

    long oldestIteratorStartTimestamp() {
        try {
            final Iterator<MeteredIterator> iterator = openIterators.iterator();
            return iterator.hasNext() ? iterator.next().startTimestamp() : 0L;
        } catch (final NoSuchElementException e) {
            return 0L;
        }
    }
}
