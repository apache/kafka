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
package org.apache.kafka.streams.internals.metrics;

import org.apache.kafka.streams.state.internals.MeteredIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

public class OpenIterators {

    private final NavigableSet<MeteredIterator> openIterators = new ConcurrentSkipListSet<>(Comparator.comparingLong(MeteredIterator::startTimestamp));
    private final AtomicLong oldestStartTimestamp = new AtomicLong();

    public OpenIterators() { }

    public void add(final MeteredIterator iterator) {
        openIterators.add(iterator);
        updateOldestStartTimestamp();                                                                   
    }

    public void remove(final MeteredIterator iterator) {
        openIterators.remove(iterator);
        updateOldestStartTimestamp();
    }

    public long oldestStartTimestamp() {
        return oldestStartTimestamp.get();
    }

    public long sum() {
        return openIterators.size();
    }

    private void updateOldestStartTimestamp() {
        final Iterator<MeteredIterator> openIteratorsIterator = openIterators.iterator();
        if (openIteratorsIterator.hasNext()) {
            oldestStartTimestamp.set(openIteratorsIterator.next().startTimestamp());
        } else {
            oldestStartTimestamp.set(0);
        }
    }
}
