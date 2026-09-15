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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Common super-interface of all Metered Iterator types.
 *
 * This enables tracking the timestamp the Iterator was first created, for the oldest-iterator-open-since-ms metric.
 */
public interface MeteredIterator {

    AtomicLong SEQUENCE_NUMBERS = new AtomicLong();

    /**
     * Orders Iterators by the time they were opened, oldest first, falling back to the sequence
     * number for Iterators opened within the same millisecond. The fallback is what makes this
     * ordering total: any two distinct Iterators must compare as unequal, or a sorted set of open
     * Iterators would discard one of them as a duplicate.
     */
    Comparator<MeteredIterator> OPENED_FIRST = Comparator
        .comparingLong(MeteredIterator::startTimestamp)
        .thenComparingLong(MeteredIterator::sequenceNumber);

    /**
     * @return The UNIX timestamp, in milliseconds, that this Iterator was created/opened.
     */
    long startTimestamp();

    /**
     * @return A number, unique across all Iterators, that increases in the order Iterators are created.
     */
    long sequenceNumber();
}
