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
package org.apache.kafka.streams.processor.internals;

import java.util.LinkedList;

/**
 * MinTimestampTracker implements {@link TimestampTracker} that maintains the min
 * timestamp of the maintained stamped elements.
 */
public class MinTimestampTracker<E> implements TimestampTracker<E> {

    // first element has the lowest timestamp and last element the highest
    private final LinkedList<Stamped<E>> ascendingSubsequence = new LinkedList<>();

    // in the case that incoming traffic is very small, the records maybe put and polled
    // within a single iteration, in this case we need to remember the last polled
    // record's timestamp
    private long lastKnownTime = NOT_KNOWN;

    /**
     * @throws NullPointerException if the element is null
     */
    public void addElement(final Stamped<E> elem) {
        if (elem == null) throw new NullPointerException();

        Stamped<E> maxElem = ascendingSubsequence.peekLast();
        while (maxElem != null && maxElem.timestamp >= elem.timestamp) {
            ascendingSubsequence.removeLast();
            maxElem = ascendingSubsequence.peekLast();
        }
        ascendingSubsequence.offerLast(elem); //lower timestamps have been retained and all greater/equal removed
    }

    public void removeElement(final Stamped<E> elem) {
        if (elem == null) {
            return;
        }

        if (ascendingSubsequence.peekFirst() == elem) {
            ascendingSubsequence.removeFirst();
        }

        if (ascendingSubsequence.isEmpty()) {
            lastKnownTime = elem.timestamp;
        }

    }

    public int size() {
        return ascendingSubsequence.size();
    }

    /**
     * @return the lowest tracked timestamp
     */
    public long get() {
        Stamped<E> stamped = ascendingSubsequence.peekFirst();

        if (stamped == null)
            return lastKnownTime;
        else
            return stamped.timestamp;
    }

}
