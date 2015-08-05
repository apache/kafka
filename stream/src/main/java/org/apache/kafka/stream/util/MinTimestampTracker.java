/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.stream.util;

import java.util.LinkedList;

/**
 * MinTimestampTracker is a helper class for a sliding window implementation.
 * It is assumed that elements are added or removed in a FIFO manner.
 * It maintains the minimum timestamp of stamped elements that were added but not yet removed.
 */
public class MinTimestampTracker<E> implements TimestampTracker<E> {

    private final LinkedList<Stamped<E>> descendingSubsequence = new LinkedList<Stamped<E>>();

    public void addStampedElement(Stamped<E> elem) {
        if (elem == null) throw new NullPointerException();

        Stamped<E> minElem = descendingSubsequence.peekLast();
        while (minElem != null && minElem.timestamp >= elem.timestamp) {
            descendingSubsequence.removeLast();
            minElem = descendingSubsequence.peekLast();
        }
        descendingSubsequence.offerLast(elem);
    }

    public void removeStampedElement(Stamped<E> elem) {
        if (elem != null && descendingSubsequence.peekFirst() == elem)
            descendingSubsequence.removeFirst();
    }

    public int size() {
        return descendingSubsequence.size();
    }

    public long get() {
        Stamped<E> stamped = descendingSubsequence.peekFirst();
        if (stamped == null) return -1L;

        return stamped.timestamp;
    }

}
