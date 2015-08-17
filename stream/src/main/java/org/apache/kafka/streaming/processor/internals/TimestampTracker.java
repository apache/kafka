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

package org.apache.kafka.streaming.processor.internals;

/**
 * TimestampTracker is a helper class for a sliding window implementation.
 * It is assumed that elements are added or removed in a FIFO manner.
 * It maintains the timestamp, like the min timestamp, the max timestamp, etc. of stamped elements
 * that were added but not yet removed.
 */
public interface TimestampTracker<E> {

    /**
     * Adds a stamped elements to this tracker.
     *
     * @param elem the added element
     */
    void addStampedElement(Stamped<E> elem);

    /**
     * Removed a stamped elements to this tracker.
     *
     * @param elem the removed element
     */
    void removeStampedElement(Stamped<E> elem);

    /**
     * Returns the timestamp
     *
     * @return timestamp, or -1L when empty
     */
    long get();

    /**
     * Returns the size of internal structure. The meaning of "size" depends on the implementation.
     *
     * @return size
     */
    int size();

}
