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

import java.util.Iterator;

import org.apache.kafka.streams.errors.InvalidStateStoreException;

/*
 * Specialized segment iterator specifically intended for use in ReadOnlyWindowStore#fetchAll() 
 * and ReadOnlyWindowStore#all(). Allows the binary keys given to be null, and in parallel, implements
 * a slightly different version of hasNext().
 */
class TimeRangeSegmentIterator extends SegmentIterator { 
    public TimeRangeSegmentIterator(final Iterator<Segment> segments,
                                    final HasNextCondition hasNextCondition) {
        super(segments, hasNextCondition, null, null);
    }
    
    @Override
    public boolean hasNext() {
        boolean hasNext = false;
        while ((currentIterator == null || !(hasNext = hasNextCondition.hasNext(currentIterator)) || !currentSegment.isOpen())
                && segments.hasNext()) {
            super.close();
            currentSegment = segments.next();
            try {
                currentIterator = currentSegment.all();
            } catch (InvalidStateStoreException exc) {
                // Just in case that the store was closed
            }
        }
        return currentIterator != null && hasNext;
    }
}
