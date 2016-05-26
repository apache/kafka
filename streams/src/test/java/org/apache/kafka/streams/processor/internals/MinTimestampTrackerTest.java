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

package org.apache.kafka.streams.processor.internals;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MinTimestampTrackerTest {

    private Stamped<String> elem(long timestamp) {
        return new Stamped<>("", timestamp);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testTracking() {
        TimestampTracker<String> tracker = new MinTimestampTracker<>();

        Object[] elems = new Object[]{
            elem(100), elem(101), elem(102), elem(98), elem(99), elem(100)
        };

        int insertionIndex = 0;
        int removalIndex = 0;

        // add 100
        tracker.addElement((Stamped<String>) elems[insertionIndex++]);
        assertEquals(100L, tracker.get());

        // add 101
        tracker.addElement((Stamped<String>) elems[insertionIndex++]);
        assertEquals(100L, tracker.get());

        // remove 100
        tracker.removeElement((Stamped<String>) elems[removalIndex++]);
        assertEquals(101L, tracker.get());

        // add 102
        tracker.addElement((Stamped<String>) elems[insertionIndex++]);
        assertEquals(101L, tracker.get());

        // add 98
        tracker.addElement((Stamped<String>) elems[insertionIndex++]);
        assertEquals(98L, tracker.get());

        // add 99
        tracker.addElement((Stamped<String>) elems[insertionIndex++]);
        assertEquals(98L, tracker.get());

        // add 100
        tracker.addElement((Stamped<String>) elems[insertionIndex++]);
        assertEquals(98L, tracker.get());

        // remove 101
        tracker.removeElement((Stamped<String>) elems[removalIndex++]);
        assertEquals(98L, tracker.get());

        // remove 102
        tracker.removeElement((Stamped<String>) elems[removalIndex++]);
        assertEquals(98L, tracker.get());

        // remove 98
        tracker.removeElement((Stamped<String>) elems[removalIndex++]);
        assertEquals(99L, tracker.get());

        // remove 99
        tracker.removeElement((Stamped<String>) elems[removalIndex++]);
        assertEquals(100L, tracker.get());

        // remove 100
        tracker.removeElement((Stamped<String>) elems[removalIndex++]);
        assertEquals(100L, tracker.get());

        assertEquals(insertionIndex, removalIndex);
    }

}