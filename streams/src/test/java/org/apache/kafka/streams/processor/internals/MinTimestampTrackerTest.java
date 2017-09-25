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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class MinTimestampTrackerTest {

    private MinTimestampTracker<String> tracker = new MinTimestampTracker<>();

    @Test
    public void shouldReturnNotKnownTimestampWhenNoRecordsEverAdded() {
        assertThat(tracker.get(), equalTo(TimestampTracker.NOT_KNOWN));
    }

    @Test
    public void shouldReturnTimestampOfOnlyRecord() {
        tracker.addElement(elem(100));
        assertThat(tracker.get(), equalTo(100L));
    }

    @Test
    public void shouldReturnLowestAvailableTimestampFromAllInputs() {
        tracker.addElement(elem(100));
        tracker.addElement(elem(99));
        tracker.addElement(elem(102));
        assertThat(tracker.get(), equalTo(99L));
    }

    @Test
    public void shouldReturnLowestAvailableTimestampAfterPreviousLowestRemoved() {
        final Stamped<String> lowest = elem(88);
        tracker.addElement(lowest);
        tracker.addElement(elem(101));
        tracker.addElement(elem(99));
        tracker.removeElement(lowest);
        assertThat(tracker.get(), equalTo(99L));
    }

    @Test
    public void shouldReturnLastKnownTimestampWhenAllElementsHaveBeenRemoved() {
        final Stamped<String> record = elem(98);
        tracker.addElement(record);
        tracker.removeElement(record);
        assertThat(tracker.get(), equalTo(98L));
    }

    @Test
    public void shouldIgnoreNullRecordOnRemove() {
        tracker.removeElement(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionWhenTryingToAddNullElement() {
        tracker.addElement(null);
    }

    private Stamped<String> elem(final long timestamp) {
        return new Stamped<>("", timestamp);
    }
}