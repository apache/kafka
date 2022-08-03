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

package org.apache.kafka.shell;

import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 120000, unit = MILLISECONDS)
public class TrackingBatchReaderTest {
    static class MockBatchReader implements BatchReader<Integer> {
        final AtomicBoolean closed;
        private final List<Integer> ints;
        private final Iterator<Integer> iter;

        MockBatchReader(List<Integer> ints) {
            this.closed = new AtomicBoolean(false);
            this.ints = new ArrayList<>(ints);
            this.iter = this.ints.iterator();
        }

        @Override
        public long baseOffset() {
            return ints.get(0);
        }

        @Override
        public OptionalLong lastOffset() {
            return OptionalLong.empty();
        }

        @Override
        public void close() {
            closed.set(true);
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Batch<Integer> next() {
            int next = iter.next();
            return Batch.data(next, next, next, next, Collections.singletonList(next));
        }
    }

    @Test
    public void testOffsets() {
        MockBatchReader underlying = new MockBatchReader(Arrays.asList(10, 20, 30));
        assertEquals(10L, underlying.baseOffset());
        assertEquals(OptionalLong.empty(), underlying.lastOffset());
        TrackingBatchReader<Integer> tracking = new TrackingBatchReader<>(underlying);
        assertEquals(10L, tracking.baseOffset());
        assertEquals(OptionalLong.of(30), tracking.lastOffset());
    }

    @Test
    public void testClosePropagated() {
        MockBatchReader underlying = new MockBatchReader(Arrays.asList(1, 2, 3));
        assertFalse(underlying.closed.get());
        TrackingBatchReader<Integer> tracking = new TrackingBatchReader<>(underlying);
        assertFalse(underlying.closed.get());
        tracking.close();
        assertTrue(underlying.closed.get());
    }

    @Test
    public void testIteratesThroughUnderlying() {
        MockBatchReader underlying = new MockBatchReader(Arrays.asList(200, 300, 400, 500));
        assertTrue(underlying.hasNext());
        TrackingBatchReader<Integer> tracking = new TrackingBatchReader<>(underlying);
        assertFalse(underlying.hasNext());
        assertTrue(tracking.hasNext());
        assertEquals(200L, tracking.next().baseOffset());
        assertEquals(300L, tracking.next().baseOffset());
        assertEquals(400L, tracking.next().baseOffset());
        assertEquals(500L, tracking.next().baseOffset());
        assertFalse(tracking.hasNext());
    }
}

