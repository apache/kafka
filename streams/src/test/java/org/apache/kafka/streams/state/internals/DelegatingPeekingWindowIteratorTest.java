/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DelegatingPeekingWindowIteratorTest {

    private static final long DEFAULT_TIMESTAMP = 0L;
    private WindowStore<String, String> store;

    @Before
    public void setUp() throws Exception {
        store = new RocksDBWindowStore<>("test", 30000, 3, false, Serdes.String(), Serdes.String());
        final MockProcessorContext context = new MockProcessorContext(null, TestUtils.tempDirectory(), null, null, (RecordCollector) null, null);
        context.setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, 0, 0, "topic"));
        store.init(context, store);
    }

    @Test
    public void shouldPeekNext() throws Exception {
        final KeyValue<Long, String> expected = KeyValue.pair(DEFAULT_TIMESTAMP, "A");
        store.put("A", "A");
        final DelegatingPeekingWindowIterator<String> peekingIterator = new DelegatingPeekingWindowIterator<>(store.fetch("A", DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP));
        assertEquals(expected, peekingIterator.peekNext());
        assertEquals(expected, peekingIterator.peekNext());
        assertTrue(peekingIterator.hasNext());
    }

    @Test
    public void shouldPeekAndIterate() throws Exception {
        final List<KeyValue<Long, String>> expected = new ArrayList<>();
        for (long t = 0; t < 50; t += 10) {
            store.put("a", String.valueOf(t), t);
            expected.add(KeyValue.pair(t, String.valueOf(t)));
        }
        final DelegatingPeekingWindowIterator<String> peekingIterator = new DelegatingPeekingWindowIterator<>(store.fetch("a", 0, 50));
        int index = 0;
        while (peekingIterator.hasNext()) {
            final KeyValue<Long, String> peekNext = peekingIterator.peekNext();
            final KeyValue<Long, String> key = peekingIterator.next();
            assertEquals(expected.get(index), peekNext);
            assertEquals(expected.get(index), key);
            index++;
        }
        assertEquals(expected.size(), index);
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldThrowNoSuchElementWhenNoMoreItemsLeftAndNextCalled() throws Exception {
        final DelegatingPeekingWindowIterator<String> peekingIterator = new DelegatingPeekingWindowIterator<>(store.fetch("b", 10, 10));
        peekingIterator.next();
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldThrowNoSuchElementWhenNoMoreItemsLeftAndPeekNextCalled() throws Exception {
        final DelegatingPeekingWindowIterator<String> peekingIterator = new DelegatingPeekingWindowIterator<>(store.fetch("b", 10, 10));
        peekingIterator.peekNext();
    }


}