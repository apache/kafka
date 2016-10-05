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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SegmentIteratorTest {

    private final Segment segmentOne = new Segment("one", "one", 0);
    private final Segment segmentTwo = new Segment("two", "window", 1);
    private SegmentIterator<String, String> iterator;

    @Before
    public void before() {
        final MockProcessorContext context = new MockProcessorContext(null,
                                                                      TestUtils.tempDirectory(),
                                                                      Serdes.String(),
                                                                      Serdes.String(),
                                                                      new NoOpRecordCollector(),
                                                                      new ThreadCache(0));
        segmentOne.openDB(context);
        segmentTwo.openDB(context);

        final StateSerdes<String, String> serdes = new StateSerdes<>("name", Serdes.String(), Serdes.String());

        final SegmentIterator.KeyExtractor<String> keyExtractor = new SegmentIterator.KeyExtractor<String>() {
            @Override
            public String apply(final Bytes data) {
                return new String(data.get());
            }
        };

        final HasNextCondition hasNextCondition = new HasNextCondition() {
            @Override
            public boolean hasNext(final KeyValueIterator iterator) {
                return iterator.hasNext();
            }
        };


        final SegmentIterator.SegmentQuery segmentQuery = new SegmentIterator.SegmentQuery() {
            @Override
            public KeyValueIterator query(final KeyValueStore segment) {
                return segment.all();
            }
        };

        iterator = new SegmentIterator<>(serdes,
                                         Arrays.asList(segmentOne,
                                                       segmentTwo).iterator(),
                                         keyExtractor,
                                         hasNextCondition,
                                         segmentQuery);

    }

    @After
    public void closeSegments() {
        segmentOne.close();
        segmentTwo.close();
    }

    @Test
    public void shouldIterateOverSegments() throws Exception {
        segmentOne.put(Bytes.wrap("a".getBytes()), "1".getBytes());
        segmentOne.put(Bytes.wrap("b".getBytes()), "2".getBytes());
        segmentTwo.put(Bytes.wrap("c".getBytes()), "3".getBytes());
        segmentTwo.put(Bytes.wrap("d".getBytes()), "4".getBytes());

        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.peekNextKey());
        assertEquals(KeyValue.pair("a", "1"), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.peekNextKey());
        assertEquals(KeyValue.pair("b", "2"), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.peekNextKey());
        assertEquals(KeyValue.pair("c", "3"), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals("d", iterator.peekNextKey());
        assertEquals(KeyValue.pair("d", "4"), iterator.next());
    }

}