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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.test.InMemoryKeyValueStore;
import org.apache.kafka.test.KTableValueGetterStub;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.NoOpProcessorContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KeyValueStoreJoinViewTest {

    private final InMemoryKeyValueStore<Long, String> store = new InMemoryKeyValueStore<>("store");
    private final KTableValueGetterStub<String, String> valueGetter = new KTableValueGetterStub<>();
    private final KeyValueStoreJoinView<Long, String, String, String, String> join
            = new KeyValueStoreJoinView<>("view",
                                          store,
                                          valueGetter,
                                          MockKeyValueMapper.<Long, String>SelectValueMapper(),
                                          MockValueJoiner.STRING_JOINER,
                                          false);

    private final KeyValueStoreJoinView<Long, String, String, String, String> leftJoin
            = new KeyValueStoreJoinView<>("view",
                                          store,
                                          valueGetter,
                                          MockKeyValueMapper.<Long, String>SelectValueMapper(),
                                          MockValueJoiner.STRING_JOINER,
                                          true);

    @Before
    public void before() {
        join.init(new NoOpProcessorContext(), join);
        leftJoin.init(new NoOpProcessorContext(), leftJoin);
        store.put(1L, "A");
        store.put(2L, "B");
        store.put(3L, "C");
        store.put(4L, "D");
        valueGetter.put("A", "a");
        valueGetter.put("B", "b");
        valueGetter.put("C", "c");
    }

    @Test
    public void shouldGetResultOfJoin() throws Exception {
        assertEquals("A+a", join.get(1L));
        assertEquals("B+b", join.get(2L));
        assertEquals("C+c", join.get(3L));
        assertNull(join.get(4L));
    }

    @Test
    public void shouldGetResultOfLeftJoin() throws Exception {
        assertEquals("A+a", leftJoin.get(1L));
        assertEquals("B+b", leftJoin.get(2L));
        assertEquals("C+c", leftJoin.get(3L));
        assertEquals("D+null", leftJoin.get(4L));
    }

    @Test
    public void shouldReturnKeyValueIteratorWithResultOfJoin() throws Exception {
        assertEquals(Arrays.asList(
                KeyValue.pair(1L, "A+a"),
                KeyValue.pair(2L, "B+b"),
                KeyValue.pair(3L, "C+c")
        ), toList(join.all()));
    }

    @Test
    public void shouldReturnKeyValueIteratorWithResultOfLeftJoin() throws Exception {
        assertEquals(Arrays.asList(
                KeyValue.pair(1L, "A+a"),
                KeyValue.pair(2L, "B+b"),
                KeyValue.pair(3L, "C+c"),
                KeyValue.pair(4L, "D+null")
        ), toList(leftJoin.all()));
    }

    @Test
    public void shouldReturnKeyValueIteratorWithResultOfRangeJoin() throws Exception {
        assertEquals(Arrays.asList(
                KeyValue.pair(1L, "A+a"),
                KeyValue.pair(2L, "B+b")
        ), toList(join.range(0L, 2L)));
    }

    @Test
    public void shouldReturnKeyValueIteratorWithResultOfRangeLeftJoin() throws Exception {
        assertEquals(Arrays.asList(
                KeyValue.pair(3L, "C+c"),
                KeyValue.pair(4L, "D+null")
        ), toList(leftJoin.range(3L, 10L)));
    }

}