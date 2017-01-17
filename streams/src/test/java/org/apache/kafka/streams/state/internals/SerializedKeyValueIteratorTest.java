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
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SerializedKeyValueIteratorTest {

    private final StateSerdes<String, String> serdes = new StateSerdes<>("blah", Serdes.String(), Serdes.String());
    private final Iterator<KeyValue<Bytes, byte[]>> iterator
            = Arrays.asList(KeyValue.pair(Bytes.wrap("hi".getBytes()), "there".getBytes()),
                            KeyValue.pair(Bytes.wrap("hello".getBytes()), "world".getBytes()))
            .iterator();
    private final DelegatingPeekingKeyValueIterator<Bytes, byte[]> peeking
            = new DelegatingPeekingKeyValueIterator<>("store", new KeyValueIteratorStub<>(iterator));
    private final SerializedKeyValueIterator<String, String> serializedKeyValueIterator
            = new SerializedKeyValueIterator<>(peeking, serdes);

    @Test
    public void shouldReturnTrueOnHasNextWhenMoreResults() {
        assertTrue(serializedKeyValueIterator.hasNext());
    }

    @Test
    public void shouldReturnNextValueWhenItExists() throws Exception {
        assertThat(serializedKeyValueIterator.next(), equalTo(KeyValue.pair("hi", "there")));
        assertThat(serializedKeyValueIterator.next(), equalTo(KeyValue.pair("hello", "world")));
    }

    @Test
    public void shouldReturnFalseOnHasNextWhenNoMoreResults() throws Exception {
        advanceIteratorToEnd();
        assertFalse(serializedKeyValueIterator.hasNext());
    }

    @Test
    public void shouldThrowNoSuchElementOnNextWhenIteratorExhausted() throws Exception {
        advanceIteratorToEnd();
        try {
            serializedKeyValueIterator.next();
            fail("Expected NoSuchElementException on exhausted iterator");
        } catch (final NoSuchElementException nse) {
            // pass
        }
    }

    @Test
    public void shouldPeekNextKey() throws Exception {
        assertThat(serializedKeyValueIterator.peekNextKey(), equalTo("hi"));
        serializedKeyValueIterator.next();
        assertThat(serializedKeyValueIterator.peekNextKey(), equalTo("hello"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowUnsupportedOperationOnRemove() throws Exception {
        serializedKeyValueIterator.remove();
    }

    private void advanceIteratorToEnd() {
        serializedKeyValueIterator.next();
        serializedKeyValueIterator.next();
    }


}