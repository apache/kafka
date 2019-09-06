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
package org.apache.kafka.test;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.SegmentedBytesStore;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SegmentedBytesStoreStub implements SegmentedBytesStore {
    private Map<Bytes, byte[]> store = new HashMap<>();
    public boolean fetchCalled;
    public boolean allUpToCalled;
    public boolean flushed;
    public boolean closed;
    public boolean initialized;
    public boolean removeCalled;
    public boolean putCalled;
    public boolean getCalled;

    @Override
    public String name() {
        return "";
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        initialized = true;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetch(final Bytes key, final long from, final long to) {
        return fetch(key, key, from, to);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo, final long from, final long to) {
        fetchCalled = true;
        return new KeyValueIteratorStub<>(Collections.<KeyValue<Bytes, byte[]>>emptyIterator());
    }
    
    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        fetchCalled = true;
        return new KeyValueIteratorStub<>(Collections.<KeyValue<Bytes, byte[]>>emptyIterator());
    }
    
    @Override
    public KeyValueIterator<Bytes, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        fetchCalled = true;
        return new KeyValueIteratorStub<>(Collections.<KeyValue<Bytes, byte[]>>emptyIterator());
    }

    @Override
    public void remove(final Bytes key) {
        store.put(key, null);
        removeCalled = true;
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        store.put(key, value);
        putCalled = true;
    }

    @Override
    public boolean isOpen() {
        return false;
    }


    @Override
    public byte[] get(final Bytes key) {
        getCalled = true;
        return store.get(key);
    }

    @Override
    public void flush() {
        flushed = true;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean persistent() {
        return false;
    }
}
