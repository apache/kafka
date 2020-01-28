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

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.TimestampedSessionStore;

public class ReadOnlySessionStoreFacade<K, AGG> implements ReadOnlySessionStore<K, AGG> {
    protected final TimestampedSessionStore<K, AGG> inner;

    protected ReadOnlySessionStoreFacade(final TimestampedSessionStore<K, AGG> store) {
        inner = store;
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> fetch(final K key) {
        return new KeyValueIteratorFacade<>(inner.fetch(key));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> fetch(final K from, final K to) {
        return new KeyValueIteratorFacade<>(inner.fetch(from, to));
    }
}