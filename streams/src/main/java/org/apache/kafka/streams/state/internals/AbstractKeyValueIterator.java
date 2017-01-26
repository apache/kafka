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

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;

abstract class AbstractKeyValueIterator<K, V> implements KeyValueIterator<K, V> {
    private final String name;
    private volatile boolean open;

    AbstractKeyValueIterator(final String name) {
        this.name = name;
        this.open = true;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported.");
    }

    @Override
    public synchronized void close() {
        open = false;
    }

    void validateIsOpen() {
        if (!open) {
            throw new InvalidStateStoreException(String.format("Iterator %s has closed", name));
        }
    }
}
