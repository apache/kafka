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

import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.rocksdb.RocksIterator;

import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;

class RocksDbIterator extends AbstractIterator<KeyValue<Bytes, byte[]>> implements KeyValueIterator<Bytes, byte[]> {

    private final String storeName;
    private final RocksIterator iter;
    private final Set<KeyValueIterator<Bytes, byte[]>> openIterators;
    private final Consumer<RocksIterator> advanceIterator;

    private volatile boolean open = true;

    private KeyValue<Bytes, byte[]> next;

    RocksDbIterator(final String storeName,
                    final RocksIterator iter,
                    final Set<KeyValueIterator<Bytes, byte[]>> openIterators,
                    final boolean forward) {
        this.storeName = storeName;
        this.iter = iter;
        this.openIterators = openIterators;
        this.advanceIterator = forward ? RocksIterator::next : RocksIterator::prev;
    }

    @Override
    public synchronized boolean hasNext() {
        if (!open) {
            throw new InvalidStateStoreException(String.format("RocksDB iterator for store %s has closed", storeName));
        }
        return super.hasNext();
    }

    @Override
    public KeyValue<Bytes, byte[]> makeNext() {
        if (!iter.isValid()) {
            return allDone();
        } else {
            next = getKeyValue();
            advanceIterator.accept(iter);
            return next;
        }
    }

    private KeyValue<Bytes, byte[]> getKeyValue() {
        return new KeyValue<>(new Bytes(iter.key()), iter.value());
    }

    @Override
    public synchronized void close() {
        openIterators.remove(this);
        iter.close();
        open = false;
    }

    @Override
    public Bytes peekNextKey() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return next.key;
    }
}
