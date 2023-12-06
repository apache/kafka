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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.VersionedRecord;

import java.util.function.Function;

public class MeteredMultiVersionedRangeQueryIterator<K, V> implements KeyValueIterator<K, VersionedRecord<V>> {

    private final KeyValueIterator<Bytes, VersionedRecord<byte[]>> iterator;
    private final Function<VersionedRecord<byte[]>, VersionedRecord<V>> valueDeserializer;
    private final StateSerdes<K, V> serdes;


    public MeteredMultiVersionedRangeQueryIterator(final KeyValueIterator<Bytes, VersionedRecord<byte[]>> iterator,
                                                   final Function<VersionedRecord<byte[]>, VersionedRecord<V>> valueDeserializer,
                                                   final StateSerdes<K, V> serdes) {
        this.iterator = iterator;
        this.valueDeserializer = valueDeserializer;
        this.serdes = serdes;
    }


    @Override
    public void close() {
        iterator.close();
    }

    @Override
    public K peekNextKey() {
        return serdes.keyFrom(iterator.peekNextKey().get());
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public KeyValue<K, VersionedRecord<V>> next() {
        final KeyValue<Bytes, VersionedRecord<byte[]>> keyValue = iterator.next();
        return KeyValue.pair(
                serdes.keyFrom(keyValue.key.get()),
                valueDeserializer.apply(keyValue.value));
    }


}


