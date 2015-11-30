/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.MeteredKeyValueStore;
import org.apache.kafka.streams.state.RocksDBStore;
import org.apache.kafka.streams.state.Serdes;

/**
 * A KTable storage. It stores all entries in a local RocksDB database.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class KTableStoreSupplier<K, V> implements StateStoreSupplier {

    private final String name;
    private final Serdes<K, V> serdes;
    private final Time time;

    protected KTableStoreSupplier(String name,
                                  Serializer<K> keySerializer, Deserializer<K> keyDeserializer,
                                  Serializer<V> valSerializer, Deserializer<V> valDeserializer,
                                  Time time) {
        this.name = name;
        this.serdes = new Serdes<>(name, keySerializer, keyDeserializer, valSerializer, valDeserializer);
        this.time = time;
    }

    public String name() {
        return name;
    }

    public StateStore get() {
        return new MeteredKeyValueStore<>(new RocksDBStore<>(name, serdes), serdes, "rocksdb-state", time).disableLogging();
    }

}
