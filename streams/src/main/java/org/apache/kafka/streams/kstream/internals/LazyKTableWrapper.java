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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.InsufficientTypeInfoException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.lang.reflect.Type;

public abstract class LazyKTableWrapper<K, V> extends AbstractStream<K> implements KTable<K, V> {

    public LazyKTableWrapper(KStreamBuilder topology, Type keyType, Type valueType) {
        super(topology, "LAZY_KTABLE_WRAPPER", null, keyType, valueType);
    }

    protected abstract KTable<K, V> create(Type keyType, Type valueType);

    @Override
    public KTable<K, V> returns(Type keyType, Type valueType) {
        Type newKeyType = (keyType == null) ? this.keyType : keyType;
        Type newValueType = (valueType == null) ? this.valueType : valueType;

        if (newKeyType != null && newValueType != null) {
            return create(newKeyType, newValueType);
        } else {
            final LazyKTableWrapper<K, V> parent = this;
            return new LazyKTableWrapper<K, V>(topology, newKeyType, newValueType) {
                protected KTable<K, V> create(Type keyType, Type valueType) {
                    return parent.create(keyType, valueType);
                }
            };
        }
    }

    @Override
    public KTable<K, V> returnsValue(Type valueType) {
        return returns(this.keyType, valueType);
    }

    @Override
    public KTable<K, V> filter(Predicate<K, V> predicate) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public KTable<K, V> filterOut(Predicate<K, V> predicate) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <V1> KTable<K, V1> mapValues(ValueMapper<V, V1> mapper) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public KTable<K, V> through(String topic) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public void to(String topic) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public KStream<K, V> toStream() {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <V1, R> KTable<K, R> join(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <V1, R> KTable<K, R> outerJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <V1, R> KTable<K, R> leftJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <K1, V1> KTable<K1, V1> reduce(Reducer<V1> addReducer,
                                          Reducer<V1> removeReducer,
                                          KeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                          String name) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <K1, V1, T> KTable<K1, T> aggregate(Initializer<T> initializer,
                                               Aggregator<K1, V1, T> add,
                                               Aggregator<K1, V1, T> remove,
                                               KeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                               String name) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <K1, V1> KTable<K1, Long> count(KeyValueMapper<K, V, KeyValue<K1, V1>> selector, String name) {
        throw new InsufficientTypeInfoException();
    }

}
