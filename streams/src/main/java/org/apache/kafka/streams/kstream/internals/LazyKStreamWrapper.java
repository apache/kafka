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
import org.apache.kafka.streams.kstream.AggregatorSupplier;
import org.apache.kafka.streams.kstream.InsufficientTypeInfoException;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.KeyValueToDoubleMapper;
import org.apache.kafka.streams.kstream.KeyValueToIntMapper;
import org.apache.kafka.streams.kstream.KeyValueToLongMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Set;

public abstract class LazyKStreamWrapper<K, V> extends AbstractStream<K> implements KStream<K, V> {

    public LazyKStreamWrapper(KStreamBuilder topology, Set<String> sourceNodes, Type keyType, Type valueType) {
        super(topology, "LAZY_KSTREAM_WRAPPER", sourceNodes, keyType, valueType);
    }

    protected abstract KStreamImpl<K, V> create(Type keyType, Type valueType);

    @Override
    public KStream<K, V> returns(Type keyType, Type valueType) {
        Type newKeyType = (keyType == null) ? this.keyType : keyType;
        Type newValueType = (valueType == null) ? this.valueType : valueType;

        if (newKeyType != null && newValueType != null) {
            return create(keyType == null ? this.keyType : keyType, valueType == null ? this.valueType : valueType);
        } else {
            final LazyKStreamWrapper<K, V> parent = this;
            return new LazyKStreamWrapper<K, V>(topology, sourceNodes, newKeyType, newValueType) {
                protected KStreamImpl<K, V> create(Type keyType, Type valueType) {
                    return parent.create(keyType, valueType);
                }
            };
        }
    }

    @Override
    public KStream<K, V> filter(Predicate<K, V> predicate) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public KStream<K, V> filterOut(Predicate<K, V> predicate) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <K1, V1> KStream<K1, V1> map(KeyValueMapper<K, V, KeyValue<K1, V1>> mapper) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <V1> KStream<K, V1> mapValues(ValueMapper<V, V1> mapper) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K, V, Iterable<KeyValue<K1, V1>>> mapper) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <V1> KStream<K, V1> flatMapValues(ValueMapper<V, Iterable<V1>> mapper) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public KStream<K, V>[] branch(Predicate<K, V>... predicates) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public KStream<K, V> through(String topic, Serializer<K> keySerializer, Serializer<V> valSerializer, Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public KStream<K, V> through(String topic) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public void to(String topic) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public void to(String topic, Serializer<K> keySerializer, Serializer<V> valSerializer) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <K1, V1> KStream<K1, V1> transform(TransformerSupplier<K, V, KeyValue<K1, V1>> transformerSupplier, String... stateStoreNames) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <V1> KStream<K, V1> transformValues(ValueTransformerSupplier<V, V1> valueTransformerSupplier, String... stateStoreNames) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public void process(ProcessorSupplier<K, V> processorSupplier, String... stateStoreNames) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <V1, R> KStream<K, R> join(KStream<K, V1> other, ValueJoiner<V, V1, R> joiner, JoinWindows windows) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <V1, R> KStream<K, R> outerJoin(KStream<K, V1> other, ValueJoiner<V, V1, R> joiner, JoinWindows windows) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <V1, R> KStream<K, R> leftJoin(KStream<K, V1> other, ValueJoiner<V, V1, R> joiner, JoinWindows windows) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <V1, R> KStream<K, R> leftJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <T, W extends Window> KTable<Windowed<K>, T> aggregateByKey(AggregatorSupplier<K, V, T> aggregatorSupplier, Windows<W> windows, Serializer<K> keySerializer, Serializer<T> aggValueSerializer, Deserializer<K> keyDeserializer, Deserializer<T> aggValueDeserializer) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <W extends Window> KTable<Windowed<K>, Long> sumByKey(KeyValueToLongMapper<K, V> valueSelector, Windows<W> windows, Serializer<K> keySerializer, Deserializer<K> keyDeserializer) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <W extends Window> KTable<Windowed<K>, Integer> sumByKey(KeyValueToIntMapper<K, V> valueSelector, Windows<W> windows, Serializer<K> keySerializer, Deserializer<K> keyDeserializer) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <W extends Window> KTable<Windowed<K>, Double> sumByKey(KeyValueToDoubleMapper<K, V> valueSelector, Windows<W> windows, Serializer<K> keySerializer, Deserializer<K> keyDeserializer) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <W extends Window> KTable<Windowed<K>, Long> countByKey(Windows<W> windows, Serializer<K> keySerializer, Deserializer<K> keyDeserializer) {
        throw new InsufficientTypeInfoException();
    }

    @Override
    public <W extends Window, V1 extends Comparable<V1>> KTable<Windowed<K>, Collection<V1>> topKByKey(int k, KeyValueMapper<K, V, V1> valueSelector, Windows<W> windows, Serializer<K> keySerializer, Serializer<V1> aggValueSerializer, Deserializer<K> keyDeserializer, Deserializer<V1> aggValueDeserializer) {
        throw new InsufficientTypeInfoException();
    }

}
