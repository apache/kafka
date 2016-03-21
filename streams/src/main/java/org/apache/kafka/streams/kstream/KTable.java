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

package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;

/**
 * KTable is an abstraction of a <i>changelog stream</i> from a primary-keyed table.
 *
 * @param <K> Type of primary keys
 * @param <V> Type of value changes
 */
@InterfaceStability.Unstable
public interface KTable<K, V> {

    /**
     * Create a new instance of {@link KTable} that consists of all elements of this stream which satisfy a predicate.
     *
     * @param predicate     the instance of {@link Predicate}
     */
    KTable<K, V> filter(Predicate<K, V> predicate);

    /**
     * Create a new instance of {@link KTable} that consists all elements of this stream which do not satisfy a predicate
     *
     * @param predicate     the instance of {@link Predicate}
     */
    KTable<K, V> filterOut(Predicate<K, V> predicate);

    /**
     * Create a new instance of {@link KTable} by transforming the value of each element in this stream into a new value in the new stream.
     *
     * @param mapper        the instance of {@link ValueMapper}
     * @param <V1>          the value type of the new stream
     */
    <V1> KTable<K, V1> mapValues(ValueMapper<V, V1> mapper);

    /**
     * Materialize this stream to a topic, also creates a new instance of {@link KTable} from the topic
     * using default serializers and deserializers.
     * This is equivalent to calling {@link #to(String)} and {@link org.apache.kafka.streams.kstream.KStreamBuilder#table(String)}.
     *
     * @param topic         the topic name
     */
    KTable<K, V> through(String topic);

    /**
     * Materialize this stream to a topic, also creates a new instance of {@link KTable} from the topic.
     * This is equivalent to calling {@link #to(Serde, Serde, String)} and
     * {@link org.apache.kafka.streams.kstream.KStreamBuilder#table(Serde, Serde, String)}.
     *
     * @param keySerde  key serde used to send key-value pairs,
     *                  if not specified the default key serde defined in the configuration will be used
     * @param valSerde  value serde used to send key-value pairs,
     *                  if not specified the default value serde defined in the configuration will be used
     * @param topic     the topic name
     */
    KTable<K, V> through(Serde<K> keySerde, Serde<V> valSerde, String topic);

    /**
     * Materialize this stream to a topic using default serializers specified in the config.
     *
     * @param topic         the topic name
     */
    void to(String topic);

    /**
     * Materialize this stream to a topic.
     *
     * @param keySerde  key serde used to send key-value pairs,
     *                  if not specified the default serde defined in the configs will be used
     * @param valSerde  value serde used to send key-value pairs,
     *                  if not specified the default serde defined in the configs will be used
     * @param topic     the topic name
     */
    void to(Serde<K> keySerde, Serde<V> valSerde, String topic);

    /**
     * Convert this stream to a new instance of {@link KStream}.
     */
    KStream<K, V> toStream();

    /**
     * Combine values of this stream with another {@link KTable} stream's elements of the same key using Inner Join.
     *
     * @param other         the instance of {@link KTable} joined with this stream
     * @param joiner        the instance of {@link ValueJoiner}
     * @param <V1>          the value type of the other stream
     * @param <R>           the value type of the new stream
     */
    <V1, R> KTable<K, R> join(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner);

    /**
     * Combine values of this stream with another {@link KTable} stream's elements of the same key using Outer Join.
     *
     * @param other         the instance of {@link KTable} joined with this stream
     * @param joiner        the instance of {@link ValueJoiner}
     * @param <V1>          the value type of the other stream
     * @param <R>           the value type of the new stream
     */
    <V1, R> KTable<K, R> outerJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner);

    /**
     * Combine values of this stream with another {@link KTable} stream's elements of the same key using Left Join.
     *
     * @param other         the instance of {@link KTable} joined with this stream
     * @param joiner        the instance of {@link ValueJoiner}
     * @param <V1>          the value type of the other stream
     * @param <R>           the value type of the new stream
     */
    <V1, R> KTable<K, R> leftJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner);

    /**
     * Combine updating values of this stream by the selected key into a new instance of {@link KTable}.
     *
     * @param adder             the instance of {@link Reducer} for addition
     * @param subtractor        the instance of {@link Reducer} for subtraction
     * @param selector          the instance of {@link KeyValueMapper} that select the aggregate key
     * @param keySerde          key serdes for materializing the aggregated table,
     *                          if not specified the default serdes defined in the configs will be used
     * @param valueSerde        value serdes for materializing the aggregated table,
     *                          if not specified the default serdes defined in the configs will be used
     * @param name              the name of the resulted {@link KTable}
     * @param <K1>              the key type of the aggregated {@link KTable}
     * @param <V1>              the value type of the aggregated {@link KTable}
     */
    <K1, V1> KTable<K1, V1> reduce(Reducer<V1> adder,
                                   Reducer<V1> subtractor,
                                   KeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                   Serde<K1> keySerde,
                                   Serde<V1> valueSerde,
                                   String name);

    /**
     * Combine updating values of this stream by the selected key into a new instance of {@link KTable}
     * using default serializers and deserializers.
     *
     * @param adder         the instance of {@link Reducer} for addition
     * @param subtractor    the instance of {@link Reducer} for subtraction
     * @param selector      the instance of {@link KeyValueMapper} that select the aggregate key
     * @param name          the name of the resulted {@link KTable}
     * @param <K1>          the key type of the aggregated {@link KTable}
     * @param <V1>          the value type of the aggregated {@link KTable}
     */
    <K1, V1> KTable<K1, V1> reduce(Reducer<V1> adder,
                                   Reducer<V1> subtractor,
                                   KeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                   String name);

    /**
     * Aggregate updating values of this stream by the selected key into a new instance of {@link KTable}.
     *
     * @param initializer   the instance of {@link Initializer}
     * @param adder         the instance of {@link Aggregator} for addition
     * @param substractor   the instance of {@link Aggregator} for subtraction
     * @param selector      the instance of {@link KeyValueMapper} that select the aggregate key
     * @param keySerde      key serdes for materializing this stream and the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param valueSerde    value serdes for materializing this stream,
     *                      if not specified the default serdes defined in the configs will be used
     * @param aggValueSerde value serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param name          the name of the resulted table
     * @param <K1>          the key type of this {@link KTable}
     * @param <V1>          the value type of this {@link KTable}
     * @param <T>           the value type of the aggregated {@link KTable}
     */
    <K1, V1, T> KTable<K1, T> aggregate(Initializer<T> initializer,
                                        Aggregator<K1, V1, T> adder,
                                        Aggregator<K1, V1, T> substractor,
                                        KeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                        Serde<K1> keySerde,
                                        Serde<V1> valueSerde,
                                        Serde<T> aggValueSerde,
                                        String name);

    /**
     * Aggregate updating values of this stream by the selected key into a new instance of {@link KTable}
     * using default serializers and deserializers.
     *
     * @param initializer   the instance of {@link Initializer}
     * @param adder         the instance of {@link Aggregator} for addition
     * @param substractor   the instance of {@link Aggregator} for subtraction
     * @param selector      the instance of {@link KeyValueMapper} that select the aggregate key
     * @param name          the name of the resulted {@link KTable}
     * @param <K1>          the key type of the aggregated {@link KTable}
     * @param <V1>          the value type of the aggregated {@link KTable}
     * @param <T>           the value type of the aggregated {@link KTable}
     */
    <K1, V1, T> KTable<K1, T> aggregate(Initializer<T> initializer,
                                        Aggregator<K1, V1, T> adder,
                                        Aggregator<K1, V1, T> substractor,
                                        KeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                        String name);

    /**
     * Count number of records of this stream by the selected key into a new instance of {@link KTable}.
     *
     * @param selector      the instance of {@link KeyValueMapper} that select the aggregate key
     * @param keySerde      key serdes for materializing this stream,
     *                      if not specified the default serdes defined in the configs will be used
     * @param valueSerde    value serdes for materializing this stream,
     *                      if not specified the default serdes defined in the configs will be used
     * @param name          the name of the resulted table
     * @param <K1>          the key type of the aggregated {@link KTable}
     */
    <K1> KTable<K1, Long> count(KeyValueMapper<K, V, K1> selector,
                                Serde<K1> keySerde,
                                Serde<V> valueSerde,
                                String name);

    /**
     * Count number of records of this stream by the selected key into a new instance of {@link KTable}
     * using default serializers and deserializers.
     *
     * @param selector      the instance of {@link KeyValueMapper} that select the aggregate key
     * @param name          the name of the resulted {@link KTable}
     * @param <K1>          the key type of the aggregated {@link KTable}
     */
    <K1> KTable<K1, Long> count(KeyValueMapper<K, V, K1> selector, String name);
}
