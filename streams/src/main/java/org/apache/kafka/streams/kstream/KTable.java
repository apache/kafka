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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;

/**
 * KTable is an abstraction of a change log stream from a primary-keyed table.
 *
 * @param <K> Type of primary keys
 * @param <V> Type of value changes
 */
public interface KTable<K, V> {

    /**
     * Creates a new instance of KTable consists of all elements of this stream which satisfy a predicate
     *
     * @param predicate the instance of Predicate
     * @return the instance of KTable with only those elements that satisfy the predicate
     */
    KTable<K, V> filter(Predicate<K, V> predicate);

    /**
     * Creates a new instance of KTable consists all elements of this stream which do not satisfy a predicate
     *
     * @param predicate the instance of Predicate
     * @return the instance of KTable with only those elements that do not satisfy the predicate
     */
    KTable<K, V> filterOut(Predicate<K, V> predicate);

    /**
     * Creates a new instance of KTable by transforming each value in this stream into a different value in the new stream.
     *
     * @param mapper the instance of ValueMapper
     * @param <V1>   the value type of the new stream
     * @return the instance of KTable
     */
    <V1> KTable<K, V1> mapValues(ValueMapper<V, V1> mapper);

    /**
     * Sends key-value to a topic, also creates a new instance of KTable from the topic.
     * This is equivalent to calling to(topic) and table(topic).
     *
     * @param topic           the topic name
     * @return the instance of KTable that consumes the given topic
     */
    KTable<K, V> through(String topic);

    /**
     * Sends key-value to a topic, also creates a new instance of KTable from the topic.
     * This is equivalent to calling to(topic) and table(topic).
     *
     * @param topic           the topic name
     * @param keySerde   key serde used to send key-value pairs,
     *                        if not specified the default key serde defined in the configuration will be used
     * @param valSerde   value serde used to send key-value pairs,
     *                        if not specified the default value serde defined in the configuration will be used
     * @return the new stream that consumes the given topic
     */
    KTable<K, V> through(String topic, Serde<K> keySerde, Serde<V> valSerde);

    /**
     * Sends key-value to a topic using default serializers specified in the config.
     *
     * @param topic         the topic name
     */
    void to(String topic);

    /**
     * Sends key-value to a topic.
     *
     * @param topic    the topic name
     * @param keySerde key serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     * @param valSerde value serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     */
    void to(String topic, Serde<K> keySerde, Serde<V> valSerde);

    /**
     * Creates a new instance of KStream from this KTable
     *
     * @return the instance of KStream
     */
    KStream<K, V> toStream();

    /**
     * Combines values of this KTable with another KTable using Inner Join.
     *
     * @param other the instance of KTable joined with this stream
     * @param joiner ValueJoiner
     * @param <V1>   the value type of the other stream
     * @param <R>   the value type of the new stream
     * @return the instance of KTable
     */
    <V1, R> KTable<K, R> join(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner);

    /**
     * Combines values of this KTable with another KTable using Outer Join.
     *
     * @param other the instance of KTable joined with this stream
     * @param joiner ValueJoiner
     * @param <V1>   the value type of the other stream
     * @param <R>   the value type of the new stream
     * @return the instance of KTable
     */
    <V1, R> KTable<K, R> outerJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner);

    /**
     * Combines values of this KTable with another KTable using Left Join.
     *
     * @param other the instance of KTable joined with this stream
     * @param joiner ValueJoiner
     * @param <V1>   the value type of the other stream
     * @param <R>   the value type of the new stream
     * @return the instance of KTable
     */
    <V1, R> KTable<K, R> leftJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner);

    /**
     * Reduce values of this table by the selected key.
     *
     * @param addReducer the class of Reducer
     * @param removeReducer the class of Reducer
     * @param selector the KeyValue mapper that select the aggregate key
     * @param name the name of the resulted table
     * @param <K1>   the key type of the aggregated table
     * @param <V1>   the value type of the aggregated table
     * @return the instance of KTable
     */
    <K1, V1> KTable<K1, V1> reduce(Reducer<V1> addReducer,
                                   Reducer<V1> removeReducer,
                                   KeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                   Serde<K1> keySerde,
                                   Serde<V1> valueSerde,
                                   String name);

    /**
     * Aggregate values of this table by the selected key.
     *
     * @param initializer the class of Initializer
     * @param add the class of Aggregator
     * @param remove the class of Aggregator
     * @param selector the KeyValue mapper that select the aggregate key
     * @param name the name of the resulted table
     * @param <K1>   the key type of the aggregated table
     * @param <V1>   the value type of the aggregated table
     * @return the instance of KTable
     */
    <K1, V1, T> KTable<K1, T> aggregate(Initializer<T> initializer,
                                        Aggregator<K1, V1, T> add,
                                        Aggregator<K1, V1, T> remove,
                                        KeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                        Serde<K1> keySerde,
                                        Serde<V1> valueSerde,
                                        Serde<T> aggValueSerde,
                                        String name);

    /**
     * Count number of records of this table by the selected key.
     *
     * @param selector the KeyValue mapper that select the aggregate key
     * @param name the name of the resulted table
     * @param <K1>   the key type of the aggregated table
     * @return the instance of KTable
     */
    <K1> KTable<K1, Long> count(KeyValueMapper<K, V, K1> selector,
                                Serde<K1> keySerde,
                                Serde<V> valueSerde,
                                String name);
}
