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

import org.apache.kafka.streams.KeyValue;

import java.lang.reflect.Type;

/**
 * KTable is an abstraction of a change log stream.
 *
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public interface KTable<K, V> {

    /**
     * Explicitly specifies the information of the key type and the value type.
     *
     * @param keyType an instance of Type that represents the key type
     * @param valueType an instance of Type that represents the value type
     * @return the new instance of KTable with explicit type information
     */
    KTable<K, V> returns(Type keyType, Type valueType);

    /**
     * Explicitly specifies the information of the value type.
     *
     * @param valueType an instance of Type that represents the value type
     * @return the new instance of KTable with explicit type information
     */
    KTable<K, V> returnsValue(Type valueType);

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
     * The serializers/deserializers are determined from the key/value type infos of this stream.
     *
     * @param topic           the topic name
     * @return the instance of KTable that consumes the given topic
     */
    KTable<K, V> through(String topic);

    /**
     * Sends key-value to a topic using default serializers specified in the config.
     * The serializers/deserializers are determined from the key/value type infos of this stream.
     *
     * @param topic         the topic name
     */
    void to(String topic);

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
                                        String name);

    /**
     * Count number of records of this table by the selected key.
     *
     * @param selector the KeyValue mapper that select the aggregate key
     * @param name the name of the resulted table
     * @param <K1>   the key type of the aggregated table
     * @param <V1>   the value type of the aggregated table
     * @return the instance of KTable
     */
    <K1, V1> KTable<K1, Long> count(KeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                    String name);
}
