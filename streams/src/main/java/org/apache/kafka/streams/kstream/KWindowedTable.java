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

/**
 * KWindowedTable is an abstraction of a change log stream on a window basis.
 *
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @param <W> the type of window definition
 */
public interface KWindowedTable<K, V, W extends Window> {

    /**
     * Creates a new instance of KTable consists of all elements of this stream which satisfy a predicate
     *
     * @param predicate the instance of Predicate
     * @return the instance of KTable with only those elements that satisfy the predicate
     */
    KWindowedTable<K, V, W> filter(Predicate<K, V> predicate);

    /**
     * Creates a new instance of KTable consists all elements of this stream which do not satisfy a predicate
     *
     * @param predicate the instance of Predicate
     * @return the instance of KTable with only those elements that do not satisfy the predicate
     */
    KWindowedTable<K, V, W> filterOut(Predicate<K, V> predicate);

    /**
     * Creates a new instance of KTable by transforming each value in this stream into a different value in the new stream.
     *
     * @param mapper the instance of ValueMapper
     * @param <V1>   the value type of the new stream
     * @return the instance of KTable
     */
    <V1> KWindowedTable<K, V1, W> mapValues(ValueMapper<V, V1> mapper);

    /**
     * Transform this windowed table into a stream
     *
     * @param windowMapper the class of WindowMapper
     * @return the instance of KStream that contains transformed keys and values
     */
    <K1, V1> KStream<K1, V1> toStream(WindowMapper<K, V, W, K1, V1> windowMapper);

    /**
     * Combines values of this KWindowedTable with another KWindowedTable using Inner Join.
     * The window definition of the other KWindowedTable must be the same.
     *
     * @param other the instance of KTable joined with this stream
     * @param joiner ValueJoiner
     * @param <V1>   the value type of the other stream
     * @param <V2>   the value type of the new stream
     * @return the instance of the KWindowedTable
     */
    <V1, V2> KWindowedTable<K, V2, W> join(KWindowedTable<K, V1, W> other, ValueJoiner<V, V1, V2> joiner);

    /**
     * Combines values of this KWindowedTable with another KWindowedTable using Outer Join.
     * The window definition of the other WindowedKtable must be the same.
     *
     * @param other the instance of KTable joined with this stream
     * @param joiner ValueJoiner
     * @param <V1>   the value type of the other stream
     * @param <V2>   the value type of the new stream
     * @return the instance of KStream
     */
    <V1, V2> KWindowedTable<K, V2, W> outerJoin(KWindowedTable<K, V1, W> other, ValueJoiner<V, V1, V2> joiner);

    /**
     * Combines values of this KWindowedTable with another KWindowedTable using Left Join.
     * The window definition of the other KWindowedTable must be the same.
     *
     * @param other the instance of KTable joined with this stream
     * @param joiner ValueJoiner
     * @param <V1>   the value type of the other stream
     * @param <V2>   the value type of the new stream
     * @return the instance of KStream
     */
    <V1, V2> KWindowedTable<K, V2, W> leftJoin(KWindowedTable<K, V1, W> other, ValueJoiner<V, V1, V2> joiner);

    /**
     * Combines values of this KWindowedTable with another KWindowedTable using Left Join.
     * The window type of the other KWindowedTable must be the same.
     *
     * @param other the instance of KTable joined with this stream
     * @param timeDifference difference in window definition
     * @param joiner ValueJoiner
     * @param <V1>   the value type of the other stream
     * @param <V2>   the value type of the new stream
     * @return the instance of KStream
     */
    <V1, V2> KWindowedTable<K, V2, W> leftJoin(KWindowedTable<K, V1, W> other, long timeDifference, ValueJoiner<V, V1, V2> joiner);
}
