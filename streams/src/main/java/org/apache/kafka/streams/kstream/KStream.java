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
import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * KStream is an abstraction of a <i>record stream</i> in key-value pairs.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
@InterfaceStability.Unstable
public interface KStream<K, V> {

    /**
     * Create a new instance of {@link KStream} consists of all elements of this stream which satisfy a predicate.
     *
     * @param predicate     the instance of {@link Predicate}
     */
    KStream<K, V> filter(Predicate<K, V> predicate);

    /**
     * Create a new instance of KStream consists all elements of this stream which do not satisfy a predicate.
     *
     * @param predicate     the instance of {@link Predicate}
     */
    KStream<K, V> filterOut(Predicate<K, V> predicate);

    /**
     * Create a new instance of {@link KStream} by transforming each element in this stream into a different element in the new stream.
     *
     * @param mapper        the instance of {@link KeyValueMapper}
     * @param <K1>          the key type of the new stream
     * @param <V1>          the value type of the new stream
     */
    <K1, V1> KStream<K1, V1> map(KeyValueMapper<K, V, KeyValue<K1, V1>> mapper);

    /**
     * Create a new instance of {@link KStream} by transforming each value in this stream into a different value in the new stream.
     *
     * @param mapper        the instance of {@link ValueMapper}
     * @param <V1>          the value type of the new stream
     */
    <V1> KStream<K, V1> mapValues(ValueMapper<V, V1> mapper);

    /**
     * Create a new instance of {@link KStream} by transforming each element in this stream into zero or more elements in the new stream.
     *
     * @param mapper        the instance of {@link KeyValueMapper}
     * @param <K1>          the key type of the new stream
     * @param <V1>          the value type of the new stream
     */
    <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K, V, Iterable<KeyValue<K1, V1>>> mapper);

    /**
     * Create a new instance of {@link KStream} by transforming each value in this stream into zero or more values in the new stream.
     *
     * @param processor     the instance of {@link ValueMapper}
     * @param <V1>          the value type of the new stream
     */
    <V1> KStream<K, V1> flatMapValues(ValueMapper<V, Iterable<V1>> processor);

    /**
     * Create an array of {@link KStream} from this stream. Each stream in the array corresponds to a predicate in
     * supplied predicates in the same order. Predicates are evaluated in order. An element is streamed to
     * a corresponding stream for the first predicate is evaluated true.
     * An element will be dropped if none of the predicates evaluate true.
     *
     * @param predicates    the ordered list of {@link Predicate} instances
     */
    KStream<K, V>[] branch(Predicate<K, V>... predicates);

    /**
     * Materialize this stream to a topic, also creates a new instance of {@link KStream} from the topic
     * using default serializers and deserializers.
     * This is equivalent to calling {@link #to(String)} and {@link org.apache.kafka.streams.kstream.KStreamBuilder#stream(String...)}.
     *
     * @param topic     the topic name
     */
    KStream<K, V> through(String topic);

    /**
     * Materialize this stream to a topic, also creates a new instance of {@link KStream} from the topic.
     * This is equivalent to calling {@link #to(Serde, Serde, String)} and
     * {@link org.apache.kafka.streams.kstream.KStreamBuilder#stream(Serde, Serde, String...)}.
     *
     * @param keySerde  key serde used to send key-value pairs,
     *                  if not specified the default key serde defined in the configuration will be used
     * @param valSerde  value serde used to send key-value pairs,
     *                  if not specified the default value serde defined in the configuration will be used
     * @param topic     the topic name
     */
    KStream<K, V> through(Serde<K> keySerde, Serde<V> valSerde, String topic);

    /**
     * Materialize this stream to a topic using default serializers specified in the config.
     *
     * @param topic     the topic name
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
     * Create a new {@link KStream} instance by applying a stateful transformation to all elements in this stream.
     *
     * @param transformerSupplier   the instance of {@link TransformerSupplier} that generates stateful processors
     * @param stateStoreNames       the names of the state store used by the processor
     */
    <K1, V1> KStream<K1, V1> transform(TransformerSupplier<K, V, KeyValue<K1, V1>> transformerSupplier, String... stateStoreNames);

    /**
     * Create a new {@link KStream} instance by applying a stateful transformation to all values in this stream.
     *
     * @param valueTransformerSupplier  the instance of {@link ValueTransformerSupplier} that generates stateful processors
     * @param stateStoreNames           the names of the state store used by the processor
     */
    <R> KStream<K, R> transformValues(ValueTransformerSupplier<V, R> valueTransformerSupplier, String... stateStoreNames);

    /**
     * Process all elements in this stream by applying a {@link org.apache.kafka.streams.processor.Processor}.
     *
     * @param processorSupplier         the supplier of {@link ProcessorSupplier} that generates processors
     * @param stateStoreNames           the names of the state store used by the processor
     */
    void process(ProcessorSupplier<K, V> processorSupplier, String... stateStoreNames);

    /**
     * Combine values of this stream with another {@link KStream} using windowed Inner Join.
     *
     * @param otherStream       the instance of {@link KStream} joined with this stream
     * @param joiner            the instance of {@link ValueJoiner}
     * @param windows           the specification of the {@link JoinWindows}
     * @param keySerde          key serdes for materializing both streams,
     *                          if not specified the default serdes defined in the configs will be used
     * @param thisValueSerde    value serdes for materializing this stream,
     *                          if not specified the default serdes defined in the configs will be used
     * @param otherValueSerde   value serdes for materializing the other stream,
     *                          if not specified the default serdes defined in the configs will be used
     * @param <V1>              the value type of the other stream
     * @param <R>               the value type of the new stream
     */
    <V1, R> KStream<K, R> join(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows,
            Serde<K> keySerde,
            Serde<V> thisValueSerde,
            Serde<V1> otherValueSerde);

    /**
     * Combine values of this stream with another {@link KStream} using windowed Inner Join
     * with default serializers and deserializers.
     *
     * @param otherStream   the instance of {@link KStream} joined with this stream
     * @param joiner        the instance of {@link ValueJoiner}
     * @param windows       the specification of the {@link JoinWindows}
     * @param <V1>          the value type of the other stream
     * @param <R>           the value type of the new stream
     */
    <V1, R> KStream<K, R> join(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows);

    /**
     * Combine values of this stream with another {@link KStream} using windowed Outer Join.
     *
     * @param otherStream       the instance of {@link KStream} joined with this stream
     * @param joiner            the instance of {@link ValueJoiner}
     * @param windows           the specification of the {@link JoinWindows}
     * @param keySerde          key serdes for materializing both streams,
     *                          if not specified the default serdes defined in the configs will be used
     * @param thisValueSerde    value serdes for materializing this stream,
     *                          if not specified the default serdes defined in the configs will be used
     * @param otherValueSerde   value serdes for materializing the other stream,
     *                          if not specified the default serdes defined in the configs will be used
     * @param <V1>              the value type of the other stream
     * @param <R>               the value type of the new stream
     */
    <V1, R> KStream<K, R> outerJoin(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows,
            Serde<K> keySerde,
            Serde<V> thisValueSerde,
            Serde<V1> otherValueSerde);

    /**
     * Combine values of this stream with another {@link KStream} using windowed Outer Join
     * with default serializers and deserializers.
     *
     * @param otherStream   the instance of {@link KStream} joined with this stream
     * @param joiner        the instance of {@link ValueJoiner}
     * @param windows       the specification of the {@link JoinWindows}
     * @param <V1>          the value type of the other stream
     * @param <R>           the value type of the new stream
     */
    <V1, R> KStream<K, R> outerJoin(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows);

    /**
     * Combine values of this stream with another {@link KStream} using windowed Left Join.
     *
     * @param otherStream       the instance of {@link KStream} joined with this stream
     * @param joiner            the instance of {@link ValueJoiner}
     * @param windows           the specification of the {@link JoinWindows}
     * @param keySerde          key serdes for materializing the other stream,
     *                          if not specified the default serdes defined in the configs will be used
     * @param otherValueSerde   value serdes for materializing the other stream,
     *                          if not specified the default serdes defined in the configs will be used
     * @param <V1>              the value type of the other stream
     * @param <R>               the value type of the new stream
     */
    <V1, R> KStream<K, R> leftJoin(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows,
            Serde<K> keySerde,
            Serde<V1> otherValueSerde);

    /**
     * Combine values of this stream with another {@link KStream} using windowed Left Join
     * with default serializers and deserializers.
     *
     * @param otherStream   the instance of {@link KStream} joined with this stream
     * @param joiner        the instance of {@link ValueJoiner}
     * @param windows       the specification of the {@link JoinWindows}
     * @param <V1>          the value type of the other stream
     * @param <R>           the value type of the new stream
     */
    <V1, R> KStream<K, R> leftJoin(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows);

    /**
     * Combine values of this stream with {@link KTable} using non-windowed Left Join.
     *
     * @param table     the instance of {@link KTable} joined with this stream
     * @param joiner    the instance of {@link ValueJoiner}
     * @param <V1>      the value type of the table
     * @param <V2>      the value type of the new stream
     */
    <V1, V2> KStream<K, V2> leftJoin(KTable<K, V1> table, ValueJoiner<V, V1, V2> joiner);

    /**
     * Combine values of this stream by key on a window basis into a new instance of windowed {@link KTable}.
     *
     * @param reducer           the instance of {@link Reducer}
     * @param windows           the specification of the aggregation {@link Windows}
     * @param keySerde          key serdes for materializing the aggregated table,
     *                          if not specified the default serdes defined in the configs will be used
     * @param valueSerde        value serdes for materializing the aggregated table,
     *                          if not specified the default serdes defined in the configs will be used
     */
    <W extends Window> KTable<Windowed<K>, V> reduceByKey(Reducer<V> reducer,
                                                          Windows<W> windows,
                                                          Serde<K> keySerde,
                                                          Serde<V> valueSerde);

    /**
     * Combine values of this stream by key on a window basis into a new instance of windowed {@link KTable}
     * with default serializers and deserializers.
     *
     * @param reducer the instance of {@link Reducer}
     * @param windows the specification of the aggregation {@link Windows}
     */
    <W extends Window> KTable<Windowed<K>, V> reduceByKey(Reducer<V> reducer, Windows<W> windows);

    /**
     * Combine values of this stream by key into a new instance of ever-updating {@link KTable}.
     *
     * @param reducer           the instance of {@link Reducer}
     * @param keySerde          key serdes for materializing the aggregated table,
     *                          if not specified the default serdes defined in the configs will be used
     * @param valueSerde        value serdes for materializing the aggregated table,
     *                          if not specified the default serdes defined in the configs will be used
     * @param name              the name of the resulted {@link KTable}
     */
    KTable<K, V> reduceByKey(Reducer<V> reducer,
                             Serde<K> keySerde,
                             Serde<V> valueSerde,
                             String name);

    /**
     * Combine values of this stream by key into a new instance of ever-updating {@link KTable} with default serializers and deserializers.
     *
     * @param reducer the instance of {@link Reducer}
     * @param name    the name of the resulted {@link KTable}
     */
    KTable<K, V> reduceByKey(Reducer<V> reducer, String name);

    /**
     * Aggregate values of this stream by key on a window basis into a new instance of {@link KTable}.
     *
     * @param initializer   the instance of {@link Initializer}
     * @param aggregator    the instance of {@link Aggregator}
     * @param windows       the specification of the aggregation {@link Windows}
     * @param keySerde      key serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param aggValueSerde aggregate value serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param <T>           the value type of the resulted {@link KTable}
     */
    <T, W extends Window> KTable<Windowed<K>, T> aggregateByKey(Initializer<T> initializer,
                                                                Aggregator<K, V, T> aggregator,
                                                                Windows<W> windows,
                                                                Serde<K> keySerde,
                                                                Serde<T> aggValueSerde);

    /**
     * Aggregate values of this stream by key on a window basis into a new instance of windowed {@link KTable}
     * with default serializers and deserializers.
     *
     * @param initializer   the instance of {@link Initializer}
     * @param aggregator    the instance of {@link Aggregator}
     * @param windows       the specification of the aggregation {@link Windows}
     * @param <T>           the value type of the resulted {@link KTable}
     */
    <T, W extends Window> KTable<Windowed<K>, T> aggregateByKey(Initializer<T> initializer,
                                                                Aggregator<K, V, T> aggregator,
                                                                Windows<W> windows);

    /**
     * Aggregate values of this stream by key into a new instance of ever-updating {@link KTable}.
     *
     * @param initializer   the class of {@link Initializer}
     * @param aggregator    the class of {@link Aggregator}
     * @param keySerde      key serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param aggValueSerde aggregate value serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param name          the name of the resulted {@link KTable}
     * @param <T>           the value type of the resulted {@link KTable}
     */
    <T> KTable<K, T> aggregateByKey(Initializer<T> initializer,
                                    Aggregator<K, V, T> aggregator,
                                    Serde<K> keySerde,
                                    Serde<T> aggValueSerde,
                                    String name);

    /**
     * Aggregate values of this stream by key into a new instance of ever-updating {@link KTable}
     * with default serializers and deserializers.
     *
     * @param initializer   the class of {@link Initializer}
     * @param aggregator    the class of {@link Aggregator}
     * @param name          the name of the resulted {@link KTable}
     * @param <T>           the value type of the resulted {@link KTable}
     */
    <T> KTable<K, T> aggregateByKey(Initializer<T> initializer,
                                    Aggregator<K, V, T> aggregator,
                                    String name);

    /**
     * Count number of messages of this stream by key on a window basis into a new instance of windowed {@link KTable}.
     *
     * @param windows       the specification of the aggregation {@link Windows}
     * @param keySerde      key serdes for materializing the counting table,
     *                      if not specified the default serdes defined in the configs will be used
     */
    <W extends Window> KTable<Windowed<K>, Long> countByKey(Windows<W> windows, Serde<K> keySerde);

    /**
     * Count number of messages of this stream by key on a window basis into a new instance of windowed {@link KTable}
     * with default serializers and deserializers.
     *
     * @param windows       the specification of the aggregation {@link Windows}
     */
    <W extends Window> KTable<Windowed<K>, Long> countByKey(Windows<W> windows);

    /**
     * Count number of messages of this stream by key into a new instance of ever-updating {@link KTable}.
     *
     * @param keySerde      key serdes for materializing the counting table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param name          the name of the resulted {@link KTable}
     */
    KTable<K, Long> countByKey(Serde<K> keySerde, String name);

    /**
     * Count number of messages of this stream by key into a new instance of ever-updating {@link KTable}
     * with default serializers and deserializers.
     *
     * @param name          the name of the resulted {@link KTable}
     */
    KTable<K, Long> countByKey(String name);

}
