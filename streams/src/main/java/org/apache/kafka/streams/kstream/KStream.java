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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * KStream is an abstraction of an event stream in key-value pairs.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
public interface KStream<K, V> {

    /**
     * Creates a new instance of KStream consists of all elements of this stream which satisfy a predicate
     *
     * @param predicate the instance of Predicate
     * @return the instance of KStream with only those elements that satisfy the predicate
     */
    KStream<K, V> filter(Predicate<K, V> predicate);

    /**
     * Creates a new instance of KStream consists all elements of this stream which do not satisfy a predicate
     *
     * @param predicate the instance of Predicate
     * @return the instance of KStream with only those elements that do not satisfy the predicate
     */
    KStream<K, V> filterOut(Predicate<K, V> predicate);

    /**
     * Creates a new instance of KStream by applying transforming each element in this stream into a different element in the new stream.
     *
     * @param mapper the instance of KeyValueMapper
     * @param <K1>   the key type of the new stream
     * @param <V1>   the value type of the new stream
     * @return the instance of KStream
     */
    <K1, V1> KStream<K1, V1> map(KeyValueMapper<K, V, KeyValue<K1, V1>> mapper);

    /**
     * Creates a new instance of KStream by transforming each value in this stream into a different value in the new stream.
     *
     * @param mapper the instance of ValueMapper
     * @param <V1>   the value type of the new stream
     * @return the instance of KStream
     */
    <V1> KStream<K, V1> mapValues(ValueMapper<V, V1> mapper);

    /**
     * Creates a new instance of KStream by transforming each element in this stream into zero or more elements in the new stream.
     *
     * @param mapper the instance of KeyValueMapper
     * @param <K1>   the key type of the new stream
     * @param <V1>   the value type of the new stream
     * @return the instance of KStream
     */
    <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K, V, Iterable<KeyValue<K1, V1>>> mapper);

    /**
     * Creates a new stream by transforming each value in this stream into zero or more values in the new stream.
     *
     * @param processor the instance of Processor
     * @param <V1>      the value type of the new stream
     * @return the instance of KStream
     */
    <V1> KStream<K, V1> flatMapValues(ValueMapper<V, Iterable<V1>> processor);

    /**
     * Creates an array of streams from this stream. Each stream in the array corresponds to a predicate in
     * supplied predicates in the same order. Predicates are evaluated in order. An element is streamed to
     * a corresponding stream for the first predicate is evaluated true.
     * An element will be dropped if none of the predicates evaluate true.
     *
     * @param predicates the ordered list of Predicate instances
     * @return the instances of KStream that each contain those elements for which their Predicate evaluated to true.
     */
    KStream<K, V>[] branch(Predicate<K, V>... predicates);

    /**
     * Sends key-value to a topic, also creates a new instance of KStream from the topic.
     * This is equivalent to calling to(topic) and from(topic).
     *
     * @param topic           the topic name
     * @return the instance of KStream that consumes the given topic
     */
    KStream<K, V> through(String topic);

    /**
     * Sends key-value to a topic, also creates a new instance of KStream from the topic.
     * This is equivalent to calling to(topic) and from(topic).
     *
     * @param topic           the topic name
     * @param keySerializer   key serializer used to send key-value pairs,
     *                        if not specified the default key serializer defined in the configuration will be used
     * @param valSerializer   value serializer used to send key-value pairs,
     *                        if not specified the default value serializer defined in the configuration will be used
     * @param keyDeserializer key deserializer used to create the new KStream,
     *                        if not specified the default key deserializer defined in the configuration will be used
     * @param valDeserializer value deserializer used to create the new KStream,
     *                        if not specified the default value deserializer defined in the configuration will be used
     * @return the instance of KStream that consumes the given topic
     */
    KStream<K, V> through(String topic, Serializer<K> keySerializer, Serializer<V> valSerializer, Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer);

    /**
     * Sends key-value to a topic using default serializers specified in the config.
     *
     * @param topic         the topic name
     */
    void to(String topic);

    /**
     * Sends key-value to a topic.
     *
     * @param topic         the topic name
     * @param keySerializer key serializer used to send key-value pairs,
     *                      if not specified the default serializer defined in the configs will be used
     * @param valSerializer value serializer used to send key-value pairs,
     *                      if not specified the default serializer defined in the configs will be used
     */
    void to(String topic, Serializer<K> keySerializer, Serializer<V> valSerializer);

    /**
     * Applies a stateful transformation to all elements in this stream.
     *
     * @param transformerSupplier the class of valueTransformerSupplier
     * @param stateStoreNames the names of the state store used by the processor
     * @return the instance of KStream that contains transformed keys and values
     */
    <K1, V1> KStream<K1, V1> transform(TransformerSupplier<K, V, KeyValue<K1, V1>> transformerSupplier, String... stateStoreNames);

    /**
     * Applies a stateful transformation to all values in this stream.
     *
     * @param valueTransformerSupplier the class of valueTransformerSupplier
     * @param stateStoreNames the names of the state store used by the processor
     * @return the instance of KStream that contains the keys and transformed values
     */
    <R> KStream<K, R> transformValues(ValueTransformerSupplier<V, R> valueTransformerSupplier, String... stateStoreNames);

    /**
     * Processes all elements in this stream by applying a processor.
     *
     * @param processorSupplier the supplier of the Processor to use
     * @param stateStoreNames the names of the state store used by the processor
     */
    void process(ProcessorSupplier<K, V> processorSupplier, String... stateStoreNames);

    /**
     * Combines values of this stream with another KStream using Windowed Inner Join.
     *
     * @param otherStream the instance of KStream joined with this stream
     * @param joiner ValueJoiner
     * @param windows the specification of the join window
     * @param keySerializer key serializer,
     *                      if not specified the default serializer defined in the configs will be used
     * @param thisValueSerializer value serializer for this stream,
     *                      if not specified the default serializer defined in the configs will be used
     * @param otherValueSerializer value serializer for other stream,
     *                      if not specified the default serializer defined in the configs will be used
     * @param keyDeserializer key deserializer,
     *                      if not specified the default serializer defined in the configs will be used
     * @param thisValueDeserializer value deserializer for this stream,
     *                      if not specified the default serializer defined in the configs will be used
     * @param otherValueDeserializer value deserializer for other stream,
     *                      if not specified the default serializer defined in the configs will be used
     * @param <V1>   the value type of the other stream
     * @param <R>   the value type of the new stream
     */
    <V1, R> KStream<K, R> join(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows,
            Serializer<K> keySerializer,
            Serializer<V> thisValueSerializer,
            Serializer<V1> otherValueSerializer,
            Deserializer<K> keyDeserializer,
            Deserializer<V> thisValueDeserializer,
            Deserializer<V1> otherValueDeserializer);

    /**
     * Combines values of this stream with another KStream using Windowed Outer Join.
     *
     * @param otherStream the instance of KStream joined with this stream
     * @param joiner ValueJoiner
     * @param windows the specification of the join window
     * @param keySerializer key serializer,
     *                      if not specified the default serializer defined in the configs will be used
     * @param thisValueSerializer value serializer for this stream,
     *                      if not specified the default serializer defined in the configs will be used
     * @param otherValueSerializer value serializer for other stream,
     *                      if not specified the default serializer defined in the configs will be used
     * @param keyDeserializer key deserializer,
     *                      if not specified the default serializer defined in the configs will be used
     * @param thisValueDeserializer value deserializer for this stream,
     *                      if not specified the default serializer defined in the configs will be used
     * @param otherValueDeserializer value deserializer for other stream,
     *                      if not specified the default serializer defined in the configs will be used
     * @param <V1>   the value type of the other stream
     * @param <R>   the value type of the new stream
     */
    <V1, R> KStream<K, R> outerJoin(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows,
            Serializer<K> keySerializer,
            Serializer<V> thisValueSerializer,
            Serializer<V1> otherValueSerializer,
            Deserializer<K> keyDeserializer,
            Deserializer<V> thisValueDeserializer,
            Deserializer<V1> otherValueDeserializer);

    /**
     * Combines values of this stream with another KStream using Windowed Left Join.
     *
     * @param otherStream the instance of KStream joined with this stream
     * @param joiner ValueJoiner
     * @param windows the specification of the join window
     * @param keySerializer key serializer,
     *                      if not specified the default serializer defined in the configs will be used
     * @param otherValueSerializer value serializer for other stream,
     *                      if not specified the default serializer defined in the configs will be used
     * @param keyDeserializer key deserializer,
     *                      if not specified the default serializer defined in the configs will be used
     * @param otherValueDeserializer value deserializer for other stream,
     *                      if not specified the default serializer defined in the configs will be used
     * @param <V1>   the value type of the other stream
     * @param <R>   the value type of the new stream
     */
    <V1, R> KStream<K, R> leftJoin(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows,
            Serializer<K> keySerializer,
            Serializer<V1> otherValueSerializer,
            Deserializer<K> keyDeserializer,
            Deserializer<V1> otherValueDeserializer);

    /**
     * Combines values of this stream with KTable using Left Join.
     *
     * @param ktable the instance of KTable joined with this stream
     * @param joiner ValueJoiner
     * @param <V1>   the value type of the table
     * @param <V2>   the value type of the new stream
     */
    <V1, V2> KStream<K, V2> leftJoin(KTable<K, V1> ktable, ValueJoiner<V, V1, V2> joiner);

    /**
     * Aggregate values of this stream by key on a window basis.
     *
     * @param reducer the class of Reducer
     * @param windows the specification of the aggregation window
     */
    <W extends Window> KTable<Windowed<K>, V> reduceByKey(Reducer<V> reducer,
                                                          Windows<W> windows,
                                                          Serializer<K> keySerializer,
                                                          Serializer<V> aggValueSerializer,
                                                          Deserializer<K> keyDeserializer,
                                                          Deserializer<V> aggValueDeserializer);

    /**
     * Aggregate values of this stream by key on a window basis.
     *
     * @param reducer the class of Reducer
     */
    KTable<K, V> reduceByKey(Reducer<V> reducer,
                             Serializer<K> keySerializer,
                             Serializer<V> aggValueSerializer,
                             Deserializer<K> keyDeserializer,
                             Deserializer<V> aggValueDeserializer,
                             String name);

    /**
     * Aggregate values of this stream by key on a window basis.
     *
     * @param initializer the class of Initializer
     * @param aggregator the class of Aggregator
     * @param windows the specification of the aggregation window
     * @param <T>   the value type of the aggregated table
     */
    <T, W extends Window> KTable<Windowed<K>, T> aggregateByKey(Initializer<T> initializer,
                                                                Aggregator<K, V, T> aggregator,
                                                                Windows<W> windows,
                                                                Serializer<K> keySerializer,
                                                                Serializer<T> aggValueSerializer,
                                                                Deserializer<K> keyDeserializer,
                                                                Deserializer<T> aggValueDeserializer);

    /**
     * Aggregate values of this stream by key without a window basis, and hence
     * return an ever updating table
     *
     * @param initializer the class of Initializer
     * @param aggregator the class of Aggregator
     * @param <T>   the value type of the aggregated table
     */
    <T> KTable<K, T> aggregateByKey(Initializer<T> initializer,
                                    Aggregator<K, V, T> aggregator,
                                    Serializer<K> keySerializer,
                                    Serializer<T> aggValueSerializer,
                                    Deserializer<K> keyDeserializer,
                                    Deserializer<T> aggValueDeserializer,
                                    String name);

    /**
     * Count number of messages of this stream by key on a window basis.
     *
     * @param windows the specification of the aggregation window
     */
    <W extends Window> KTable<Windowed<K>, Long> countByKey(Windows<W> windows,
                                                            Serializer<K> keySerializer,
                                                            Serializer<Long> aggValueSerializer,
                                                            Deserializer<K> keyDeserializer,
                                                            Deserializer<Long> aggValueDeserializer);

    /**
     * Count number of messages of this stream by key without a window basis, and hence
     * return a ever updating counting table.
     * This method uses {@link org.apache.kafka.common.serialization.LongSerializer LongSerializer}
     * and {@link org.apache.kafka.common.serialization.LongDeserializer LongDeserializer} for serialization of counts.
     */
    KTable<K, Long> countByKey(Serializer<K> keySerializer,
                               Deserializer<K> keyDeserializer,
                               String name);

    /**
     * Count number of messages of this stream by key without a window basis, and hence
     * return a ever updating counting table
     *
     */
    KTable<K, Long> countByKey(Serializer<K> keySerializer,
                               Serializer<Long> countSerializer,
                               Deserializer<K> keyDeserializer,
                               Deserializer<Long> countDeserializer,
                               String name);

}
