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
import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * KStream is an abstraction of a stream of key-value pairs.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public interface KStream<K, V> {

    /**
     * Creates a new instance of KStream consists of all elements of this stream which satisfy a predicate
     *
     * @param predicate the instance of Predicate
     * @return the stream with only those elements that satisfy the predicate
     */
    KStream<K, V> filter(Predicate<K, V> predicate);

    /**
     * Creates a new instance of KStream consists all elements of this stream which do not satisfy a predicate
     *
     * @param predicate the instance of Predicate
     * @return the stream with only those elements that do not satisfy the predicate
     */
    KStream<K, V> filterOut(Predicate<K, V> predicate);

    /**
     * Creates a new stream by applying transforming each element in this stream into a different element in the new stream.
     *
     * @param mapper the instance of KeyValueMapper
     * @param <K1>   the key type of the new stream
     * @param <V1>   the value type of the new stream
     * @return the mapped stream
     */
    <K1, V1> KStream<K1, V1> map(KeyValueMapper<K, V, KeyValue<K1, V1>> mapper);

    /**
     * Creates a new instance of KStream by applying transforming each value in this stream into a different value in the new stream.
     *
     * @param mapper the instance of ValueMapper
     * @param <V1>   the value type of the new stream
     * @return the instance of KStream
     */
    <V1> KStream<K, V1> mapValues(ValueMapper<V, V1> mapper);

    /**
     * Creates a new instance of KStream by applying transforming each element in this stream into zero or more elements in the new stream.
     *
     * @param mapper the instance of KeyValueMapper
     * @param <K1>   the key type of the new stream
     * @param <V1>   the value type of the new stream
     * @return the instance of KStream
     */
    <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K, V, Iterable<KeyValue<K1, V1>>> mapper);

    /**
     * Creates a new instance of KStream by applying transforming each value in this stream into zero or more values in the new stream.
     *
     * @param processor the instance of Processor
     * @param <V1>      the value type of the new stream
     * @return the instance of KStream
     */
    <V1> KStream<K, V1> flatMapValues(ValueMapper<V, Iterable<V1>> processor);

    /**
     * Creates a new windowed stream using a specified window instance.
     *
     * @param windowDef the instance of Window
     * @return the windowed stream
     */
    KStreamWindowed<K, V> with(WindowSupplier<K, V> windowDef);

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
     * Sends key-value to a topic, also creates a new stream from the topic.
     * This is equivalent to calling to(topic) and from(topic).
     *
     * @param topic           the topic name
     * @return the new stream that consumes the given topic
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
     * @return the new stream that consumes the given topic
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
     * @param transformerSupplier the class of TransformerDef
     * @param stateStoreNames the names of the state store used by the processor
     * @return the instance of KStream that contains transformed keys and values
     */
    <K1, V1> KStream<K1, V1> transform(TransformerSupplier<K, V, KeyValue<K1, V1>> transformerSupplier, String... stateStoreNames);

    /**
     * Applies a stateful transformation to all values in this stream.
     *
     * @param valueTransformerSupplier the class of TransformerDef
     * @param stateStoreNames the names of the state store used by the processor
     * @return the instance of KStream that contains transformed keys and values
     */
    <R> KStream<K, R> transformValues(ValueTransformerSupplier<V, R> valueTransformerSupplier, String... stateStoreNames);

    /**
     * Processes all elements in this stream by applying a processor.
     *
     * @param processorSupplier the supplier of the Processor to use
     * @param stateStoreNames the names of the state store used by the processor
     */
    void process(ProcessorSupplier<K, V> processorSupplier, String... stateStoreNames);

}
