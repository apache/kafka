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
import org.apache.kafka.streams.processor.ProcessorDef;

/**
 * KStream is an abstraction of a stream of key-value pairs.
 */
public interface KStream<K, V> {

    /**
     * Creates a new stream consists of all elements of this stream which satisfy a predicate
     *
     * @param predicate the instance of Predicate
     * @return KStream
     */
    KStream<K, V> filter(Predicate<K, V> predicate);

    /**
     * Creates a new stream consists all elements of this stream which do not satisfy a predicate
     *
     * @param predicate the instance of Predicate
     * @return KStream
     */
    KStream<K, V> filterOut(Predicate<K, V> predicate);

    /**
     * Creates a new stream by transforming key-value pairs by a mapper to all elements of this stream
     *
     * @param mapper the instance of KeyValueMapper
     * @param <K1>   the key type of the new stream
     * @param <V1>   the value type of the new stream
     * @return KStream
     */
    <K1, V1> KStream<K1, V1> map(KeyValueMapper<K, V, KeyValue<K1, V1>> mapper);

    /**
     * Creates a new stream by transforming valuesa by a mapper to all values of this stream
     *
     * @param mapper the instance of ValueMapper
     * @param <V1>   the value type of the new stream
     * @return KStream
     */
    <V1> KStream<K, V1> mapValues(ValueMapper<V, V1> mapper);

    /**
     * Creates a new stream by applying a mapper to all elements of this stream and using the values in the resulting Iterable
     *
     * @param mapper the instance of KeyValueMapper
     * @param <K1>   the key type of the new stream
     * @param <V1>   the value type of the new stream
     * @return KStream
     */
    <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K, V, Iterable<KeyValue<K1, V1>>> mapper);

    /**
     * Creates a new stream by applying a mapper to all values of this stream and using the values in the resulting Iterable
     *
     * @param processor the instance of Processor
     * @param <V1>      the value type of the new stream
     * @return KStream
     */
    <V1> KStream<K, V1> flatMapValues(ValueMapper<V, Iterable<V1>> processor);

    /**
     * Creates a new windowed stream using a specified window instance.
     *
     * @param windowDef the instance of Window
     * @return KStream
     */
    KStreamWindowed<K, V> with(WindowDef<K, V> windowDef);

    /**
     * Creates an array of streams from this stream. Each stream in the array coresponds to a predicate in
     * supplied predicates in the same order. Predicates are evaluated in order. An element is streamed to
     * a corresponding stream for the first predicate is evaluated true.
     * An element will be dropped if none of the predicates evaluate true.
     *
     * @param predicates Instances of Predicate
     * @return KStream
     */
    KStream<K, V>[] branch(Predicate<K, V>... predicates);

    /**
     * Sends key-value to a topic, also creates a new stream from the topic.
     * The created stream is added to the default synchronization group.
     * This is equivalent to calling sendTo(topic) and from(topic).
     *
     * @param topic           the topic name
     * @param keySerializer   key serializer used to send key-value pairs,
     *                        if not specified the default serializer defined in the configs will be used
     * @param valSerializer   value serializer used to send key-value pairs,
     *                        if not specified the default serializer defined in the configs will be used
     * @param keyDeserializer key deserializer used to create the new KStream,
     *                        if not specified the default deserializer defined in the configs will be used
     * @param valDeserializer value deserializer used to create the new KStream,
     *                        if not specified the default deserializer defined in the configs will be used
     * @param <K1>            the key type of the new stream
     * @param <V1>            the value type of the new stream
     * @return KStream
     */
    <K1, V1> KStream<K1, V1> through(String topic, Serializer<K> keySerializer, Serializer<V> valSerializer, Deserializer<K1> keyDeserializer, Deserializer<V1> valDeserializer);

    /**
     * Sends key-value to a topic using default serializers specified in the config.
     *
     * @param topic         the topic name
     */
    void sendTo(String topic);

    /**
     * Sends key-value to a topic.
     *
     * @param topic         the topic name
     * @param keySerializer key serializer used to send key-value pairs,
     *                      if not specified the default serializer defined in the configs will be used
     * @param valSerializer value serializer used to send key-value pairs,
     *                      if not specified the default serializer defined in the configs will be used
     */
    void sendTo(String topic, Serializer<K> keySerializer, Serializer<V> valSerializer);

    /**
     * Processes all elements in this stream by applying a processor.
     *
     * @param processorDef the class of ProcessorDef
     */
    <K1, V1> KStream<K1, V1> process(ProcessorDef processorDef);
}
