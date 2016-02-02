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
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.kstream.type.TypeException;
import org.apache.kafka.streams.kstream.type.internal.Resolver;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KStreamBuilder is the class to create KStream instances.
 */
public class KStreamBuilder extends TopologyBuilder {

    private final AtomicInteger index = new AtomicInteger(0);
    private final Map<Type, Serializer> registeredSerializers = new HashMap<>();
    private final Map<Type, Deserializer> registeredDeserializers = new HashMap<>();
    private SerializationFactory serializationFactory = null;

    public KStreamBuilder() {
        super();
    }

    /**
     * Register a serializer for the specified type.
     * <p>
     * Examples,
     * <pre>
     *     topologyBuilder.register(String.class, new StringSerializer());
     *     topologyBuilder.register(Type.define(List.class, Integer.class), new MyIntegerListSerializer());
     * </pre>
     * </p>
     *
     * @param type the Class instance or Type instance created by Type.define() method
     * @param serializer the instance of Serializer for this type
     */
    public void register(Type type, Serializer<?> serializer) {
        try {
            Type convertedType = Resolver.resolve(type);

            if (registeredSerializers.containsKey(convertedType))
                throw new TopologyBuilderException("a serializer already registered for this type: " + type.toString());

            registeredSerializers.put(convertedType, serializer);

        } catch (TypeException ex) {
            throw new TopologyBuilderException("failed to register serializer for this type: " + type);
        }
    }

    /**
     * Register a deserializer for the specified type.
     * <p>
     * Examples,
     * <pre>
     *     topologyBuilder.register(String.class, new StringDeserializer());
     *     topologyBuilder.register(Type.define(List.class, Integer.class), new MyIntegerListDeserializer());
     * </pre>
     * </p>
     *
     * @param type the Class instance or Type instance created by Type.define() method
     * @param deserializer the instance of Deserializer for this type
     */
    public void register(Type type, Deserializer<?> deserializer) {
        try {
            Type convertedType = Resolver.resolve(type);

            if (registeredDeserializers.containsKey(convertedType))
                throw new TopologyBuilderException("a deserializer already registered for this type: " + type.toString());

            registeredDeserializers.put(convertedType, deserializer);

        } catch (TypeException ex) {
            throw new TopologyBuilderException("failed to register deserializer for this type: " + type);
        }
    }

    /**
     * Register a serializer and a deserializer for the specified type.
     * <p>
     * Examples,
     * <pre>
     *     topologyBuilder.register(String.class, new StringSerializer(), new StringDeserializer());
     *     topologyBuilder.register(Type.define(List.class, Integer.class),
     *                              new MyIntegerListSerializer(), new MyIntegerListDeserializer());
     * </pre>
     * </p>
     *
     * @param type the Class instance or Type instance created by Type.define() method
     * @param serializer the instance of Serializer for this type
     * @param deserializer the instance Deserializer for this type
     */
    public void register(Type type, Serializer<?> serializer, Deserializer<?> deserializer) {
        register(type, serializer);
        register(type, deserializer);
    }

    /**
     * Register a serialization factory.
     * The factory is used to create a serializer/deserializer for a type is not already registered
     *
     * @param serializationFactory a factory object
     */
    public void register(SerializationFactory serializationFactory) {
        if (this.serializationFactory != null)
            throw new TopologyBuilderException("the serialization factory already registered");

        this.serializationFactory = serializationFactory;
    }

    /**
     * Creates a KStream instance for the specified topic.
     *
     * @param keyType         an instance of Type that represents the key type. If the deserializer is registered for this type,
     *                        the registered deserializers is used to read this source KStream,
     *                        If there is no such deserializer, the default deserializer will be used.
     * @param valueType       an instance of Type that represents the value type. If the deserializer is registered for this type,
     *                        the registered deserializers is used to read this source KStream
     *                        If there is no such deserializer, the default deserializer will be used.
     * @param topics          the topic names, if empty default to all the topics in the config
     * @return KStream
     */
    public <K, V> KStream<K, V> stream(Type keyType, Type valueType, String... topics) {
        if (keyType == null)
            throw new TopologyBuilderException("the key type should not be null");

        if (valueType == null)
            throw new TopologyBuilderException("the value type should not be null");

        try {
            String name = newName(KStreamImpl.SOURCE_NAME);

            Type resolvedKeyType = Resolver.resolve(keyType);
            Type resolvedValueType = Resolver.resolve(valueType);

            Deserializer<K> keyDeserializer = getDeserializer(resolvedKeyType);
            Deserializer<K> valDeserializer = getDeserializer(resolvedValueType);

            addSource(name, keyDeserializer, valDeserializer, topics);

            return new KStreamImpl<>(this, name, Collections.singleton(name), resolvedKeyType, resolvedValueType);

        } catch (TypeException ex) {
            throw new TopologyBuilderException("failed to create a stream", ex);
        }
    }

    /**
     * Creates a KTable instance for the specified topic.
     *
     * @param keyType        an instance of Type that represents the key type.
     *                       If the serializer/deserializer are registered for this type,
     *                       the registered serializer/deserializer will be used to read this source KStream,
     *                       If there is no such serializer/deserializer, the default serializer/deserializer will be used
     * @param valueType      an instance of Type that represents the value type.
     *                       If the serializer/deserializer are registered for this type,
     *                       the registered serializer/deserializers will be used to read this source KStream
     *                       If there is no such serializer/deserializer, the default serializer/deserializer will be used
     * @param topic          the topic name
     * @return KStream
     */
    @SuppressWarnings("unchecked")
    public <K, V> KTable<K, V> table(Type keyType, Type valueType, String topic) {
        try {
            Type resolvedKeyType = Resolver.resolve(keyType);
            Type resolvedValueType = Resolver.resolve(valueType);

            String source = newName(KStreamImpl.SOURCE_NAME);
            String name = newName(KTableImpl.SOURCE_NAME);

            Deserializer<K> keyDeserializer = getDeserializer(resolvedKeyType);
            Deserializer<V> valDeserializer = getDeserializer(resolvedValueType);

            addSource(source, keyDeserializer, valDeserializer, topic);

            ProcessorSupplier<K, V> processorSupplier = new KTableSource<>(topic);
            addProcessor(name, processorSupplier, source);

            return new KTableImpl<>(this, name, processorSupplier, Collections.singleton(source), keyType, valueType);

        } catch (TypeException ex) {
            throw new TopologyBuilderException("failed to create a stream", ex);
        }
    }

    /**
     * Creates a new stream by merging the given streams
     *
     * @param streams the streams to be merged
     * @return KStream
     */
    public <K, V> KStream<K, V> merge(KStream<K, V>... streams) {
        return KStreamImpl.merge(this, streams);
    }

    public String newName(String prefix) {
        return prefix + String.format("%010d", index.getAndIncrement());
    }

    @SuppressWarnings("unchecked")
    public <T> Serializer<T> getSerializer(Type type) {
        Serializer<T> serializer = registeredSerializers.get(type);

        if (serializer == null) {
            serializer = (Serializer<T>) serializationFactory.getSerializer(type);
            if (serializer != null) {
                registeredSerializers.put(type, serializer);
            }
        }

        if (serializer == null)
            throw new TopologyBuilderException("failed to find a serializer for a type " + type);

        return serializer;
    }

    @SuppressWarnings("unchecked")
    public <T> Deserializer<T> getDeserializer(Type type) {
        Deserializer<T> deserializer = registeredDeserializers.get(type);

        if (deserializer == null) {
            deserializer = (Deserializer<T>) serializationFactory.getDeserializer(type);
            if (deserializer != null) {
                registeredDeserializers.put(type, deserializer);
            }
        }

        if (deserializer == null)
            throw new TopologyBuilderException("failed to find a deserializer for a type " + type);

        return deserializer;
    }

}
