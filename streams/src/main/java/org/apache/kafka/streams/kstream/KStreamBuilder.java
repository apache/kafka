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
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.kstream.type.TypeException;
import org.apache.kafka.streams.kstream.type.Resolver;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.TopologyException;

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
    private Serializer defaultSerializer = null;
    private Deserializer defaultDeserializer = null;

    /**
     * Defines an explicit type information. It is used when registering a serializer and/or a deserializer for a type.
     * And it also used to give an explicit return type information to functions given to KStream/KTable methods.
     * <p>
     * Example,
     * <pre>
     *     // assuming that the define method is declared using
     *     // import static org.apache.kafka.streams.kstream.KStreamBuilder.define;
     *
     *     builder.register(define(MyGenericClass.class, String.class),
     *         new MyGenericClassDeSerializer(), new MyGenericClassDeserializer())
     * </pre>
     * </p>
     *
     * @param type the Class instance for this type
     * @param typeArgs type arguments
     * @return Type instance
     * @throws TypeException
     */
    public static Type define(Class<?> type, Type... typeArgs) throws TypeException {
        if (typeArgs != null && typeArgs.length > 0) {
            return Resolver.getTypeWithTypeArgs(type, typeArgs);
        } else {
            return type;
        }
    }

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
                throw new TopologyException("a serializer already registered for this type: " + type.toString());

            registeredSerializers.put(convertedType, serializer);

        } catch (TypeException ex) {
            throw new TopologyException("failed to register serializer for this type: " + type);
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
                throw new TopologyException("a deserializer already registered for this type: " + type.toString());

            registeredDeserializers.put(convertedType, deserializer);

        } catch (TypeException ex) {
            throw new TopologyException("failed to register deserializer for this type: " + type);
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
     * Register a default serializer.
     *
     * @param serializer the instance of the default Serializer
     */
    public void registerDefault(Serializer<?> serializer) {
        if (defaultSerializer != null)
            throw new TopologyException("the default serializer already registered");

        defaultSerializer = serializer;
    }

    /**
     * Register a default deserializer.
     *
     * @param deserializer the instance of the default Deserializer
     */
    public void registerDefault(Deserializer<?> deserializer) {
        if (defaultDeserializer == null)
            throw new TopologyException("the default deserializer already registered");

        defaultDeserializer = deserializer;
    }

    /**
     * Register a default serializer and a default deserializer for the specified type.
     *
     * @param serializer the default Serializer instance for this type
     * @param deserializer the default Deserializer instance for this type
     */
    public void registerDefault(Serializer<?> serializer, Deserializer<?> deserializer) {
        registerDefault(serializer);
        registerDefault(deserializer);
    }


    /**
     * Creates a KStream instance for the specified topic.
     * The default deserializers specified in the config are used.
     *
     * @param topics          the topic names, if empty default to all the topics in the config
     * @return KStream
     */
    public <K, V> KStream<K, V> stream(String... topics) {
        return stream((Type) null, (Type) null, topics);
    }

    /**
     * Creates a KStream instance for the specified topic.
     *
     * @param keyDeserializer key deserializer used to read this source KStream,
     *                        if not specified the default deserializer defined in the configs will be used
     * @param valDeserializer value deserializer used to read this source KStream,
     *                        if not specified the default deserializer defined in the configs will be used
     * @param topics          the topic names, if empty default to all the topics in the config
     * @return KStream
     */
    public <K, V> KStream<K, V> stream(Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String... topics) {
        String name = newName(KStreamImpl.SOURCE_NAME);

        addSource(name, keyDeserializer, valDeserializer, topics);

        return new KStreamImpl<>(this, name, Collections.singleton(name),
                // infer type from key deserializers
                (keyDeserializer != null) ? Resolver.resolveFromDeserializer(keyDeserializer) : null,
                // infer type from value deserializers
                (valDeserializer != null) ? Resolver.resolveFromDeserializer(valDeserializer) : null);
    }

    /**
     * Creates a KStream instance for the specified topic.
     *
     * @param keyType an instance of Type that represents the key type. If the deserializer is registered for this type,
     *                the registered deserializers is used to read this source KStream,
     *                        if not specified the default deserializer defined in the configs will be used
     * @param valueType an instance of Type that represents the value type. If the deserializer is registered for this type,
     *                the registered deserializers is used to read this source KStream
     * @param topics          the topic names, if empty default to all the topics in the config
     * @return KStream
     */
    public <K, V> KStream<K, V> stream(Type keyType, Type valueType, String... topics) {
        try {
            Type convertedKeyType = (keyType != null) ? Resolver.resolve(keyType) : null;
            Type convertedValueType = (valueType != null) ? Resolver.resolve(valueType) : null;

            String name = newName(KStreamImpl.SOURCE_NAME);

            addSource(name, getDeserializer(convertedKeyType), getDeserializer(convertedValueType), topics);

            return new KStreamImpl<>(this, name, Collections.singleton(name), convertedKeyType, convertedValueType);

        } catch (TypeException ex) {
            throw new TopologyException("failed to create a stream", ex);
        }
    }

    /**
     * Creates a KTable instance for the specified topic.
     * The default deserializers specified in the config are used.
     *
     * @param topic          the topic name
     * @return KTable
     */
    public <K, V> KTable<K, V> table(String topic) {
        return table((Type) null, (Type) null, topic);
    }

    /**
     * Creates a KTable instance for the specified topic.
     *
     * @param keySerializer   key serializer used to send key-value pairs,
     *                        if not specified the default key serializer defined in the configuration will be used
     * @param valSerializer   value serializer used to send key-value pairs,
     *                        if not specified the default value serializer defined in the configuration will be used
     * @param keyDeserializer key deserializer used to read this source KStream,
     *                        if not specified the default deserializer defined in the configs will be used
     * @param valDeserializer value deserializer used to read this source KStream,
     *                        if not specified the default deserializer defined in the configs will be used
     * @param topic          the topic name
     * @return KStream
     */
    public <K, V> KTable<K, V> table(Serializer<K> keySerializer, Serializer<V> valSerializer, Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String topic) {
        String source = newName(KStreamImpl.SOURCE_NAME);
        String name = newName(KTableImpl.SOURCE_NAME);

        addSource(source, keyDeserializer, valDeserializer, topic);

        ProcessorSupplier<K, V> processorSupplier = new KTableSource<>(topic);
        addProcessor(name, processorSupplier, source);

        return new KTableImpl<>(this, name, processorSupplier, Collections.singleton(source),
                keySerializer,
                valSerializer,
                keyDeserializer,
                valDeserializer,
                // infer type from key deserializers
                (keyDeserializer != null) ? Resolver.resolveFromDeserializer(keyDeserializer) : null,
                // infer type from value deserializers
                (valDeserializer != null) ? Resolver.resolveFromDeserializer(valDeserializer) : null);
    }

    /**
     * Creates a KTable instance for the specified topic.
     *
     * @param keyType an instance of Type that represents the key type. If the deserializer is registered for this type,
     *                the registered deserializers is used to read this source KStream,
     *                        if not specified the default deserializer defined in the configs will be used
     * @param valueType an instance of Type that represents the value type. If the deserializer is registered for this type,
     *                the registered deserializers is used to read this source KStream
     * @param topic          the topic name
     * @return KStream
     */
    @SuppressWarnings("unchecked")
    public <K, V> KTable<K, V> table(Type keyType, Type valueType, String topic) {
        try {
            Type convertedKeyType = (keyType != null) ? Resolver.resolve(keyType) : null;
            Type convertedValueType = (valueType != null) ? Resolver.resolve(valueType) : null;

            String source = newName(KStreamImpl.SOURCE_NAME);
            String name = newName(KTableImpl.SOURCE_NAME);

            addSource(source, getDeserializer(convertedKeyType), getDeserializer(convertedValueType), topic);

            ProcessorSupplier<K, V> processorSupplier = new KTableSource<>(topic);
            addProcessor(name, processorSupplier, source);

            return new KTableImpl<>(this, name, processorSupplier, Collections.singleton(source),
                    (Serializer<K>) getSerializer(convertedKeyType),
                    (Serializer<V>) getSerializer(convertedValueType),
                    (Deserializer<K>) getDeserializer(convertedKeyType),
                    (Deserializer<V>) getDeserializer(convertedValueType),
                    convertedKeyType,
                    convertedValueType);

        } catch (TypeException ex) {
            throw new TopologyException("failed to create a stream", ex);
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
        if (type != null) {
            return nvl(registeredSerializers.get(type), defaultSerializer);
        } else {
            return defaultSerializer;
        }
    }

    @SuppressWarnings("unchecked")
    public <T> Deserializer<T> getDeserializer(Type type) {
        if (type != null) {
            return nvl(registeredDeserializers.get(type), defaultDeserializer);
        } else {
            return defaultDeserializer;
        }
    }

    private static <T> T nvl(T value, T defaultValue) {
        return (value == null) ? defaultValue : value;
    }
}
