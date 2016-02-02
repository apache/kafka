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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.InconsistentTypeInfoException;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.InsufficientTypeInfoException;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.type.Types;
import org.apache.kafka.streams.kstream.type.internal.Resolver;
import org.apache.kafka.streams.kstream.type.TypeException;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractStream<K> {

    protected final KStreamBuilder topology;
    protected final String name;
    protected final Set<String> sourceNodes;
    protected final Type keyType;
    protected final Type valueType;

    public AbstractStream(KStreamBuilder topology, String name, Set<String> sourceNodes, Type keyType, Type valueType) {
        this.topology = topology;
        this.name = name;
        this.sourceNodes = sourceNodes;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    protected Set<String> ensureJoinableWith(AbstractStream<K> other) {

        if (this.keyType == null)
            throw new InsufficientTypeInfoException("key type of this stream");

        if (other.keyType == null)
            throw new InsufficientTypeInfoException("key type of other stream");

        if (!this.keyType.equals(other.keyType))
            throw new TopologyBuilderException("not joinable: key types do not match");

        Set<String> thisSourceNodes = sourceNodes;
        Set<String> otherSourceNodes = other.sourceNodes;

        if (thisSourceNodes == null || otherSourceNodes == null)
            throw new TopologyBuilderException(this.name + " and " + other.name + " are not joinable");

        Set<String> allSourceNodes = new HashSet<>();
        allSourceNodes.addAll(thisSourceNodes);
        allSourceNodes.addAll(otherSourceNodes);

        topology.copartitionSources(allSourceNodes);

        return allSourceNodes;
    }

    public static <T2, T1, R> ValueJoiner<T2, T1, R> reverseJoiner(final ValueJoiner<T1, T2, R> joiner) {
        return new ValueJoiner<T2, T1, R>() {
            @Override
            public R apply(T2 value2, T1 value1) {
                return joiner.apply(value1, value2);
            }
        };
    }

    @SuppressWarnings("unchecked")
    protected <T> Serializer<T> getSerializer(Type type) {

        if (type == null)
            throw new InsufficientTypeInfoException();

        // If this is a windowed key, we construct a special serializer.
        Type windowedRawKeyType = Resolver.getRawKeyTypeFromWindowedType(type);
        if (windowedRawKeyType != null) {
            return new WindowedSerializer(topology.getSerializer(windowedRawKeyType));
        } else {
            return topology.getSerializer(type);
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> Deserializer<T> getDeserializer(Type type) {

        if (type == null)
            throw new InsufficientTypeInfoException();

        // If this is a windowed key, we construct a special serializer.
        Type windowedRawKeyType = Resolver.getRawKeyTypeFromWindowedType(type);
        if (windowedRawKeyType != null) {
            return new WindowedDeserializer(topology.getDeserializer(windowedRawKeyType));
        } else {
            return topology.getDeserializer(type);
        }
    }

    protected Type getWindowedKeyType() {
        try {
            if (keyType == null)
                throw new InsufficientTypeInfoException("key type");

            return Types.type(Windowed.class, keyType);

        } catch (TypeException ex) {
            throw new TopologyBuilderException("failed to define a windowed key type", ex);
        }
    }

    public static Type getKeyTypeFromKeyValueType(Type type) {
        return (type != null) ? Resolver.getKeyTypeFromKeyValueType(type) : null;
    }

    public static Type getValueTypeFromKeyValueType(Type type) {
        return (type != null) ? Resolver.getValueTypeFromKeyValueType(type) : null;
    }

    public static Type resolveReturnType(KeyValueMapper function) {
        return resolveReturnType(KeyValueMapper.class, "apply", function);
    }

    public static Type resolveReturnType(ValueMapper function) {
        return resolveReturnType(ValueMapper.class, "apply", function);
    }

    public static Type resolveReturnType(ValueJoiner function) {
        return resolveReturnType(ValueJoiner.class, "apply", function);
    }

    public static Type resolveReturnType(Reducer function) {
        return resolveReturnType(Reducer.class, "apply", function);
    }

    public static Type resolveReturnType(Initializer function) {
        return resolveReturnType(Initializer.class, "apply", function);
    }

    public static Type resolveReturnType(Aggregator function) {
        return resolveReturnType(Aggregator.class, "apply", function);
    }

    private static <T> Type resolveReturnType(Class<T> interfaceClass, String methodName, T implementation) {
        try {
            return Resolver.resolveReturnType(interfaceClass, methodName, implementation.getClass());

        } catch (TypeException ex) {
            return null;
        }
    }

    public static Type resolveReturnTypeFromSupplier(Class interfaceClass, String methodName, Object supplier) {
        try {
            Class supplierInterface;

            if (interfaceClass.equals(Transformer.class)) {
                supplierInterface = TransformerSupplier.class;
            } else if (interfaceClass.equals(ValueTransformer.class)) {
                supplierInterface = ValueTransformerSupplier.class;
            } else if (interfaceClass.equals(Aggregator.class)) {
                supplierInterface = Aggregator.class;
            } else {
                return null;
            }

            Type implementationType = Resolver.resolveReturnType(supplierInterface, "get", supplier.getClass());

            Method[] methods = interfaceClass.getDeclaredMethods();

            Method method = null;
            for (Method m : methods) {
                if (methodName.equals(m.getName())) {
                    method = m;
                    break;
                }
            }
            if (method == null)
                return null;

            return Resolver.resolveReturnType(method, implementationType);

        } catch (TypeException ex) {
            return null;
        }
    }

    public static Type resolveElementTypeFromIterable(Type iterableType) {
        try {
            return Resolver.resolveElementTypeFromIterableType(iterableType);
        } catch (TypeException ex) {
            return null;
        }
    }

    public static Type ensureConsistentTypes(Type... types) {
        Type type = null;

        for (Type t : types) {
            if (type == null) {
                type = t;
            } else {
                if (type != t)
                    throw new InconsistentTypeInfoException(types);
            }
        }
        return type;
    }

}
