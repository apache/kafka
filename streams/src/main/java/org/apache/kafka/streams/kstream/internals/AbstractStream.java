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
import org.apache.kafka.streams.kstream.InsufficientTypeInfoException;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.type.internal.Resolver;
import org.apache.kafka.streams.kstream.type.TypeException;
import org.apache.kafka.streams.processor.TopologyException;

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
            throw new TopologyException("not joinable: key types do not match");

        Set<String> thisSourceNodes = sourceNodes;
        Set<String> otherSourceNodes = other.sourceNodes;

        if (thisSourceNodes == null || otherSourceNodes == null)
            throw new TopologyException("not joinable");

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

    protected <T> Serializer<T> getSerializer(Type type) {

        if (type == null)
            throw new InsufficientTypeInfoException();

        return topology.getSerializer(type);
    }

    protected <T> Deserializer<T> getDeserializer(Type type) {

        if (type == null)
            throw new InsufficientTypeInfoException();

        return topology.getDeserializer(type);
    }

    public static Type getWindowedRawKeyType(Type type) {
        if (type == null)
            throw new InsufficientTypeInfoException();

        return Resolver.getWindowedRawKeyType(type);
    }

    public static Type getKeyTypeFromKeyValueType(Type type) {
        return (type != null) ? Resolver.getKeyTypeFromKeyValueType(type) : null;
    }

    public static Type getValueTypeFromKeyValueType(Type type) {
        return (type != null) ? Resolver.getValueTypeFromKeyValueType(type) : null;
    }

    public static Type resolveReturnType(Object function) {
        try {
            Class funcInterface;

            if (function instanceof KeyValueMapper) {
                funcInterface = KeyValueMapper.class;
            } else if (function instanceof ValueMapper) {
                funcInterface = ValueMapper.class;
            } else if (function instanceof ValueJoiner) {
                funcInterface = ValueJoiner.class;
            } else {
                return null;
            }

            return Resolver.resolveReturnType(funcInterface, function);

        } catch (TypeException ex) {
            return null;
        }
    }

    public static Type resolveReturnType(Class interfaceClass, String methodName, Object supplier) {
        try {
            Class supplierInterface;

            if (interfaceClass.equals(Transformer.class)) {
                supplierInterface = TransformerSupplier.class;
            } else if (interfaceClass.equals(ValueTransformer.class)) {
                supplierInterface = ValueTransformerSupplier.class;
            } else {
                return null;
            }

            Type implementationType = Resolver.resolveReturnType(supplierInterface, supplier);

            return Resolver.resolveReturnType(interfaceClass, methodName, implementationType);

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

}
