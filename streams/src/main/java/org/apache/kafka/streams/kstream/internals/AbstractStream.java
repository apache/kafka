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
import org.apache.kafka.streams.kstream.ValueJoiner;
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

        if (this.keyType == null || other.keyType == null)
            throw new InsufficientTypeInfoException();

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
        return getSerializer(type, null);
    }

    protected <T> Serializer<T> getSerializer(Type type, Serializer<T> defaultSerializer) {

        if (type == null)
            throw new InsufficientTypeInfoException();

        Serializer<T> serializer = topology.getSerializer(type);

        if (serializer == null)
            serializer = defaultSerializer;

        if (serializer == null)
            throw new TopologyException("unable to find a serializer for a type " + type);

        return serializer;
    }

    protected <T> Deserializer<T> getDeserializer(Type type) {
        return getDeserializer(type, null);
    }

    protected <T> Deserializer<T> getDeserializer(Type type, Deserializer<T> defaultDeserializer) {

        if (type == null)
            throw new InsufficientTypeInfoException();

        Deserializer<T> deserializer = topology.getDeserializer(type);

        if (deserializer == null)
            deserializer = defaultDeserializer;

        if (deserializer == null)
            throw new TopologyException("unable to find a serializer for a type " + type);

        return deserializer;
    }

}
