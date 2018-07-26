/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public abstract class AbstractStream<K> {

    protected final InternalStreamsBuilder builder;
    protected final String name;
    protected final Set<String> sourceNodes;

    // This copy-constructor will allow to extend KStream
    // and KTable APIs with new methods without impacting the public interface.
    public AbstractStream(AbstractStream<K> stream) {
        this.builder = stream.builder;
        this.name = stream.name;
        this.sourceNodes = stream.sourceNodes;
    }

    AbstractStream(final InternalStreamsBuilder builder, String name, final Set<String> sourceNodes) {
        if (sourceNodes == null || sourceNodes.isEmpty()) {
            throw new IllegalArgumentException("parameter <sourceNodes> must not be null or empty");
        }

        this.builder = builder;
        this.name = name;
        this.sourceNodes = sourceNodes;
    }

    // This method allows to expose the InternalTopologyBuilder instance
    // to subclasses that extend AbstractStream class.
    protected InternalTopologyBuilder internalTopologyBuilder() {
        return builder.internalTopologyBuilder;
    }

    Set<String> ensureJoinableWith(final AbstractStream<K> other) {
        Set<String> allSourceNodes = new HashSet<>();
        allSourceNodes.addAll(sourceNodes);
        allSourceNodes.addAll(other.sourceNodes);

        builder.internalTopologyBuilder.copartitionSources(allSourceNodes);

        return allSourceNodes;
    }

    static <T2, T1, R> ValueJoiner<T2, T1, R> reverseJoiner(final ValueJoiner<T1, T2, R> joiner) {
        return new ValueJoiner<T2, T1, R>() {
            @Override
            public R apply(T2 value2, T1 value1) {
                return joiner.apply(value1, value2);
            }
        };
    }

    static <K, V, VR> ValueMapperWithKey<K, V, VR> withKey(final ValueMapper<V, VR> valueMapper) {
        Objects.requireNonNull(valueMapper, "valueMapper can't be null");
        return new ValueMapperWithKey<K, V, VR>() {
            @Override
            public VR apply(final K readOnlyKey, final V value) {
                return valueMapper.apply(value);
            }
        };
    }

    static <K, V, VR> ValueTransformerWithKeySupplier<K, V, VR> toValueTransformerWithKeySupplier(final ValueTransformerSupplier<V, VR> valueTransformerSupplier) {
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformerSupplier can't be null");
        return new ValueTransformerWithKeySupplier<K, V, VR>() {
            @Override
            public ValueTransformerWithKey<K, V, VR> get() {
                final ValueTransformer<V, VR> valueTransformer = valueTransformerSupplier.get();
                return new ValueTransformerWithKey<K, V, VR>() {
                    @Override
                    public void init(final ProcessorContext context) {
                        valueTransformer.init(context);
                    }

                    @Override
                    public VR transform(final K readOnlyKey, final V value) {
                        return valueTransformer.transform(value);
                    }

                    @Override
                    public void close() {
                        valueTransformer.close();
                    }
                };
            }
        };
    }
}
