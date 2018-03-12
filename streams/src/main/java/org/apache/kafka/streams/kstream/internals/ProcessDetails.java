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


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Class used to hold the implementation details when making calls to inheritors of
 * {@link AbstractStream} when building a topology
 */
@SuppressWarnings("deprecation")
public class ProcessDetails<K, V, S extends StateStore> {

    private final Collection<String> sourceTopics;
    private final String sinkTopic;
    private final Pattern sourcePattern;
    private final Produced<K, V> produced;
    private final Consumed<K, V> consumed;
    private final Materialized<K, V, S> materialized;
    private final ProcessorSupplier<K, V> processorSupplier;
    private MaterializedInternal<K, V, S> materializedInternal;
    private ProducedInternal<K, V> producedInternal;
    private ConsumedInternal<K, V> consumedInternal;
    private final StateStoreSupplier<KeyValueStore<K, V>> storeSupplier;
    private final StoreBuilder<KeyValueStore<K, V>> storeBuilder;
    private final KTableSource<K, V> kTableSource;


    private ProcessDetails(final Collection<String> sourceTopics,
                           final String sinkTopic,
                           final Pattern sourcePattern,
                           final Produced<K, V> produced,
                           final Consumed<K, V> consumed,
                           final Materialized<K, V, S> materialized,
                           final ProcessorSupplier<K, V> processorSupplier,
                           final StateStoreSupplier<KeyValueStore<K, V>> storeSupplier,
                           final StoreBuilder<KeyValueStore<K, V>> storeBuilder,
                           final KTableSource<K, V> kTableSource) {
        this.sourceTopics = sourceTopics;
        this.sinkTopic = sinkTopic;
        this.sourcePattern = sourcePattern;
        this.produced = produced;
        this.consumed = consumed;
        this.materialized = materialized;
        this.processorSupplier = processorSupplier;
        this.storeSupplier = storeSupplier;
        this.storeBuilder = storeBuilder;
        this.kTableSource = kTableSource;
        if (this.materialized != null) {
            this.materializedInternal = new MaterializedInternal(materialized);
        }

        if (produced != null) {
            producedInternal = new ProducedInternal<>(produced);
        }

        if (consumed != null) {
            consumedInternal = new ConsumedInternal<>(consumed);
        }
    }

    public Collection<String> getSourceTopics() {
        final Set<String> copy = new HashSet<>(sourceTopics);
        return Collections.unmodifiableSet(copy);
    }

    public String[] getSourceTopicArray() {
        final String[] topics = new String[sourceTopics.size()];
        sourceTopics.toArray(topics);
        return topics;
    }

    public String getSinkTopic() {
        return sinkTopic;
    }

    public Pattern getSourcePattern() {
        return sourcePattern;
    }

    public Produced<K, V> getProduced() {
        return produced;
    }

    public Consumed<K, V> getConsumed() {
        return consumed;
    }

    public Serde<K> producedKeySerde() {
        return producedInternal.keySerde();
    }

    public Serde<V> producedValueSerde() {
        return producedInternal.valueSerde();
    }

    public StreamPartitioner<? super K, ? super   V> streamPartitioner() {

        final Serializer<K> keySerializer = producedInternal.keySerde() == null ? null : producedInternal.keySerde().serializer();
        StreamPartitioner<? super K, ? super V> partitioner = producedInternal.streamPartitioner();
        if (partitioner == null && keySerializer != null && keySerializer instanceof WindowedSerializer) {
            final WindowedSerializer<Object> windowedSerializer = (WindowedSerializer<Object>) keySerializer;
            partitioner = (StreamPartitioner<K, V>) new WindowedStreamPartitioner<Object, V>(sinkTopic, windowedSerializer);

        }
        return partitioner;
    }

    public Serde<K> consumedKeySerde() {
        return consumedInternal.keySerde();
    }

    public Topology.AutoOffsetReset getConsumedResetPolicy() {
        return consumedInternal.offsetResetPolicy();
    }

    public TimestampExtractor getConsumedTimestampExtractor() {
        return consumedInternal.timestampExtractor();
    }

    public Serde<V> consumedValueSerde() {
        return consumedInternal.valueSerde();
    }

    public Materialized<K, V, S> getMaterialized() {
        return materialized;
    }

    public MaterializedInternal<K, V, S> getMaterializedInternal() {
        return materializedInternal;
    }

    public ProcessorSupplier<K, V> getProcessorSupplier() {
        return processorSupplier;
    }

    public StateStoreSupplier<KeyValueStore<K, V>> getStoreSupplier() {
        return storeSupplier;
    }

    public StoreBuilder<KeyValueStore<K, V>> getStoreBuilder() {
        return storeBuilder;
    }

    public static Builder builder() {
        return new Builder<>();
    }

    public static final class Builder<K, V, S extends StateStore> {

        private Collection<String> sourceTopics;
        private String sinkTopic;
        private Pattern sourcePattern;
        private Produced<K, V> produced;
        private Consumed<K, V> consumed;
        private Materialized<K, V, S> materialized;
        private ProcessorSupplier<K, V> processorSupplier;
        private StateStoreSupplier<KeyValueStore<K, V>> storeSupplier;
        private StoreBuilder<KeyValueStore<K, V>> storeBuilder;
        private KTableSource<K, V> kTableSource;

        private Builder() {
        }

        public Builder withSourceTopics(Collection<String> sourceTopics) {
            this.sourceTopics = sourceTopics;
            return this;
        }

        public Builder withSinkTopic(String sinkTopic) {
            this.sinkTopic = sinkTopic;
            return this;
        }

        public Builder withSourcePattern(Pattern sourcePattern) {
            this.sourcePattern = sourcePattern;
            return this;
        }

        public Builder withProduced(Produced<K, V> produced) {
            this.produced = produced;
            return this;
        }

        public Builder withConsumed(Consumed<K, V> consumed) {
            this.consumed = consumed;
            return this;
        }

        public Builder withMaterialized(Materialized<K, V, S> materialized) {
            this.materialized = materialized;
            return this;
        }

        public Builder withProcessorSupplier(ProcessorSupplier<K, V> processorSupplier) {
            this.processorSupplier = processorSupplier;
            return this;
        }

        public Builder withStoreSupplier(StateStoreSupplier<KeyValueStore<K, V>> storeSupplier) {
            this.storeSupplier = storeSupplier;
            return this;
        }

        public Builder withStoreBuilder(StoreBuilder<KeyValueStore<K, V>> storeBuilder) {
            this.storeBuilder = storeBuilder;
            return this;
        }

        public Builder withKTableSource(KTableSource<K, V> kTableSource) {
            this.kTableSource = kTableSource;
            return this;
        }

        public ProcessDetails<K, V, S> build() {
            return new ProcessDetails<>(sourceTopics,
                                        sinkTopic,
                                        sourcePattern,
                                        produced,
                                        consumed,
                                        materialized,
                                        processorSupplier,
                                        storeSupplier,
                                        storeBuilder,
                                        kTableSource);
        }
    }
}
