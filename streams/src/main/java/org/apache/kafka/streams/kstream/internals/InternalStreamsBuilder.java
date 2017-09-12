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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class InternalStreamsBuilder {

    final InternalTopologyBuilder internalTopologyBuilder;

    private final AtomicInteger index = new AtomicInteger(0);

    public InternalStreamsBuilder(final InternalTopologyBuilder internalTopologyBuilder) {
        this.internalTopologyBuilder = internalTopologyBuilder;
    }

    public <K, V> KStream<K, V> stream(final Collection<String> topics,
                                       final ConsumedInternal<K, V> consumed) {
        final String name = newName(KStreamImpl.SOURCE_NAME);

        internalTopologyBuilder.addSource(consumed.offsetResetPolicy(),
                                          name,
                                          consumed.timestampExtractor(),
                                          consumed.keySerde() == null ? null : consumed.keySerde().deserializer(),
                                          consumed.valueSerde() == null ? null : consumed.valueSerde().deserializer(),
                                          topics.toArray(new String[topics.size()]));

        return new KStreamImpl<>(this, name, Collections.singleton(name), false);
    }

    public <K, V> KStream<K, V> stream(final Pattern topicPattern, final ConsumedInternal<K, V> consumed) {
        final String name = newName(KStreamImpl.SOURCE_NAME);

        internalTopologyBuilder.addSource(consumed.offsetResetPolicy(),
                                          name,
                                          consumed.timestampExtractor(),
                                          consumed.keySerde() == null ? null : consumed.keySerde().deserializer(),
                                          consumed.valueSerde() == null ? null : consumed.valueSerde().deserializer(),
                                          topicPattern);

        return new KStreamImpl<>(this, name, Collections.singleton(name), false);
    }

    public <K, V> KTable<K, V> table(final String topic,
                                     final ConsumedInternal<K, V> consumed,
                                     final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doTable(consumed, topic, storeSupplier);
    }

    @SuppressWarnings("unchecked")
    public <K, V> KTable<K, V> table(final String topic,
                                     final ConsumedInternal<K, V> consumed,
                                     final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        final StoreBuilder<KeyValueStore<K, V>> storeBuilder = new KeyValueStoreMaterializer<>(materialized).materialize();
        return doTable(consumed, topic, storeBuilder, materialized.isQueryable());
    }

    private <K, V> KTable<K, V> doTable(final ConsumedInternal<K, V> consumed,
                                        final String topic,
                                        final StoreBuilder<KeyValueStore<K, V>> storeBuilder,
                                        final boolean isQueryable) {
        final String source = newName(KStreamImpl.SOURCE_NAME);
        final String name = newName(KTableImpl.SOURCE_NAME);
        final ProcessorSupplier<K, V> processorSupplier = new KTableSource<>(storeBuilder.name());

        internalTopologyBuilder.addSource(consumed.offsetResetPolicy(),
                                          source,
                                          consumed.timestampExtractor(),
                                          consumed.keySerde() == null ? null : consumed.keySerde().deserializer(),
                                          consumed.valueSerde() == null ? null : consumed.valueSerde().deserializer(),
                                          topic);
        internalTopologyBuilder.addProcessor(name, processorSupplier, source);

        final KTableImpl<K, ?, V> kTable = new KTableImpl<>(this, name, processorSupplier,
            consumed.keySerde(), consumed.valueSerde(), Collections.singleton(source), storeBuilder.name(), isQueryable);

        internalTopologyBuilder.addStateStore(storeBuilder, name);
        internalTopologyBuilder.connectSourceStoreAndTopic(storeBuilder.name(), topic);

        return kTable;
    }

    private <K, V> KTable<K, V> doTable(final ConsumedInternal<K, V> consumed,
                                        final String topic,
                                        final StateStoreSupplier<KeyValueStore> storeSupplier) {
        final String source = newName(KStreamImpl.SOURCE_NAME);
        final String name = newName(KTableImpl.SOURCE_NAME);
        final ProcessorSupplier<K, V> processorSupplier = new KTableSource<>(storeSupplier.name());

        internalTopologyBuilder.addSource(consumed.offsetResetPolicy(),
                                          source,
                                          consumed.timestampExtractor(),
                                          consumed.keySerde() == null ? null : consumed.keySerde().deserializer(),
                                          consumed.valueSerde() == null ? null : consumed.valueSerde().deserializer(),
                                          topic);
        internalTopologyBuilder.addProcessor(name, processorSupplier, source);

        final KTableImpl<K, ?, V> kTable = new KTableImpl<>(this,
                                                            name,
                                                            processorSupplier,
                                                            consumed.keySerde(),
                                                            consumed.valueSerde(),
                                                            Collections.singleton(source),
                                                            storeSupplier.name(),
                                                            true);

        internalTopologyBuilder.addStateStore(storeSupplier, name);
        internalTopologyBuilder.connectSourceStoreAndTopic(storeSupplier.name(), topic);

        return kTable;
    }

    public <K, V> GlobalKTable<K, V> globalTable(final String topic,
                                                 final ConsumedInternal<K, V> consumed,
                                                 final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(consumed, "consumed can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        // explicitly disable logging for global stores
        materialized.withLoggingDisabled();
        final StoreBuilder storeBuilder = new KeyValueStoreMaterializer<>(materialized).materialize();
        final String sourceName = newName(KStreamImpl.SOURCE_NAME);
        final String processorName = newName(KTableImpl.SOURCE_NAME);
        final KTableSource<K, V> tableSource = new KTableSource<>(storeBuilder.name());


        final Deserializer<K> keyDeserializer = consumed.keySerde() == null ? null : consumed.keySerde().deserializer();
        final Deserializer<V> valueDeserializer = consumed.valueSerde() == null ? null : consumed.valueSerde().deserializer();

        internalTopologyBuilder.addGlobalStore(storeBuilder,
                                               sourceName,
                                               consumed.timestampExtractor(),
                                               keyDeserializer,
                                               valueDeserializer,
                                               topic,
                                               processorName,
                                               tableSource);
        return new GlobalKTableImpl<>(new KTableSourceValueGetterSupplier<K, V>(storeBuilder.name()));
    }


    public <K, V> KStream<K, V> merge(final KStream<K, V>... streams) {
        return KStreamImpl.merge(this, streams);
    }

    String newName(final String prefix) {
        return prefix + String.format("%010d", index.getAndIncrement());
    }

    public String newStoreName(final String prefix) {
        return prefix + String.format(KTableImpl.STATE_STORE_NAME + "%010d", index.getAndIncrement());
    }

    public synchronized void addStateStore(final StoreBuilder builder,
                                           final String... processorNames) {
        internalTopologyBuilder.addStateStore(builder, processorNames);
    }

    public synchronized void addGlobalStore(final StateStoreSupplier<KeyValueStore> storeSupplier,
                                            final String sourceName,
                                            final Deserializer keyDeserializer,
                                            final Deserializer valueDeserializer,
                                            final String topic,
                                            final String processorName,
                                            final ProcessorSupplier stateUpdateSupplier) {
        internalTopologyBuilder.addGlobalStore(storeSupplier, sourceName, null, keyDeserializer,
            valueDeserializer, topic, processorName, stateUpdateSupplier);
    }

    public synchronized void addGlobalStore(final StoreBuilder<KeyValueStore> storeBuilder,
                                            final String sourceName,
                                            final String topic,
                                            final ConsumedInternal consumed,
                                            final String processorName,
                                            final ProcessorSupplier stateUpdateSupplier) {
        // explicitly disable logging for global stores
        storeBuilder.withLoggingDisabled();
        final Deserializer keyDeserializer = consumed.keySerde() == null ? null : consumed.keySerde().deserializer();
        final Deserializer valueDeserializer = consumed.valueSerde() == null ? null : consumed.valueSerde().deserializer();
        internalTopologyBuilder.addGlobalStore(storeBuilder,
                                               sourceName,
                                               consumed.timestampExtractor(),
                                               keyDeserializer,
                                               valueDeserializer,
                                               topic,
                                               processorName,
                                               stateUpdateSupplier);
    }
}
