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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.RocksDBKeyValueStoreSupplier;

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

    public <K, V> KStream<K, V> stream(final Topology.AutoOffsetReset offsetReset,
                                                                  final TimestampExtractor timestampExtractor,
                                                                  final Serde<K> keySerde,
                                                                  final Serde<V> valSerde,
                                                                  final String... topics) {
        final String name = newName(KStreamImpl.SOURCE_NAME);

        internalTopologyBuilder.addSource(offsetReset, name, timestampExtractor, keySerde == null ? null : keySerde.deserializer(), valSerde == null ? null : valSerde.deserializer(), topics);

        return new KStreamImpl<>(this, name, Collections.singleton(name), false);
    }

    public <K, V> KStream<K, V> stream(final Topology.AutoOffsetReset offsetReset,
                                       final TimestampExtractor timestampExtractor,
                                       final Serde<K> keySerde,
                                       final Serde<V> valSerde,
                                       final Pattern topicPattern) {
        final String name = newName(KStreamImpl.SOURCE_NAME);

        internalTopologyBuilder.addSource(offsetReset, name, timestampExtractor, keySerde == null ? null : keySerde.deserializer(), valSerde == null ? null : valSerde.deserializer(), topicPattern);

        return new KStreamImpl<>(this, name, Collections.singleton(name), false);
    }

    @SuppressWarnings("unchecked")
    public <K, V> KTable<K, V> table(final Topology.AutoOffsetReset offsetReset,
                                     final TimestampExtractor timestampExtractor,
                                     final Serde<K> keySerde,
                                     final Serde<V> valSerde,
                                     final String topic,
                                     final String queryableStoreName) {
        final String internalStoreName = queryableStoreName != null ? queryableStoreName : newStoreName(KTableImpl.SOURCE_NAME);
        final StateStoreSupplier storeSupplier = new RocksDBKeyValueStoreSupplier<>(internalStoreName,
            keySerde,
            valSerde,
            false,
            Collections.<String, String>emptyMap(),
            true);
        return doTable(offsetReset, keySerde, valSerde, timestampExtractor, topic, storeSupplier, queryableStoreName != null);
    }

    public <K, V> KTable<K, V> table(final Topology.AutoOffsetReset offsetReset,
                                     final TimestampExtractor timestampExtractor,
                                     final Serde<K> keySerde,
                                     final Serde<V> valSerde,
                                     final String topic,
                                     final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doTable(offsetReset, keySerde, valSerde, timestampExtractor, topic, storeSupplier, true);
    }

    private <K, V> KTable<K, V> doTable(final Topology.AutoOffsetReset offsetReset,
                                        final Serde<K> keySerde,
                                        final Serde<V> valSerde,
                                        final TimestampExtractor timestampExtractor,
                                        final String topic,
                                        final StateStoreSupplier<KeyValueStore> storeSupplier,
                                        final boolean isQueryable) {
        final String source = newName(KStreamImpl.SOURCE_NAME);
        final String name = newName(KTableImpl.SOURCE_NAME);
        final ProcessorSupplier<K, V> processorSupplier = new KTableSource<>(storeSupplier.name());

        internalTopologyBuilder.addSource(offsetReset, source, timestampExtractor, keySerde == null ? null : keySerde.deserializer(),
            valSerde == null ? null : valSerde.deserializer(),
            topic);
        internalTopologyBuilder.addProcessor(name, processorSupplier, source);

        final KTableImpl<K, ?, V> kTable = new KTableImpl<>(this, name, processorSupplier,
            keySerde, valSerde, Collections.singleton(source), storeSupplier.name(), isQueryable);

        internalTopologyBuilder.addStateStore(storeSupplier, name);
        internalTopologyBuilder.connectSourceStoreAndTopic(storeSupplier.name(), topic);

        return kTable;
    }

    public <K, V> GlobalKTable<K, V> globalTable(final Serde<K> keySerde,
                                                 final Serde<V> valSerde,
                                                 final TimestampExtractor timestampExtractor,
                                                 final String topic,
                                                 final String queryableStoreName) {
        final String internalStoreName = queryableStoreName != null ? queryableStoreName : newStoreName(KTableImpl.SOURCE_NAME);
        return doGlobalTable(keySerde, valSerde, timestampExtractor, topic, new RocksDBKeyValueStoreSupplier<>(internalStoreName,
            keySerde,
            valSerde,
            false,
            Collections.<String, String>emptyMap(),
            true));
    }

    public <K, V> GlobalKTable<K, V> globalTable(final Serde<K> keySerde,
                                                 final Serde<V> valSerde,
                                                 final String topic,
                                                 final StateStoreSupplier<KeyValueStore> storeSupplier) {
        return doGlobalTable(keySerde, valSerde, null, topic, storeSupplier);
    }

    @SuppressWarnings("unchecked")
    private <K, V> GlobalKTable<K, V> doGlobalTable(final Serde<K> keySerde,
                                                    final Serde<V> valSerde,
                                                    final TimestampExtractor timestampExtractor,
                                                    final String topic,
                                                    final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        final String sourceName = newName(KStreamImpl.SOURCE_NAME);
        final String processorName = newName(KTableImpl.SOURCE_NAME);
        final KTableSource<K, V> tableSource = new KTableSource<>(storeSupplier.name());


        final Deserializer<K> keyDeserializer = keySerde == null ? null : keySerde.deserializer();
        final Deserializer<V> valueDeserializer = valSerde == null ? null : valSerde.deserializer();

        internalTopologyBuilder.addGlobalStore(storeSupplier, sourceName, timestampExtractor, keyDeserializer, valueDeserializer, topic, processorName, tableSource);
        return new GlobalKTableImpl(new KTableSourceValueGetterSupplier<>(storeSupplier.name()));
    }

    public <K, V> KStream<K, V> merge(final KStream<K, V>... streams) {
        return KStreamImpl.merge(this, streams);
    }

    String newName(final String prefix) {
        return prefix + String.format("%010d", index.getAndIncrement());
    }

    String newStoreName(final String prefix) {
        return prefix + String.format(KTableImpl.STATE_STORE_NAME + "%010d", index.getAndIncrement());
    }

    public synchronized void addStateStore(final StateStoreSupplier supplier,
                                           final String... processorNames) {
        internalTopologyBuilder.addStateStore(supplier, processorNames);
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

    public synchronized void addGlobalStore(final StateStoreSupplier<KeyValueStore> storeSupplier,
                                            final String sourceName,
                                            final TimestampExtractor timestampExtractor,
                                            final Deserializer keyDeserializer,
                                            final Deserializer valueDeserializer,
                                            final String topic,
                                            final String processorName,
                                            final ProcessorSupplier stateUpdateSupplier) {
        internalTopologyBuilder.addGlobalStore(storeSupplier, sourceName, timestampExtractor, keyDeserializer,
            valueDeserializer, topic, processorName, stateUpdateSupplier);
    }

}
