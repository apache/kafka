/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.internals.RocksDBKeyValueStoreSupplier;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * The implementation class of {@link KTable}.
 *
 * @param <K> the key type
 * @param <S> the source's (parent's) value type
 * @param <V> the value type
 */
public class KTableImpl<K, S, V> extends AbstractStream<K> implements KTable<K, V> {

    private static final String FILTER_NAME = "KTABLE-FILTER-";

    private static final String FOREACH_NAME = "KTABLE-FOREACH-";

    private static final String JOINTHIS_NAME = "KTABLE-JOINTHIS-";

    private static final String JOINOTHER_NAME = "KTABLE-JOINOTHER-";

    private static final String LEFTTHIS_NAME = "KTABLE-LEFTTHIS-";

    private static final String LEFTOTHER_NAME = "KTABLE-LEFTOTHER-";

    private static final String MAPVALUES_NAME = "KTABLE-MAPVALUES-";

    private static final String MERGE_NAME = "KTABLE-MERGE-";

    private static final String OUTERTHIS_NAME = "KTABLE-OUTERTHIS-";

    private static final String OUTEROTHER_NAME = "KTABLE-OUTEROTHER-";

    private static final String PRINTING_NAME = "KSTREAM-PRINTER-";

    private static final String SELECT_NAME = "KTABLE-SELECT-";

    public static final String SOURCE_NAME = "KTABLE-SOURCE-";

    private static final String TOSTREAM_NAME = "KTABLE-TOSTREAM-";

    private final ProcessorSupplier<?, ?> processorSupplier;

    private final Serde<K> keySerde;
    private final Serde<V> valSerde;
    private final String storeName;

    private boolean sendOldValues = false;


    public KTableImpl(final KStreamBuilder topology,
                      final String name,
                      final ProcessorSupplier<?, ?> processorSupplier,
                      final Set<String> sourceNodes,
                      final String storeName) {
        this(topology, name, processorSupplier, sourceNodes, null, null, storeName);
    }

    public KTableImpl(final KStreamBuilder topology,
                      final String name,
                      final ProcessorSupplier<?, ?> processorSupplier,
                      final Set<String> sourceNodes,
                      final Serde<K> keySerde,
                      final Serde<V> valSerde,
                      final String storeName) {
        super(topology, name, sourceNodes);
        this.processorSupplier = processorSupplier;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        this.storeName = storeName;
    }

    @Override
    public String getStoreName() {
        return storeName;
    }

    @Override
    public KTable<K, V> filter(final Predicate<K, V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        final String name = topology.newName(FILTER_NAME);
        final KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(this, predicate, false);
        topology.addProcessor(name, processorSupplier, this.name);

        return new KTableImpl<>(topology, name, processorSupplier, sourceNodes, storeName);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<K, V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        final String name = topology.newName(FILTER_NAME);
        final KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(this, predicate, true);

        topology.addProcessor(name, processorSupplier, this.name);

        return new KTableImpl<>(topology, name, processorSupplier, sourceNodes, storeName);
    }

    @Override
    public <V1> KTable<K, V1> mapValues(final ValueMapper<V, V1> mapper) {
        Objects.requireNonNull(mapper);
        final String name = topology.newName(MAPVALUES_NAME);
        final KTableProcessorSupplier<K, V, V1> processorSupplier = new KTableMapValues<>(this, mapper);

        topology.addProcessor(name, processorSupplier, this.name);

        return new KTableImpl<>(topology, name, processorSupplier, sourceNodes, storeName);
    }

    @Override
    public void print() {
        print(null, null, null);
    }

    @Override
    public void print(final String streamName) {
        print(null, null, streamName);
    }

    @Override
    public void print(final Serde<K> keySerde, final Serde<V> valSerde) {
        print(keySerde, valSerde, null);
    }


    @Override
    public void print(final Serde<K> keySerde, final Serde<V> valSerde, String streamName) {
        final String name = topology.newName(PRINTING_NAME);
        streamName = (streamName == null) ? this.name : streamName;
        topology.addProcessor(name, new KeyValuePrinter<>(keySerde, valSerde, streamName), this.name);
    }

    @Override
    public void writeAsText(final String filePath) {
        writeAsText(filePath, null, null, null);
    }

    @Override
    public void writeAsText(final String filePath, final String streamName) {
        writeAsText(filePath, streamName, null, null);
    }

    @Override
    public void writeAsText(final String filePath, final Serde<K> keySerde, final Serde<V> valSerde) {
        writeAsText(filePath, null, keySerde, valSerde);
    }

    /**
     * @throws TopologyBuilderException if file is not found
     */
    @Override
    public void writeAsText(final String filePath, String streamName, final Serde<K> keySerde, final Serde<V> valSerde) {
        final String name = topology.newName(PRINTING_NAME);
        streamName = (streamName == null) ? this.name : streamName;
        try {
            final PrintStream printStream = new PrintStream(new FileOutputStream(filePath));
            topology.addProcessor(name, new KeyValuePrinter<>(printStream, keySerde, valSerde, streamName), this.name);
        } catch (final FileNotFoundException e) {
            final String message = "Unable to write stream to file at [" + filePath + "] " + e.getMessage();
            throw new TopologyBuilderException(message);
        }
    }

    @Override
    public void foreach(final ForeachAction<K, V> action) {
        Objects.requireNonNull(action, "action can't be null");
        final String name = topology.newName(FOREACH_NAME);
        final KStreamForeach<K, Change<V>> processorSupplier = new KStreamForeach<>(new ForeachAction<K, Change<V>>() {
            @Override
            public void apply(final K key, final Change<V> value) {
                action.apply(key, value.newValue);
            }
        });
        topology.addProcessor(name, processorSupplier, this.name);
    }

    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final StreamPartitioner<K, V> partitioner,
                                final String topic,
                                final String storeName) {
        Objects.requireNonNull(storeName, "storeName can't be null");
        to(keySerde, valSerde, partitioner, topic);

        return topology.table(keySerde, valSerde, topic, storeName);
    }

    @Override
    public KTable<K, V> through(final Serde<K> keySerde, final Serde<V> valSerde, final String topic, final String storeName) {
        return through(keySerde, valSerde, null, topic, storeName);
    }

    @Override
    public KTable<K, V> through(final StreamPartitioner<K, V> partitioner, final String topic, final String storeName) {
        return through(null, null, partitioner, topic, storeName);
    }

    @Override
    public KTable<K, V> through(final String topic, final String storeName) {
        return through(null, null, null, topic, storeName);
    }

    @Override
    public void to(final String topic) {
        to(null, null, null, topic);
    }

    @Override
    public void to(final StreamPartitioner<K, V> partitioner, final String topic) {
        to(null, null, partitioner, topic);
    }

    @Override
    public void to(final Serde<K> keySerde, final Serde<V> valSerde, final String topic) {
        toStream().to(keySerde, valSerde, null, topic);
    }

    @Override
    public void to(final Serde<K> keySerde, final Serde<V> valSerde, final StreamPartitioner<K, V> partitioner, final String topic) {
        toStream().to(keySerde, valSerde, partitioner, topic);
    }

    @Override
    public KStream<K, V> toStream() {
        final String name = topology.newName(TOSTREAM_NAME);

        topology.addProcessor(name, new KStreamMapValues<K, Change<V>, V>(new ValueMapper<Change<V>, V>() {
            @Override
            public V apply(final Change<V> change) {
                return change.newValue;
            }
        }), this.name);

        return new KStreamImpl<>(topology, name, sourceNodes, false);
    }

    @Override
    public <K1> KStream<K1, V> toStream(final KeyValueMapper<K, V, K1> mapper) {
        return toStream().selectKey(mapper);
    }

    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other, final ValueJoiner<V, V1, R> joiner) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

        final Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K>) other);

        final String joinThisName = topology.newName(JOINTHIS_NAME);
        final String joinOtherName = topology.newName(JOINOTHER_NAME);
        final String joinMergeName = topology.newName(MERGE_NAME);

        final KTableKTableJoin<K, R, V, V1> joinThis = new KTableKTableJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
        final KTableKTableJoin<K, R, V1, V> joinOther = new KTableKTableJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        final KTableKTableJoinMerger<K, R> joinMerge = new KTableKTableJoinMerger<>(
            new KTableImpl<K, V, R>(topology, joinThisName, joinThis, sourceNodes, storeName),
            new KTableImpl<K, V1, R>(topology, joinOtherName, joinOther, ((KTableImpl<K, ?, ?>) other).sourceNodes, other.getStoreName())
        );

        topology.addProcessor(joinThisName, joinThis, name);
        topology.addProcessor(joinOtherName, joinOther, ((KTableImpl) other).name);
        topology.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);
        topology.connectProcessorAndStateStores(joinThisName, ((KTableImpl) other).valueGetterSupplier().storeNames());
        topology.connectProcessorAndStateStores(joinOtherName, valueGetterSupplier().storeNames());

        return new KTableImpl<>(topology, joinMergeName, joinMerge, allSourceNodes, null);
    }

    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other, final ValueJoiner<V, V1, R> joiner) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

        final Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K>) other);
        enableSendingOldValues();
        ((KTableImpl) other).enableSendingOldValues();

        final String joinThisName = topology.newName(OUTERTHIS_NAME);
        final String joinOtherName = topology.newName(OUTEROTHER_NAME);
        final String joinMergeName = topology.newName(MERGE_NAME);

        final KTableKTableOuterJoin<K, R, V, V1> joinThis = new KTableKTableOuterJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
        final KTableKTableOuterJoin<K, R, V1, V> joinOther = new KTableKTableOuterJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        final KTableKTableJoinMerger<K, R> joinMerge = new KTableKTableJoinMerger<>(
            new KTableImpl<K, V, R>(topology, joinThisName, joinThis, sourceNodes, storeName),
            new KTableImpl<K, V1, R>(topology, joinOtherName, joinOther, ((KTableImpl<K, ?, ?>) other).sourceNodes, other.getStoreName())
        );

        topology.addProcessor(joinThisName, joinThis, name);
        topology.addProcessor(joinOtherName, joinOther, ((KTableImpl) other).name);
        topology.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);
        topology.connectProcessorAndStateStores(joinThisName, ((KTableImpl) other).valueGetterSupplier().storeNames());
        topology.connectProcessorAndStateStores(joinOtherName, valueGetterSupplier().storeNames());

        return new KTableImpl<>(topology, joinMergeName, joinMerge, allSourceNodes, null);
    }

    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other, final ValueJoiner<V, V1, R> joiner) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

        final Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K>) other);
        enableSendingOldValues();

        final String joinThisName = topology.newName(LEFTTHIS_NAME);
        final String joinOtherName = topology.newName(LEFTOTHER_NAME);
        final String joinMergeName = topology.newName(MERGE_NAME);

        final KTableKTableLeftJoin<K, R, V, V1> joinThis = new KTableKTableLeftJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
        final KTableKTableRightJoin<K, R, V1, V> joinOther = new KTableKTableRightJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        final KTableKTableJoinMerger<K, R> joinMerge = new KTableKTableJoinMerger<>(
            new KTableImpl<K, V, R>(topology, joinThisName, joinThis, sourceNodes, storeName),
            new KTableImpl<K, V1, R>(topology, joinOtherName, joinOther, ((KTableImpl<K, ?, ?>) other).sourceNodes, other.getStoreName())
        );

        topology.addProcessor(joinThisName, joinThis, name);
        topology.addProcessor(joinOtherName, joinOther, ((KTableImpl) other).name);
        topology.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);
        topology.connectProcessorAndStateStores(joinThisName, ((KTableImpl) other).valueGetterSupplier().storeNames());
        topology.connectProcessorAndStateStores(joinOtherName, valueGetterSupplier().storeNames());

        return new KTableImpl<>(topology, joinMergeName, joinMerge, allSourceNodes, null);
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                                  final Serde<K1> keySerde,
                                                  final Serde<V1> valueSerde) {

        Objects.requireNonNull(selector, "selector can't be null");
        final String selectName = topology.newName(SELECT_NAME);

        final KTableProcessorSupplier<K, V, KeyValue<K1, V1>> selectSupplier = new KTableRepartitionMap<>(this, selector);

        // select the aggregate key and values (old and new), it would require parent to send old values
        topology.addProcessor(selectName, selectSupplier, name);
        enableSendingOldValues();

        return new KGroupedTableImpl<>(topology, selectName, name, keySerde, valueSerde);
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<K, V, KeyValue<K1, V1>> selector) {
        return groupBy(selector, null, null);
    }

    KTableValueGetterSupplier<K, V> valueGetterSupplier() {
        if (processorSupplier instanceof KTableSource) {
            final KTableSource<K, V> source = (KTableSource<K, V>) processorSupplier;
            if (!source.isMaterialized()) {
                throw new StreamsException("Source is not materialized");
            }
            return new KTableSourceValueGetterSupplier<>(source.storeName);
        } else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
            return ((KStreamAggProcessorSupplier<?, K, S, V>) processorSupplier).view();
        } else {
            return ((KTableProcessorSupplier<K, S, V>) processorSupplier).view();
        }
    }

    void enableSendingOldValues() {
        if (!sendOldValues) {
            if (processorSupplier instanceof KTableSource) {
                final KTableSource<K, ?> source = (KTableSource<K, V>) processorSupplier;
                if (!source.isMaterialized()) {
                    throw new StreamsException("Source is not materialized");
                }
                source.enableSendingOldValues();
            } else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
                ((KStreamAggProcessorSupplier<?, K, S, V>) processorSupplier).enableSendingOldValues();
            } else {
                ((KTableProcessorSupplier<K, S, V>) processorSupplier).enableSendingOldValues();
            }
            sendOldValues = true;
        }
    }

    boolean sendingOldValueEnabled() {
        return sendOldValues;
    }

    public void materialize(final KTableSource<K, ?> source) {
        synchronized (source) {
            if (!source.isMaterialized()) {
                final StateStoreSupplier storeSupplier = new RocksDBKeyValueStoreSupplier<>(source.storeName,
                    keySerde,
                    valSerde,
                    false,
                    Collections.<String, String>emptyMap(),
                    true);
                // mark this state as non internal hence it is read directly from a user topic
                topology.addStateStore(storeSupplier, name);
                source.materialize();
            }
        }
    }
}
