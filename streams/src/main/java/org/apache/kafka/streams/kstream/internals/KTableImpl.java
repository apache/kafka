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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

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

    // TODO: these two fields can be package-private after KStreamBuilder is removed
    public static final String SOURCE_NAME = "KTABLE-SOURCE-";

    public static final String STATE_STORE_NAME = "STATE-STORE-";

    private static final String FILTER_NAME = "KTABLE-FILTER-";

    private static final String JOINTHIS_NAME = "KTABLE-JOINTHIS-";

    private static final String JOINOTHER_NAME = "KTABLE-JOINOTHER-";

    private static final String MAPVALUES_NAME = "KTABLE-MAPVALUES-";

    private static final String MERGE_NAME = "KTABLE-MERGE-";

    private static final String SELECT_NAME = "KTABLE-SELECT-";

    private static final String TOSTREAM_NAME = "KTABLE-TOSTREAM-";

    private final ProcessorSupplier<?, ?> processorSupplier;

    private final String queryableStoreName;
    private final boolean isQueryable;

    private boolean sendOldValues = false;
    private final Serde<K> keySerde;
    private final Serde<V> valSerde;

    public KTableImpl(final InternalStreamsBuilder builder,
                      final String name,
                      final ProcessorSupplier<?, ?> processorSupplier,
                      final Set<String> sourceNodes,
                      final String queryableStoreName,
                      final boolean isQueryable) {
        super(builder, name, sourceNodes);
        this.processorSupplier = processorSupplier;
        this.queryableStoreName = queryableStoreName;
        this.keySerde = null;
        this.valSerde = null;
        this.isQueryable = isQueryable;
    }

    public KTableImpl(final InternalStreamsBuilder builder,
                      final String name,
                      final ProcessorSupplier<?, ?> processorSupplier,
                      final Serde<K> keySerde,
                      final Serde<V> valSerde,
                      final Set<String> sourceNodes,
                      final String queryableStoreName,
                      final boolean isQueryable) {
        super(builder, name, sourceNodes);
        this.processorSupplier = processorSupplier;
        this.queryableStoreName = queryableStoreName;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        this.isQueryable = isQueryable;
    }

    @Override
    public String queryableStoreName() {
        if (!isQueryable) {
            return null;
        }
        return this.queryableStoreName;
    }

    @SuppressWarnings("deprecation")
    private KTable<K, V> doFilter(final Predicate<? super K, ? super V> predicate,
                                  final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier,
                                  final boolean isFilterNot) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        String name = builder.newProcessorName(FILTER_NAME);
        String internalStoreName = null;
        if (storeSupplier != null) {
            internalStoreName = storeSupplier.name();
        }
        KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(this, predicate, isFilterNot, internalStoreName);
        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);
        if (storeSupplier != null) {
            builder.internalTopologyBuilder.addStateStore(storeSupplier, name);
            return new KTableImpl<>(builder, name, processorSupplier, this.keySerde, this.valSerde, sourceNodes, internalStoreName, true);
        } else {
            return new KTableImpl<>(builder, name, processorSupplier, sourceNodes, this.queryableStoreName, false);
        }
    }

    private KTable<K, V> doFilter(final Predicate<? super K, ? super V> predicate,
                                  final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized,
                                  final boolean filterNot) {
        String name = builder.newProcessorName(FILTER_NAME);

        KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(this,
                                                                                predicate,
                                                                                filterNot,
                                                                                materialized.storeName());
        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);

        final StoreBuilder builder = new KeyValueStoreMaterializer<>(materialized).materialize();
        this.builder.internalTopologyBuilder.addStateStore(builder, name);

        return new KTableImpl<>(this.builder,
                                name,
                                processorSupplier,
                                this.keySerde,
                                this.valSerde,
                                sourceNodes,
                                builder.name(),
                                true);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        return filter(predicate, (String) null);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                               final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        return doFilter(predicate, new MaterializedInternal<>(materialized, builder, FILTER_NAME), false);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                               final String queryableStoreName) {
        org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier = null;
        if (queryableStoreName != null) {
            storeSupplier = keyValueStore(this.keySerde, this.valSerde, queryableStoreName);
        }
        return doFilter(predicate, storeSupplier, false);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                               final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doFilter(predicate, storeSupplier, false);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        return filterNot(predicate, (String) null);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        return doFilter(predicate, new MaterializedInternal<>(materialized, builder, FILTER_NAME), true);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final String queryableStoreName) {
        org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier = null;
        if (queryableStoreName != null) {
            storeSupplier = keyValueStore(this.keySerde, this.valSerde, queryableStoreName);
        }
        return doFilter(predicate, storeSupplier, true);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doFilter(predicate, storeSupplier, true);
    }

    @SuppressWarnings("deprecation")
    private <V1> KTable<K, V1> doMapValues(final ValueMapperWithKey<? super K, ? super V, ? extends V1> mapper,
                                           final Serde<V1> valueSerde,
                                           final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(mapper);
        String name = builder.newProcessorName(MAPVALUES_NAME);
        String internalStoreName = null;
        if (storeSupplier != null) {
            internalStoreName = storeSupplier.name();
        }
        KTableProcessorSupplier<K, V, V1> processorSupplier = new KTableMapValues<>(this, mapper, internalStoreName);
        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);
        if (storeSupplier != null) {
            builder.internalTopologyBuilder.addStateStore(storeSupplier, name);
            return new KTableImpl<>(builder, name, processorSupplier, this.keySerde, valueSerde, sourceNodes, internalStoreName, true);
        } else {
            return new KTableImpl<>(builder, name, processorSupplier, sourceNodes, this.queryableStoreName, false);
        }
    }

    @Override
    public <V1> KTable<K, V1> mapValues(final ValueMapper<? super V, ? extends V1> mapper) {
        return doMapValues(withKey(mapper), null, null);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        return doMapValues(mapper, null, null);

    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper,
                                        final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return mapValues(withKey(mapper), materialized);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
                                        final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal
                = new MaterializedInternal<>(materialized, builder, MAPVALUES_NAME);
        final String name = builder.newProcessorName(MAPVALUES_NAME);
        final KTableProcessorSupplier<K, V, VR> processorSupplier = new KTableMapValues<>(
                this,
                mapper,
                materializedInternal.storeName());
        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);
        builder.internalTopologyBuilder.addStateStore(
                new KeyValueStoreMaterializer<>(materializedInternal).materialize(),
                name);
        return new KTableImpl<>(builder, name, processorSupplier, sourceNodes, this.queryableStoreName, true);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <V1> KTable<K, V1> mapValues(final ValueMapper<? super V, ? extends V1> mapper,
                                        final Serde<V1> valueSerde,
                                        final String queryableStoreName) {
        return mapValues(withKey(mapper), Materialized.<K, V1, KeyValueStore<Bytes, byte[]>>as(queryableStoreName).
                withValueSerde(valueSerde).withKeySerde(this.keySerde));
    }

    @SuppressWarnings("deprecation")
    @Override
    public  <V1> KTable<K, V1> mapValues(final ValueMapper<? super V, ? extends V1> mapper,
                                         final Serde<V1> valueSerde,
                                         final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doMapValues(withKey(mapper), valueSerde, storeSupplier);
    }

    @Override
    public KStream<K, V> toStream() {
        String name = builder.newProcessorName(TOSTREAM_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamMapValues<>(new ValueMapperWithKey<K, Change<V>, V>() {
            @Override
            public V apply(final K key, final Change<V> change) {
                return change.newValue;
            }
        }), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, false);
    }

    @Override
    public <K1> KStream<K1, V> toStream(final KeyValueMapper<? super K, ? super V, ? extends K1> mapper) {
        return toStream().selectKey(mapper);
    }

    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, null, false, false);
    }

    @Override
    public <VO, VR> KTable<K, VR> join(final KTable<K, VO> other,
                                       final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                       final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        return doJoin(other, joiner, new MaterializedInternal<>(materialized, builder, MERGE_NAME), false, false);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                     final Serde<R> joinSerde,
                                     final String queryableStoreName) {
        return doJoin(other, joiner, false, false, joinSerde, queryableStoreName);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                     final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, joiner, false, false, storeSupplier);
    }

    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, null, true, true);
    }

    @Override
    public <VO, VR> KTable<K, VR> outerJoin(final KTable<K, VO> other,
                                            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return doJoin(other, joiner, new MaterializedInternal<>(materialized, builder, MERGE_NAME), true, true);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                          final Serde<R> joinSerde,
                                          final String queryableStoreName) {
        return doJoin(other, joiner, true, true, joinSerde, queryableStoreName);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                          final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, joiner, true, true, storeSupplier);
    }

    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, null, true, false);
    }

    @Override
    public <VO, VR> KTable<K, VR> leftJoin(final KTable<K, VO> other,
                                           final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                           final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return doJoin(other,
                      joiner,
                      new MaterializedInternal<>(materialized, builder, MERGE_NAME),
                      true,
                      false);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                         final Serde<R> joinSerde,
                                         final String queryableStoreName) {
        return doJoin(other, joiner, true, false, joinSerde, queryableStoreName);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                         final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, joiner, true, false, storeSupplier);
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    private <V1, R> KTable<K, R> doJoin(final KTable<K, V1> other,
                                        final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                        final boolean leftOuter,
                                        final boolean rightOuter,
                                        final Serde<R> joinSerde,
                                        final String queryableStoreName) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

        final org.apache.kafka.streams.processor.StateStoreSupplier storeSupplier
            = queryableStoreName == null ? null : keyValueStore(this.keySerde, joinSerde, queryableStoreName);

        return doJoin(other, joiner, leftOuter, rightOuter, storeSupplier);
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    private <V1, R> KTable<K, R> doJoin(final KTable<K, V1> other,
                                        final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                        final boolean leftOuter,
                                        final boolean rightOuter,
                                        final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        final String joinMergeName = builder.newProcessorName(MERGE_NAME);
        final String internalQueryableName = storeSupplier == null ? null : storeSupplier.name();
        final KTable<K, R> result = buildJoin((AbstractStream<K>) other,
                                              joiner,
                                              leftOuter,
                                              rightOuter,
                                              joinMergeName,
                                              internalQueryableName);

        if (internalQueryableName != null) {
            builder.internalTopologyBuilder.addStateStore(storeSupplier, joinMergeName);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private <VO, VR> KTable<K, VR> doJoin(final KTable<K, VO> other,
                                          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                          final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
                                          final boolean leftOuter,
                                          final boolean rightOuter) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        final String internalQueryableName = materialized == null ? null : materialized.storeName();
        final String joinMergeName = builder.newProcessorName(MERGE_NAME);
        final KTable<K, VR> result = buildJoin((AbstractStream<K>) other,
                                               joiner,
                                               leftOuter,
                                               rightOuter,
                                               joinMergeName,
                                               internalQueryableName);

        if (materialized != null) {
            final StoreBuilder<KeyValueStore<K, VR>> storeBuilder
                    = new KeyValueStoreMaterializer<>(materialized).materialize();
            builder.internalTopologyBuilder.addStateStore(storeBuilder, joinMergeName);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <V1, R> KTable<K, R> buildJoin(final AbstractStream<K> other,
                                           final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                           final boolean leftOuter,
                                           final boolean rightOuter,
                                           final String joinMergeName,
                                           final String internalQueryableName) {
        final Set<String> allSourceNodes = ensureJoinableWith(other);

        if (leftOuter) {
            enableSendingOldValues();
        }
        if (rightOuter) {
            ((KTableImpl) other).enableSendingOldValues();
        }

        final String joinThisName = builder.newProcessorName(JOINTHIS_NAME);
        final String joinOtherName = builder.newProcessorName(JOINOTHER_NAME);


        final KTableKTableAbstractJoin<K, R, V, V1> joinThis;
        final KTableKTableAbstractJoin<K, R, V1, V> joinOther;

        if (!leftOuter) { // inner
            joinThis = new KTableKTableInnerJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableInnerJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        } else if (!rightOuter) { // left
            joinThis = new KTableKTableLeftJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableRightJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        } else { // outer
            joinThis = new KTableKTableOuterJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableOuterJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        }

        final KTableKTableJoinMerger<K, R> joinMerge = new KTableKTableJoinMerger<>(
                new KTableImpl<K, V, R>(builder, joinThisName, joinThis, sourceNodes, this.queryableStoreName, false),
                new KTableImpl<K, V1, R>(builder, joinOtherName, joinOther, ((KTableImpl<K, ?, ?>) other).sourceNodes,
                        ((KTableImpl<K, ?, ?>) other).queryableStoreName, false),
                internalQueryableName
        );

        builder.internalTopologyBuilder.addProcessor(joinThisName, joinThis, this.name);
        builder.internalTopologyBuilder.addProcessor(joinOtherName, joinOther, ((KTableImpl) other).name);
        builder.internalTopologyBuilder.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);
        builder.internalTopologyBuilder.connectProcessorAndStateStores(joinThisName, ((KTableImpl) other).valueGetterSupplier().storeNames());
        builder.internalTopologyBuilder.connectProcessorAndStateStores(joinOtherName, valueGetterSupplier().storeNames());
        return new KTableImpl<>(builder, joinMergeName, joinMerge, allSourceNodes, internalQueryableName, internalQueryableName != null);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector,
                                                  final Serde<K1> keySerde,
                                                  final Serde<V1> valueSerde) {
        return groupBy(selector, Serialized.with(keySerde, valueSerde));
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector) {
        return this.groupBy(selector, Serialized.<K1, V1>with(null, null));
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector,
                                                  final Serialized<K1, V1> serialized) {
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(serialized, "serialized can't be null");
        String selectName = builder.newProcessorName(SELECT_NAME);

        KTableProcessorSupplier<K, V, KeyValue<K1, V1>> selectSupplier = new KTableRepartitionMap<>(this, selector);

        // select the aggregate key and values (old and new), it would require parent to send old values
        builder.internalTopologyBuilder.addProcessor(selectName, selectSupplier, this.name);
        this.enableSendingOldValues();
        final SerializedInternal<K1, V1> serializedInternal  = new SerializedInternal<>(serialized);
        return new KGroupedTableImpl<>(builder,
                                       selectName,
                                       this.name,
                                       serializedInternal.keySerde(),
                                       serializedInternal.valueSerde());
    }

    @SuppressWarnings("unchecked")
    KTableValueGetterSupplier<K, V> valueGetterSupplier() {
        if (processorSupplier instanceof KTableSource) {
            KTableSource<K, V> source = (KTableSource<K, V>) processorSupplier;
            return new KTableSourceValueGetterSupplier<>(source.storeName);
        } else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
            return ((KStreamAggProcessorSupplier<?, K, S, V>) processorSupplier).view();
        } else {
            return ((KTableProcessorSupplier<K, S, V>) processorSupplier).view();
        }
    }

    @SuppressWarnings("unchecked")
    void enableSendingOldValues() {
        if (!sendOldValues) {
            if (processorSupplier instanceof KTableSource) {
                KTableSource<K, ?> source = (KTableSource<K, V>) processorSupplier;
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

}
