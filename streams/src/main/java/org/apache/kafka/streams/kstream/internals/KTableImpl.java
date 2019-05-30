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
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKey;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKeyForeignKeyPartitioner;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKeySerde;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.ForeignKeySingleLookupProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.KTableKTablePrefixScanProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.KTableRepartitionerProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionResolverJoinProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionResponseWrapper;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionResponseWrapperSerde;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapperSerde;
import org.apache.kafka.streams.kstream.internals.graph.KTableKTableForeignKeyJoinResolutionNode;
import org.apache.kafka.streams.kstream.internals.graph.KTableKTableJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.TableProcessorNode;
import org.apache.kafka.streams.kstream.internals.suppress.FinalResultsSuppressionBuilder;
import org.apache.kafka.streams.kstream.internals.suppress.KTableSuppressProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.suppress.NamedSuppressed;
import org.apache.kafka.streams.kstream.internals.suppress.SuppressedInternal;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.streams.state.internals.RocksDbKeyValueBytesStoreSupplier;

import java.time.Duration;
import java.util.Collections;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.streams.kstream.internals.graph.GraphGraceSearchUtil.findAndVerifyWindowGrace;

/**
 * The implementation class of {@link KTable}.
 *
 * @param <K> the key type
 * @param <S> the source's (parent's) value type
 * @param <V> the value type
 */
public class KTableImpl<K, S, V> extends AbstractStream<K, V> implements KTable<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(KTableImpl.class);

    static final String SOURCE_NAME = "KTABLE-SOURCE-";

    static final String STATE_STORE_NAME = "STATE-STORE-";

    private static final String FILTER_NAME = "KTABLE-FILTER-";

    private static final String JOINTHIS_NAME = "KTABLE-JOINTHIS-";

    private static final String JOINOTHER_NAME = "KTABLE-JOINOTHER-";

    private static final String MAPVALUES_NAME = "KTABLE-MAPVALUES-";

    private static final String MERGE_NAME = "KTABLE-MERGE-";

    private static final String SELECT_NAME = "KTABLE-SELECT-";

    private static final String SUPPRESS_NAME = "KTABLE-SUPPRESS-";

    private static final String TOSTREAM_NAME = "KTABLE-TOSTREAM-";

    private static final String TRANSFORMVALUES_NAME = "KTABLE-TRANSFORMVALUES-";

    public static final String BY_PREFIX = "KTABLE-JOIN-BYPREFIX-";

    private static final String REPARTITION_NAME = "KTABLE-REPARTITION-";

    private static final String SINK_NAME = "KTABLE-SINK-";

    private final ProcessorSupplier<?, ?> processorSupplier;

    private final String queryableStoreName;

    private boolean sendOldValues = false;

    public KTableImpl(final String name,
                      final Serde<K> keySerde,
                      final Serde<V> valSerde,
                      final Set<String> sourceNodes,
                      final String queryableStoreName,
                      final ProcessorSupplier<?, ?> processorSupplier,
                      final StreamsGraphNode streamsGraphNode,
                      final InternalStreamsBuilder builder) {
        super(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder);
        this.processorSupplier = processorSupplier;
        this.queryableStoreName = queryableStoreName;
    }

    @Override
    public String queryableStoreName() {
        return queryableStoreName;
    }

    private KTable<K, V> doFilter(final Predicate<? super K, ? super V> predicate,
                                  final Named named,
                                  final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal,
                                  final boolean filterNot) {
        final Serde<K> keySerde;
        final Serde<V> valueSerde;
        final String queryableStoreName;
        final StoreBuilder<TimestampedKeyValueStore<K, V>> storeBuilder;

        if (materializedInternal != null) {
            // we actually do not need to generate store names at all since if it is not specified, we will not
            // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
            if (materializedInternal.storeName() == null) {
                builder.newStoreName(FILTER_NAME);
            }
            // we can inherit parent key and value serde if user do not provide specific overrides, more specifically:
            // we preserve the key following the order of 1) materialized, 2) parent
            keySerde = materializedInternal.keySerde() != null ? materializedInternal.keySerde() : this.keySerde;
            // we preserve the value following the order of 1) materialized, 2) parent
            valueSerde = materializedInternal.valueSerde() != null ? materializedInternal.valueSerde() : this.valSerde;
            queryableStoreName = materializedInternal.queryableStoreName();
            // only materialize if materialized is specified and it has queryable name
            storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<>(materializedInternal)).materialize() : null;
        } else {
            keySerde = this.keySerde;
            valueSerde = this.valSerde;
            queryableStoreName = null;
            storeBuilder = null;
        }
        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FILTER_NAME);

        final KTableProcessorSupplier<K, V, V> processorSupplier =
            new KTableFilter<>(this, predicate, filterNot, queryableStoreName);

        final ProcessorParameters<K, V> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            new ProcessorParameters<>(processorSupplier, name)
        );

        final StreamsGraphNode tableNode = new TableProcessorNode<>(
            name,
            processorParameters,
            storeBuilder
        );

        builder.addGraphNode(this.streamsGraphNode, tableNode);

        return new KTableImpl<>(name,
                                keySerde,
                                valueSerde,
                                sourceNodes,
                                queryableStoreName,
                                processorSupplier,
                                tableNode,
                                builder);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, NamedInternal.empty(), null, false);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate, final Named named) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, named, null, false);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                               final Named named,
                               final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doFilter(predicate, named, materializedInternal, false);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                               final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return filter(predicate, NamedInternal.empty(), materialized);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, NamedInternal.empty(), null, true);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final Named named) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, named, null, true);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return filterNot(predicate, NamedInternal.empty(), materialized);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final Named named,
                                  final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        final NamedInternal renamed = new NamedInternal(named);
        return doFilter(predicate, renamed, materializedInternal, true);
    }

    private <VR> KTable<K, VR> doMapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
                                           final Named named,
                                           final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal) {
        final Serde<K> keySerde;
        final Serde<VR> valueSerde;
        final String queryableStoreName;
        final StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;

        if (materializedInternal != null) {
            // we actually do not need to generate store names at all since if it is not specified, we will not
            // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
            if (materializedInternal.storeName() == null) {
                builder.newStoreName(MAPVALUES_NAME);
            }
            keySerde = materializedInternal.keySerde() != null ? materializedInternal.keySerde() : this.keySerde;
            valueSerde = materializedInternal.valueSerde();
            queryableStoreName = materializedInternal.queryableStoreName();
            // only materialize if materialized is specified and it has queryable name
            storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<>(materializedInternal)).materialize() : null;
        } else {
            keySerde = this.keySerde;
            valueSerde = null;
            queryableStoreName = null;
            storeBuilder = null;
        }

        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, MAPVALUES_NAME);

        final KTableProcessorSupplier<K, V, VR> processorSupplier = new KTableMapValues<>(this, mapper, queryableStoreName);

        // leaving in calls to ITB until building topology with graph

        final ProcessorParameters<K, VR> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            new ProcessorParameters<>(processorSupplier, name)
        );
        final StreamsGraphNode tableNode = new TableProcessorNode<>(
            name,
            processorParameters,
            storeBuilder
        );

        builder.addGraphNode(this.streamsGraphNode, tableNode);

        // don't inherit parent value serde, since this operation may change the value type, more specifically:
        // we preserve the key following the order of 1) materialized, 2) parent, 3) null
        // we preserve the value following the order of 1) materialized, 2) null
        return new KTableImpl<>(
            name,
            keySerde,
            valueSerde,
            sourceNodes,
            queryableStoreName,
            processorSupplier,
            tableNode,
            builder
        );
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(withKey(mapper), NamedInternal.empty(), null);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper,
                                        final Named named) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(withKey(mapper), named, null);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(mapper, NamedInternal.empty(), null);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
                                        final Named named) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(mapper, named, null);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper,
                                        final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return mapValues(mapper, NamedInternal.empty(), materialized);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper,
                                        final Named named,
                                        final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doMapValues(withKey(mapper), named, materializedInternal);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
                                        final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return mapValues(mapper, NamedInternal.empty(), materialized);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
                                        final Named named,
                                        final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doMapValues(mapper, named, materializedInternal);
    }

    @Override
    public <VR> KTable<K, VR> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
                                              final String... stateStoreNames) {
        return doTransformValues(transformerSupplier, null, NamedInternal.empty(), stateStoreNames);
    }

    @Override
    public <VR> KTable<K, VR> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
                                              final Named named,
                                              final String... stateStoreNames) {
        Objects.requireNonNull(named, "processorName can't be null");
        return doTransformValues(transformerSupplier, null, new NamedInternal(named), stateStoreNames);
    }

    @Override
    public <VR> KTable<K, VR> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
                                              final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
                                              final String... stateStoreNames) {
        return transformValues(transformerSupplier, materialized, NamedInternal.empty(), stateStoreNames);
    }

    @Override
    public <VR> KTable<K, VR> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
                                              final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
                                              final Named named,
                                              final String... stateStoreNames) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        Objects.requireNonNull(named, "named can't be null");
        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doTransformValues(transformerSupplier, materializedInternal, new NamedInternal(named), stateStoreNames);
    }

    private <VR> KTable<K, VR> doTransformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
                                                 final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal,
                                                 final NamedInternal namedInternal,
                                                 final String... stateStoreNames) {
        Objects.requireNonNull(stateStoreNames, "stateStoreNames");
        final Serde<K> keySerde;
        final Serde<VR> valueSerde;
        final String queryableStoreName;
        final StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;

        if (materializedInternal != null) {
            // don't inherit parent value serde, since this operation may change the value type, more specifically:
            // we preserve the key following the order of 1) materialized, 2) parent, 3) null
            keySerde = materializedInternal.keySerde() != null ? materializedInternal.keySerde() : this.keySerde;
            // we preserve the value following the order of 1) materialized, 2) null
            valueSerde = materializedInternal.valueSerde();
            queryableStoreName = materializedInternal.queryableStoreName();
            // only materialize if materialized is specified and it has queryable name
            storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<>(materializedInternal)).materialize() : null;
        } else {
            keySerde = this.keySerde;
            valueSerde = null;
            queryableStoreName = null;
            storeBuilder = null;
        }

        final String name = namedInternal.orElseGenerateWithPrefix(builder, TRANSFORMVALUES_NAME);

        final KTableProcessorSupplier<K, V, VR> processorSupplier = new KTableTransformValues<>(
            this,
            transformerSupplier,
            queryableStoreName);

        final ProcessorParameters<K, VR> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            new ProcessorParameters<>(processorSupplier, name)
        );

        final StreamsGraphNode tableNode = new TableProcessorNode<>(
            name,
            processorParameters,
            storeBuilder,
            stateStoreNames
        );

        builder.addGraphNode(this.streamsGraphNode, tableNode);

        return new KTableImpl<>(
            name,
            keySerde,
            valueSerde,
            sourceNodes,
            queryableStoreName,
            processorSupplier,
            tableNode,
            builder);
    }

    @Override
    public KStream<K, V> toStream() {
        return toStream(NamedInternal.empty());
    }

    @Override
    public KStream<K, V> toStream(final Named named) {
        Objects.requireNonNull(named, "named can't be null");

        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, TOSTREAM_NAME);
        final ProcessorSupplier<K, Change<V>> kStreamMapValues = new KStreamMapValues<>((key, change) -> change.newValue);
        final ProcessorParameters<K, V> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            new ProcessorParameters<>(kStreamMapValues, name)
        );

        final ProcessorGraphNode<K, V> toStreamNode = new ProcessorGraphNode<>(
            name,
            processorParameters
        );

        builder.addGraphNode(this.streamsGraphNode, toStreamNode);

        // we can inherit parent key and value serde
        return new KStreamImpl<>(name, keySerde, valSerde, sourceNodes, false, toStreamNode, builder);
    }

    @Override
    public <K1> KStream<K1, V> toStream(final KeyValueMapper<? super K, ? super V, ? extends K1> mapper) {
        return toStream().selectKey(mapper);
    }

    @Override
    public <K1> KStream<K1, V> toStream(final KeyValueMapper<? super K, ? super V, ? extends K1> mapper,
                                        final Named named) {
        return toStream(named).selectKey(mapper);
    }

    @Override
    public KTable<K, V> suppress(final Suppressed<? super K> suppressed) {
        final String name;
        if (suppressed instanceof NamedSuppressed) {
            final String givenName = ((NamedSuppressed<?>) suppressed).name();
            name = givenName != null ? givenName : builder.newProcessorName(SUPPRESS_NAME);
        } else {
            throw new IllegalArgumentException("Custom subclasses of Suppressed are not supported.");
        }

        final SuppressedInternal<K> suppressedInternal = buildSuppress(suppressed, name);

        final String storeName =
            suppressedInternal.name() != null ? suppressedInternal.name() + "-store" : builder.newStoreName(SUPPRESS_NAME);

        final ProcessorSupplier<K, Change<V>> suppressionSupplier = new KTableSuppressProcessorSupplier<>(
            suppressedInternal,
            storeName,
            this
        );
        
        final ProcessorGraphNode<K, Change<V>> node = new StatefulProcessorNode<>(
            name,
            new ProcessorParameters<>(suppressionSupplier, name),
            new InMemoryTimeOrderedKeyValueBuffer.Builder<>(storeName, keySerde, valSerde)
        );

        builder.addGraphNode(streamsGraphNode, node);

        return new KTableImpl<K, S, V>(
            name,
            keySerde,
            valSerde,
            Collections.singleton(this.name),
            null,
            suppressionSupplier,
            node,
            builder
        );
    }

    @SuppressWarnings("unchecked")
    private SuppressedInternal<K> buildSuppress(final Suppressed<? super K> suppress, final String name) {
        if (suppress instanceof FinalResultsSuppressionBuilder) {
            final long grace = findAndVerifyWindowGrace(streamsGraphNode);
            LOG.info("Using grace period of [{}] as the suppress duration for node [{}].",
                     Duration.ofMillis(grace), name);

            final FinalResultsSuppressionBuilder<?> builder = (FinalResultsSuppressionBuilder<?>) suppress;

            final SuppressedInternal<?> finalResultsSuppression =
                builder.buildFinalResultsSuppression(Duration.ofMillis(grace));

            return (SuppressedInternal<K>) finalResultsSuppression;
        } else if (suppress instanceof SuppressedInternal) {
            return (SuppressedInternal<K>) suppress;
        } else {
            throw new IllegalArgumentException("Custom subclasses of Suppressed are not allowed.");
        }
    }

    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, NamedInternal.empty(), null, false, false);
    }

    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                     final Named named) {
        return doJoin(other, joiner, named, null, false, false);
    }

    @Override
    public <VO, VR> KTable<K, VR> join(final KTable<K, VO> other,
                                       final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                       final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return join(other, joiner, NamedInternal.empty(), materialized);
    }

    @Override
    public <VO, VR> KTable<K, VR> join(final KTable<K, VO> other,
                                       final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                       final Named named,
                                       final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, MERGE_NAME);

        return doJoin(other, joiner, named, materializedInternal, false, false);
    }

    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return outerJoin(other, joiner, NamedInternal.empty());
    }

    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                          final Named named) {
        return doJoin(other, joiner, named, null, true, true);
    }

    @Override
    public <VO, VR> KTable<K, VR> outerJoin(final KTable<K, VO> other,
                                            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return outerJoin(other, joiner, NamedInternal.empty(), materialized);
    }

    @Override
    public <VO, VR> KTable<K, VR> outerJoin(final KTable<K, VO> other,
                                            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                            final Named named,
                                            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, MERGE_NAME);

        return doJoin(other, joiner, named, materializedInternal, true, true);
    }

    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return leftJoin(other, joiner, NamedInternal.empty());
    }

    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                         final Named named) {
        return doJoin(other, joiner, named, null, true, false);
    }

    @Override
    public <VO, VR> KTable<K, VR> leftJoin(final KTable<K, VO> other,
                                           final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                           final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return leftJoin(other, joiner, NamedInternal.empty(), materialized);
    }

    @Override
    public <VO, VR> KTable<K, VR> leftJoin(final KTable<K, VO> other,
                                           final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                           final Named named,
                                           final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, MERGE_NAME);

        return doJoin(other, joiner, named, materializedInternal, true, false);
    }

    @SuppressWarnings("unchecked")
    private <VO, VR> KTable<K, VR> doJoin(final KTable<K, VO> other,
                                          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                          final Named joinName,
                                          final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal,
                                          final boolean leftOuter,
                                          final boolean rightOuter) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(joinName, "joinName can't be null");

        final NamedInternal renamed = new NamedInternal(joinName);
        final String joinMergeName = renamed.orElseGenerateWithPrefix(builder, MERGE_NAME);
        final Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K, VO>) other);

        if (leftOuter) {
            enableSendingOldValues();
        }
        if (rightOuter) {
            ((KTableImpl) other).enableSendingOldValues();
        }

        final KTableKTableAbstractJoin<K, VR, V, VO> joinThis;
        final KTableKTableAbstractJoin<K, VR, VO, V> joinOther;

        if (!leftOuter) { // inner
            joinThis = new KTableKTableInnerJoin<>(this, (KTableImpl<K, ?, VO>) other, joiner);
            joinOther = new KTableKTableInnerJoin<>((KTableImpl<K, ?, VO>) other, this, reverseJoiner(joiner));
        } else if (!rightOuter) { // left
            joinThis = new KTableKTableLeftJoin<>(this, (KTableImpl<K, ?, VO>) other, joiner);
            joinOther = new KTableKTableRightJoin<>((KTableImpl<K, ?, VO>) other, this, reverseJoiner(joiner));
        } else { // outer
            joinThis = new KTableKTableOuterJoin<>(this, (KTableImpl<K, ?, VO>) other, joiner);
            joinOther = new KTableKTableOuterJoin<>((KTableImpl<K, ?, VO>) other, this, reverseJoiner(joiner));
        }

        final String joinThisName = renamed.suffixWithOrElseGet("-join-this", builder, JOINTHIS_NAME);
        final String joinOtherName = renamed.suffixWithOrElseGet("-join-other", builder, JOINOTHER_NAME);

        final ProcessorParameters<K, Change<V>> joinThisProcessorParameters = new ProcessorParameters<>(joinThis, joinThisName);
        final ProcessorParameters<K, Change<VO>> joinOtherProcessorParameters = new ProcessorParameters<>(joinOther, joinOtherName);

        final Serde<K> keySerde;
        final Serde<VR> valueSerde;
        final String queryableStoreName;
        final StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;

        if (materializedInternal != null) {
            keySerde = materializedInternal.keySerde() != null ? materializedInternal.keySerde() : this.keySerde;
            valueSerde = materializedInternal.valueSerde();
            queryableStoreName = materializedInternal.storeName();
            storeBuilder = new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize();
        } else {
            keySerde = this.keySerde;
            valueSerde = null;
            queryableStoreName = null;
            storeBuilder = null;
        }

        final KTableKTableJoinNode<K, V, VO, VR> kTableKTableJoinNode =
            KTableKTableJoinNode.<K, V, VO, VR>kTableKTableJoinNodeBuilder()
                .withNodeName(joinMergeName)
                .withJoinThisProcessorParameters(joinThisProcessorParameters)
                .withJoinOtherProcessorParameters(joinOtherProcessorParameters)
                .withThisJoinSideNodeName(name)
                .withOtherJoinSideNodeName(((KTableImpl) other).name)
                .withJoinThisStoreNames(valueGetterSupplier().storeNames())
                .withJoinOtherStoreNames(((KTableImpl) other).valueGetterSupplier().storeNames())
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde)
                .withQueryableStoreName(queryableStoreName)
                .withStoreBuilder(storeBuilder)
                .build();
        builder.addGraphNode(this.streamsGraphNode, kTableKTableJoinNode);

        // we can inherit parent key serde if user do not provide specific overrides
        return new KTableImpl<K, Change<VR>, VR>(
            kTableKTableJoinNode.nodeName(),
            kTableKTableJoinNode.keySerde(),
            kTableKTableJoinNode.valueSerde(),
            allSourceNodes,
            kTableKTableJoinNode.queryableStoreName(),
            kTableKTableJoinNode.joinMerger(),
            kTableKTableJoinNode,
            builder
        );
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector) {
        return groupBy(selector, Grouped.with(null, null));
    }

    @Override
    @Deprecated
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector,
                                                  final org.apache.kafka.streams.kstream.Serialized<K1, V1> serialized) {
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(serialized, "serialized can't be null");
        final SerializedInternal<K1, V1> serializedInternal = new SerializedInternal<>(serialized);
        return groupBy(selector, Grouped.with(serializedInternal.keySerde(), serializedInternal.valueSerde()));
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector,
                                                  final Grouped<K1, V1> grouped) {
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(grouped, "grouped can't be null");
        final GroupedInternal<K1, V1> groupedInternal = new GroupedInternal<>(grouped);
        final String selectName = new NamedInternal(groupedInternal.name()).orElseGenerateWithPrefix(builder, SELECT_NAME);

        final KTableProcessorSupplier<K, V, KeyValue<K1, V1>> selectSupplier = new KTableRepartitionMap<>(this, selector);
        final ProcessorParameters<K, Change<V>> processorParameters = new ProcessorParameters<>(selectSupplier, selectName);

        // select the aggregate key and values (old and new), it would require parent to send old values
        final ProcessorGraphNode<K, Change<V>> groupByMapNode = new ProcessorGraphNode<>(selectName, processorParameters);

        builder.addGraphNode(this.streamsGraphNode, groupByMapNode);

        this.enableSendingOldValues();
        return new KGroupedTableImpl<>(
            builder,
            selectName,
            sourceNodes,
            groupedInternal,
            groupByMapNode
        );
    }

    @SuppressWarnings("unchecked")
    public KTableValueGetterSupplier<K, V> valueGetterSupplier() {
        if (processorSupplier instanceof KTableSource) {
            final KTableSource<K, V> source = (KTableSource<K, V>) processorSupplier;
            // whenever a source ktable is required for getter, it should be materialized
            source.materialize();
            return new KTableSourceValueGetterSupplier<>(source.queryableName());
        } else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
            return ((KStreamAggProcessorSupplier<?, K, S, V>) processorSupplier).view();
        } else {
            return ((KTableProcessorSupplier<K, S, V>) processorSupplier).view();
        }
    }

    @SuppressWarnings("unchecked")
    public void enableSendingOldValues() {
        if (!sendOldValues) {
            if (processorSupplier instanceof KTableSource) {
                final KTableSource<K, ?> source = (KTableSource<K, V>) processorSupplier;
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

    /**
     * We conflate V with Change<V> in many places. It might be nice to fix that eventually.
     * For now, I'm just explicitly lying about the parameterized type.
     */
    @SuppressWarnings("unchecked")
    private <VR> ProcessorParameters<K, VR> unsafeCastProcessorParametersToCompletelyDifferentType(final ProcessorParameters<K, Change<V>> kObjectProcessorParameters) {
        return (ProcessorParameters<K, VR>) kObjectProcessorParameters;
    }

    @Override
    public <VR, KO, VO> KTable<K, VR> join(final KTable<KO, VO> other,
                                           final ValueMapper<V, KO> foreignKeyExtractor,
                                           final ValueJoiner<V, VO, VR> joiner,
                                           final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(foreignKeyExtractor, "foreignKeyExtractor can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        return doJoinOnForeignKey(other, foreignKeyExtractor, joiner, new MaterializedInternal<>(materialized));
    }


    private <VR, KO, VO> KTable<K, VR> doJoinOnForeignKey(final KTable<KO, VO> other,
                                                          final ValueMapper<V, KO> foreignKeyExtractor,
                                                          final ValueJoiner<V, VO, VR> joiner,
                                                          final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal) {
        //Old values are a useful optimization. The old values from the other table are compared to the new values,
        //such that identical values do not cause a prefixScan. PrefixScan and propagation can be expensive and should
        //not be done needlessly.
        ((KTableImpl<?, ?, ?>) other).enableSendingOldValues();

        //Old values must be sent such that the KTableRepartitionerProcessorSupplier can propagate deletions to the correct node.
        //This occurs whenever the extracted foreignKey changes values.
        enableSendingOldValues();

        final String repartitionProcessorName = builder.newProcessorName(REPARTITION_NAME);
        final String repartitionTopicName = JOINOTHER_NAME + repartitionProcessorName;
        final String repartitionSourceName = builder.newProcessorName(SOURCE_NAME + repartitionProcessorName);
        final String repartitionSinkName = builder.newProcessorName(SINK_NAME + repartitionProcessorName);

        final String oneToOneName = builder.newProcessorName(JOINTHIS_NAME);

        // repartition original => intermediate topic
        final KTableRepartitionerProcessorSupplier<K, KO, V> repartitionProcessor =
                new KTableRepartitionerProcessorSupplier<>(foreignKeyExtractor, valSerde.serializer());

        final CombinedKeySerde<KO, K> combinedKeySerde = new CombinedKeySerde<>(((KTableImpl<KO, VO, ?>) other).keySerde(), keySerde);

        //Create the partitioner that will partition CombinedKey on just the foreign portion (right) of the combinedKey.
        final CombinedKeyForeignKeyPartitioner<KO, K, V> partitioner = new CombinedKeyForeignKeyPartitioner<>(combinedKeySerde);

        //The processor for the CombinedKey intermediary state store. It does two main things:
        // 1) Loads the data into the new state store. This data is accessed by the KTableKTablePrefixJoin processor.
        // 2) Gets the foreignKey value data from the Other KTable and creates a SubscriptionWrapperResponse.
        //    Returns the data keyed on K. Discards the CombinedKey as it is no longer needed after this stage.
        final String thisStateStoreName = builder.newStoreName(STATE_STORE_NAME + "-thisSSName-");
        final ForeignKeySingleLookupProcessorSupplier<K, KO, VO> oneToOne =
                new ForeignKeySingleLookupProcessorSupplier<>(thisStateStoreName, ((KTableImpl<KO, VO, VO>) other).valueGetterSupplier());

        final RocksDbKeyValueBytesStoreSupplier thisRocksDBRef = new RocksDbKeyValueBytesStoreSupplier(thisStateStoreName, false); //Dont need timestamped store.

        final StoreBuilder<KeyValueStore<Bytes, byte[]>> prefixScanStoreBuilder = Stores.keyValueStoreBuilder(thisRocksDBRef,
                combinedKeySerde,
                new SubscriptionWrapperSerde());

        //Performs other-table/foreign-key-driven updates
        final ProcessorSupplier<KO, Change<VO>> prefixScanProcessorSupplier =
                new KTableKTablePrefixScanProcessorSupplier<>(oneToOne.valueGetterSupplier());

        //Need to write all updates to a given K back to the same partition, as at this point in the topology
        //everything is partitioned on KO from the previous repartition step.
        final String finalRepartitionerName = builder.newProcessorName(REPARTITION_NAME);
        final String finalRepartitionTopicName = JOINOTHER_NAME + finalRepartitionerName;
        final String finalRepartitionSourceName = builder.newProcessorName(SOURCE_NAME + finalRepartitionerName);
        final String finalRepartitionSinkName = builder.newProcessorName(SINK_NAME + finalRepartitionerName);

        //Create the processor to resolve the subscription updates.
        final KTableValueGetterSupplier<K, V> thisKTableValueGetterSupplier = this.valueGetterSupplier();

        final SubscriptionResolverJoinProcessorSupplier<K, V, VO, VR> resolverProcessor = new SubscriptionResolverJoinProcessorSupplier<>(thisKTableValueGetterSupplier, valueSerde().serializer(), joiner);
        final String resolverProcessorName = builder.newProcessorName(KTableImpl.SOURCE_NAME + "resolver");

        final KTableSource<K, VR> outputProcessor = new KTableSource<>(materializedInternal.storeName(), materializedInternal.queryableStoreName());
        final String outputProcessorName = builder.newProcessorName(SOURCE_NAME + "output");

        final HashSet<String> copartitions = new HashSet<>();
        copartitions.add(repartitionSourceName);
        copartitions.addAll(((KTableImpl<?, ?, ?>) other).sourceNodes);
        //TODO - Figure out a correct copartitioning strategy for topics with differing partition counts.
//        copartitions.add(finalRepartitionSourceName);
//        copartitions.addAll(sourceNodes);
        builder.internalTopologyBuilder.copartitionSources(copartitions);

        final HashSet<String> copartitions2 = new HashSet<>();
        copartitions2.add(finalRepartitionSourceName);
        copartitions2.addAll(sourceNodes);
        builder.internalTopologyBuilder.copartitionSources(copartitions2);

        final ProcessorParameters repartitionProcessorParameters = new ProcessorParameters<>(
                repartitionProcessor,
                repartitionProcessorName
        );

        final ProcessorParameters<CombinedKey<KO, K>, SubscriptionWrapper> oneToOneProcessorParameters = new ProcessorParameters<>(
                oneToOne,
                oneToOneName
        );

        final ProcessorParameters<KO, Change<VO>> prefixScanProcessorParameters = new ProcessorParameters<>(
                prefixScanProcessorSupplier,
                builder.newProcessorName(BY_PREFIX)
        );

        final ProcessorParameters<K, SubscriptionResponseWrapper<VO>> resolverProcessorParameters = new ProcessorParameters<>(
                resolverProcessor,
                resolverProcessorName
        );

        final ProcessorParameters<K, VR> outputProcessorParameters = new ProcessorParameters<>(
                outputProcessor,
                outputProcessorName
        );

        final KTableKTableForeignKeyJoinResolutionNode fkSinkAndResolveNode = new KTableKTableForeignKeyJoinResolutionNode<VR, K, V, KO, VO>(
                resolverProcessorParameters.processorName(),
                oneToOneProcessorParameters,
                prefixScanProcessorParameters,
                resolverProcessorParameters,
                finalRepartitionTopicName,
                finalRepartitionSinkName,
                finalRepartitionSourceName,
                keySerde,
                new SubscriptionResponseWrapperSerde<>(
                        ((KTableImpl<KO, VO, VO>) other).valSerde.serializer(),
                        ((KTableImpl<KO, VO, VO>) other).valSerde.deserializer()
                ),
                valueGetterSupplier()
                );

        final OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder<CombinedKey<KO, K>, V> repartitionNodeBuilder =
                OptimizableRepartitionNode.optimizableRepartitionNodeBuilder();

        final OptimizableRepartitionNode<CombinedKey<KO, K>, V> repartitionNode = repartitionNodeBuilder
                .withNodeName(repartitionSourceName)
                .withProcessorParameters(repartitionProcessorParameters)
                .withSinkName(repartitionSinkName)
                .withSourceName(repartitionSourceName)
                .withRepartitionTopic(repartitionTopicName)
                .withValueSerde(new SubscriptionWrapperSerde())
                .withPartitioner(partitioner)
                .withKeySerde(combinedKeySerde)
                .build();

        final StatefulProcessorNode<CombinedKey<KO, K>, V> oneToOneNode = new StatefulProcessorNode(
                oneToOneProcessorParameters.processorName(),
                oneToOneProcessorParameters,
                ((KTableImpl<KO, VO, ?>) other).valueGetterSupplier().storeNames(),
                prefixScanStoreBuilder
        );

        final StatefulProcessorNode<KO, Change<VO>> oneToManyNode = new StatefulProcessorNode<>(
                prefixScanProcessorParameters.processorName(),
                prefixScanProcessorParameters,
                new String[]{prefixScanStoreBuilder.name()}
        );

        final StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;
        if (materializedInternal != null) {
            storeBuilder = new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize();
        } else {
            storeBuilder = null;
        }

        final TableProcessorNode outputTableNode = new TableProcessorNode<>(
                outputProcessorParameters.processorName(),
                outputProcessorParameters,
                storeBuilder,
                thisKTableValueGetterSupplier.storeNames()
        );

        builder.addGraphNode(streamsGraphNode, repartitionNode);
        builder.addGraphNode(repartitionNode, oneToOneNode);
        builder.addGraphNode(((KTableImpl<KO, VO, ?>) other).streamsGraphNode, oneToManyNode);
        final Set<StreamsGraphNode> joinerParentNodes = new HashSet<>(2);
        joinerParentNodes.add(oneToManyNode);
        joinerParentNodes.add(oneToOneNode);
        builder.addGraphNode(joinerParentNodes, fkSinkAndResolveNode);
        builder.addGraphNode(fkSinkAndResolveNode, outputTableNode);

        return new KTableImpl<K,V,VR>(
                outputProcessorName,
                keySerde,
                materializedInternal.valueSerde(),
                Collections.singleton(finalRepartitionSourceName),
                materializedInternal.storeName(),
                outputProcessor,
                outputTableNode,
                builder);
    }
}
