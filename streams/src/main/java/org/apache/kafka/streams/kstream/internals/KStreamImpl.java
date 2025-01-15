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
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.internals.graph.BaseRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.BaseRepartitionNode.BaseRepartitionNodeBuilder;
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorToStateConnectorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamSinkNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamTableJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamToTableNode;
import org.apache.kafka.streams.kstream.internals.graph.UnoptimizableRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.UnoptimizableRepartitionNode.UnoptimizableRepartitionNodeBuilder;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopicProperties;
import org.apache.kafka.streams.processor.internals.StaticTopicNameExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDBTimeOrderedKeyValueBuffer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode.optimizableRepartitionNodeBuilder;

public class KStreamImpl<K, V> extends AbstractStream<K, V> implements KStream<K, V> {

    static final String JOINTHIS_NAME = "KSTREAM-JOINTHIS-";

    static final String JOINOTHER_NAME = "KSTREAM-JOINOTHER-";

    static final String JOIN_NAME = "KSTREAM-JOIN-";

    static final String LEFTJOIN_NAME = "KSTREAM-LEFTJOIN-";

    static final String MERGE_NAME = "KSTREAM-MERGE-";

    static final String OUTERTHIS_NAME = "KSTREAM-OUTERTHIS-";

    static final String OUTEROTHER_NAME = "KSTREAM-OUTEROTHER-";

    static final String WINDOWED_NAME = "KSTREAM-WINDOWED-";

    static final String OUTERSHARED_NAME = "KSTREAM-OUTERSHARED-";

    static final String SOURCE_NAME = "KSTREAM-SOURCE-";

    static final String SINK_NAME = "KSTREAM-SINK-";

    static final String REPARTITION_TOPIC_SUFFIX = "-repartition";

    private static final String FILTER_NAME = "KSTREAM-FILTER-";

    private static final String PEEK_NAME = "KSTREAM-PEEK-";

    private static final String FLATMAP_NAME = "KSTREAM-FLATMAP-";

    private static final String FLATMAPVALUES_NAME = "KSTREAM-FLATMAPVALUES-";

    private static final String MAP_NAME = "KSTREAM-MAP-";

    private static final String MAPVALUES_NAME = "KSTREAM-MAPVALUES-";

    private static final String PROCESSOR_NAME = "KSTREAM-PROCESSOR-";

    private static final String PROCESSVALUES_NAME = "KSTREAM-PROCESSVALUES-";

    private static final String PRINTING_NAME = "KSTREAM-PRINTER-";

    private static final String KEY_SELECT_NAME = "KSTREAM-KEY-SELECT-";

    private static final String FOREACH_NAME = "KSTREAM-FOREACH-";

    private static final String TO_KTABLE_NAME = "KSTREAM-TOTABLE-";

    private static final String REPARTITION_NAME = "KSTREAM-REPARTITION-";

    private final boolean repartitionRequired;

    private OptimizableRepartitionNode<K, V> repartitionNode;

    KStreamImpl(final String name,
                final Serde<K> keySerde,
                final Serde<V> valueSerde,
                final Set<String> subTopologySourceNodes,
                final boolean repartitionRequired,
                final GraphNode graphNode,
                final InternalStreamsBuilder builder) {
        super(name, keySerde, valueSerde, subTopologySourceNodes, graphNode, builder);
        this.repartitionRequired = repartitionRequired;
    }

    @Override
    public KStream<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        return filter(predicate, NamedInternal.empty());
    }

    @Override
    public KStream<K, V> filter(final Predicate<? super K, ? super V> predicate,
                                final Named named) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(named, "named can't be null");

        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FILTER_NAME);
        final ProcessorParameters<? super K, ? super V, ?, ?> processorParameters =
            new ProcessorParameters<>(new KStreamFilter<>(predicate, false), name);
        final ProcessorGraphNode<? super K, ? super V> filterProcessorNode =
            new ProcessorGraphNode<>(name, processorParameters);

        builder.addGraphNode(graphNode, filterProcessorNode);

        return new KStreamImpl<>(
            name,
            keySerde,
            valueSerde,
            subTopologySourceNodes,
            repartitionRequired,
            filterProcessorNode,
            builder);
    }

    @Override
    public KStream<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        return filterNot(predicate, NamedInternal.empty());
    }

    @Override
    public KStream<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                   final Named named) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(named, "named can't be null");

        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FILTER_NAME);
        final ProcessorParameters<? super K, ? super V, ?, ?> processorParameters =
            new ProcessorParameters<>(new KStreamFilter<>(predicate, true), name);
        final ProcessorGraphNode<? super K, ? super V> filterNotProcessorNode =
            new ProcessorGraphNode<>(name, processorParameters);

        builder.addGraphNode(graphNode, filterNotProcessorNode);

        return new KStreamImpl<>(
            name,
            keySerde,
            valueSerde,
            subTopologySourceNodes,
            repartitionRequired,
            filterNotProcessorNode,
            builder);
    }

    @Override
    public <KR> KStream<KR, V> selectKey(final KeyValueMapper<? super K, ? super V, ? extends KR> mapper) {
        return selectKey(mapper, NamedInternal.empty());
    }

    @Override
    public <KR> KStream<KR, V> selectKey(final KeyValueMapper<? super K, ? super V, ? extends KR> mapper,
                                         final Named named) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(named, "named can't be null");

        final ProcessorGraphNode<K, V> selectKeyProcessorNode = internalSelectKey(mapper, new NamedInternal(named));
        selectKeyProcessorNode.keyChangingOperation(true);

        builder.addGraphNode(graphNode, selectKeyProcessorNode);

        // key serde cannot be preserved
        return new KStreamImpl<>(
            selectKeyProcessorNode.nodeName(),
            null,
            valueSerde,
            subTopologySourceNodes,
            true,
            selectKeyProcessorNode,
            builder);
    }

    private <KR> ProcessorGraphNode<K, V> internalSelectKey(final KeyValueMapper<? super K, ? super V, ? extends KR> mapper,
                                                            final NamedInternal named) {
        final String name = named.orElseGenerateWithPrefix(builder, KEY_SELECT_NAME);
        final KStreamMap<K, V, KR, V> kStreamMap =
            new KStreamMap<>((key, value) -> new KeyValue<>(mapper.apply(key, value), value));
        final ProcessorParameters<K, V, ?, ?> processorParameters = new ProcessorParameters<>(kStreamMap, name);

        return new ProcessorGraphNode<>(name, processorParameters);
    }

    @Override
    public <KR, VR> KStream<KR, VR> map(final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper) {
        return map(mapper, NamedInternal.empty());
    }

    @Override
    public <KR, VR> KStream<KR, VR> map(final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
                                        final Named named) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(named, "named can't be null");

        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, MAP_NAME);
        final ProcessorParameters<? super K, ? super V, ?, ?> processorParameters =
            new ProcessorParameters<>(new KStreamMap<>(mapper), name);
        final ProcessorGraphNode<? super K, ? super V> mapProcessorNode =
            new ProcessorGraphNode<>(name, processorParameters);
        mapProcessorNode.keyChangingOperation(true);

        builder.addGraphNode(graphNode, mapProcessorNode);

        // key and value serde cannot be preserved
        return new KStreamImpl<>(
            name,
            null,
            null,
            subTopologySourceNodes,
            true,
            mapProcessorNode,
            builder);
    }

    @Override
    public <VR> KStream<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> valueMapper) {
        return mapValues(withKey(valueMapper));
    }

    @Override
    public <VR> KStream<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper,
                                         final Named named) {
        return mapValues(withKey(mapper), named);
    }

    @Override
    public <VR> KStream<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> valueMapperWithKey) {
        return mapValues(valueMapperWithKey, NamedInternal.empty());
    }

    @Override
    public <VR> KStream<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> valueMapperWithKey,
                                         final Named named) {
        Objects.requireNonNull(valueMapperWithKey, "valueMapperWithKey can't be null");
        Objects.requireNonNull(named, "named can't be null");

        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, MAPVALUES_NAME);
        final ProcessorParameters<? super K, ? super V, ?, ?> processorParameters =
            new ProcessorParameters<>(new KStreamMapValues<>(valueMapperWithKey), name);
        final ProcessorGraphNode<? super K, ? super V> mapValuesProcessorNode =
            new ProcessorGraphNode<>(name, processorParameters);
        mapValuesProcessorNode.setValueChangingOperation(true);

        builder.addGraphNode(graphNode, mapValuesProcessorNode);

        // value serde cannot be preserved
        return new KStreamImpl<>(
            name,
            keySerde,
            null,
            subTopologySourceNodes,
            repartitionRequired,
            mapValuesProcessorNode,
            builder);
    }

    @Override
    public <KR, VR> KStream<KR, VR> flatMap(final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper) {
        return flatMap(mapper, NamedInternal.empty());
    }

    @Override
    public <KR, VR> KStream<KR, VR> flatMap(final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper,
                                            final Named named) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(named, "named can't be null");

        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FLATMAP_NAME);
        final ProcessorParameters<? super K, ? super V, ?, ?> processorParameters =
            new ProcessorParameters<>(new KStreamFlatMap<>(mapper), name);
        final ProcessorGraphNode<? super K, ? super V> flatMapNode =
            new ProcessorGraphNode<>(name, processorParameters);
        flatMapNode.keyChangingOperation(true);

        builder.addGraphNode(graphNode, flatMapNode);

        // key and value serde cannot be preserved
        return new KStreamImpl<>(name, null, null, subTopologySourceNodes, true, flatMapNode, builder);
    }

    @Override
    public <VR> KStream<K, VR> flatMapValues(final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper) {
        return flatMapValues(withKey(mapper));
    }

    @Override
    public <VR> KStream<K, VR> flatMapValues(final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
                                             final Named named) {
        return flatMapValues(withKey(mapper), named);
    }

    @Override
    public <VR> KStream<K, VR> flatMapValues(final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper) {
        return flatMapValues(mapper, NamedInternal.empty());
    }

    @Override
    public <VR> KStream<K, VR> flatMapValues(final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> valueMapper,
                                             final Named named) {
        Objects.requireNonNull(valueMapper, "valueMapper can't be null");
        Objects.requireNonNull(named, "named can't be null");

        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FLATMAPVALUES_NAME);
        final ProcessorParameters<? super K, ? super V, ?, ?> processorParameters =
            new ProcessorParameters<>(new KStreamFlatMapValues<>(valueMapper), name);
        final ProcessorGraphNode<? super K, ? super V> flatMapValuesNode =
            new ProcessorGraphNode<>(name, processorParameters);
        flatMapValuesNode.setValueChangingOperation(true);

        builder.addGraphNode(graphNode, flatMapValuesNode);

        // value serde cannot be preserved
        return new KStreamImpl<>(
            name,
            keySerde,
            null,
            subTopologySourceNodes,
            repartitionRequired,
            flatMapValuesNode,
            builder);
    }

    @Override
    public void print(final Printed<K, V> printed) {
        Objects.requireNonNull(printed, "printed can't be null");

        final PrintedInternal<K, V> printedInternal = new PrintedInternal<>(printed);
        final String name = new NamedInternal(printedInternal.name()).orElseGenerateWithPrefix(builder, PRINTING_NAME);
        final ProcessorParameters<? super K, ? super V, ?, ?> processorParameters =
            new ProcessorParameters<>(printedInternal.build(this.name), name);
        final ProcessorGraphNode<? super K, ? super V> printNode =
            new ProcessorGraphNode<>(name, processorParameters);

        builder.addGraphNode(graphNode, printNode);
    }

    @Override
    public void foreach(final ForeachAction<? super K, ? super V> action) {
        foreach(action, NamedInternal.empty());
    }

    @Override
    public void foreach(final ForeachAction<? super K, ? super V> action,
                        final Named named) {
        Objects.requireNonNull(action, "action can't be null");
        Objects.requireNonNull(named, "named can't be null");

        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FOREACH_NAME);
        final ProcessorParameters<? super K, ? super V, ?, ?> processorParameters =
            new ProcessorParameters<>(() -> new ForeachProcessor<>(action), name);
        final ProcessorGraphNode<? super K, ? super V> foreachNode =
            new ProcessorGraphNode<>(name, processorParameters);

        builder.addGraphNode(graphNode, foreachNode);
    }

    @Override
    public KStream<K, V> peek(final ForeachAction<? super K, ? super V> action) {
        return peek(action, NamedInternal.empty());
    }

    @Override
    public KStream<K, V> peek(final ForeachAction<? super K, ? super V> action,
                              final Named named) {
        Objects.requireNonNull(action, "action can't be null");
        Objects.requireNonNull(named, "named can't be null");

        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, PEEK_NAME);
        final ProcessorParameters<? super K, ? super V, ?, ?> processorParameters =
            new ProcessorParameters<>(new KStreamPeek<>(action), name);
        final ProcessorGraphNode<? super K, ? super V> peekNode =
            new ProcessorGraphNode<>(name, processorParameters);

        builder.addGraphNode(graphNode, peekNode);

        return new KStreamImpl<>(
            name,
            keySerde,
            valueSerde,
            subTopologySourceNodes,
            repartitionRequired,
            peekNode,
            builder);
    }

    @Override
    public BranchedKStream<K, V> split() {
        return new BranchedKStreamImpl<>(this, repartitionRequired, NamedInternal.empty());
    }

    @Override
    public BranchedKStream<K, V> split(final Named named) {
        Objects.requireNonNull(named, "named can't be null");
        return new BranchedKStreamImpl<>(this, repartitionRequired, new NamedInternal(named));
    }

    @Override
    public KStream<K, V> merge(final KStream<K, V> stream) {
        return merge(stream, NamedInternal.empty());
    }

    @Override
    public KStream<K, V> merge(final KStream<K, V> stream,
                               final Named named) {
        Objects.requireNonNull(stream, "stream can't be null");
        Objects.requireNonNull(named, "named can't be null");

        return merge(builder, stream, new NamedInternal(named));
    }

    private KStream<K, V> merge(final InternalStreamsBuilder builder,
                                final KStream<K, V> stream,
                                final NamedInternal named) {
        final KStreamImpl<K, V> streamImpl = (KStreamImpl<K, V>) stream;
        final boolean requireRepartitioning = streamImpl.repartitionRequired || repartitionRequired;
        final String name = named.orElseGenerateWithPrefix(builder, MERGE_NAME);
        final Set<String> allSubTopologySourceNodes = new HashSet<>();
        allSubTopologySourceNodes.addAll(subTopologySourceNodes);
        allSubTopologySourceNodes.addAll(streamImpl.subTopologySourceNodes);

        final ProcessorParameters<? super K, ? super V, ?, ?> processorParameters =
            new ProcessorParameters<>(new PassThrough<>(), name);
        final ProcessorGraphNode<? super K, ? super V> mergeNode =
            new ProcessorGraphNode<>(name, processorParameters);
        mergeNode.setMergeNode(true);

        builder.addGraphNode(Arrays.asList(graphNode, streamImpl.graphNode), mergeNode);

        // drop the serde as we cannot safely use either one to represent both streams
        return new KStreamImpl<>(
            name,
            null,
            null,
            allSubTopologySourceNodes,
            requireRepartitioning,
            mergeNode,
            builder);
    }

    @Override
    public KStream<K, V> repartition() {
        return doRepartition(Repartitioned.as(null));
    }

    @Override
    public KStream<K, V> repartition(final Repartitioned<K, V> repartitioned) {
        return doRepartition(repartitioned);
    }

    private KStream<K, V> doRepartition(final Repartitioned<K, V> repartitioned) {
        Objects.requireNonNull(repartitioned, "repartitioned can't be null");

        final RepartitionedInternal<K, V> repartitionedInternal = new RepartitionedInternal<>(repartitioned);

        final String name = repartitionedInternal.name() != null ? repartitionedInternal.name() : builder
            .newProcessorName(REPARTITION_NAME);

        final Serde<V> valueSerde = repartitionedInternal.valueSerde() == null ? this.valueSerde : repartitionedInternal.valueSerde();
        final Serde<K> keySerde = repartitionedInternal.keySerde() == null ? this.keySerde : repartitionedInternal.keySerde();

        final UnoptimizableRepartitionNodeBuilder<K, V> unoptimizableRepartitionNodeBuilder = UnoptimizableRepartitionNode
            .unoptimizableRepartitionNodeBuilder();

        final InternalTopicProperties internalTopicProperties = repartitionedInternal.toInternalTopicProperties();

        final String repartitionSourceName = createRepartitionedSource(
            builder,
            repartitionedInternal.keySerde(),
            valueSerde,
            name,
            repartitionedInternal.streamPartitioner(),
            unoptimizableRepartitionNodeBuilder.withInternalTopicProperties(internalTopicProperties)
        );

        final UnoptimizableRepartitionNode<K, V> unoptimizableRepartitionNode = unoptimizableRepartitionNodeBuilder.build();

        builder.addGraphNode(graphNode, unoptimizableRepartitionNode);

        final Set<String> sourceNodes = new HashSet<>();
        sourceNodes.add(unoptimizableRepartitionNode.nodeName());

        return new KStreamImpl<>(
            repartitionSourceName,
            keySerde,
            valueSerde,
            Collections.unmodifiableSet(sourceNodes),
            false,
            unoptimizableRepartitionNode,
            builder
        );
    }

    @Override
    public void to(final String topic) {
        to(topic, Produced.with(keySerde, valueSerde, null));
    }

    @Override
    public void to(final String topic,
                   final Produced<K, V> produced) {
        Objects.requireNonNull(topic, "topic can't be null");
        Objects.requireNonNull(produced, "produced can't be null");

        final ProducedInternal<K, V> producedInternal = new ProducedInternal<>(produced);
        if (producedInternal.keySerde() == null) {
            producedInternal.withKeySerde(keySerde);
        }
        if (producedInternal.valueSerde() == null) {
            producedInternal.withValueSerde(valueSerde);
        }
        to(new StaticTopicNameExtractor<>(topic), producedInternal);
    }

    @Override
    public void to(final TopicNameExtractor<K, V> topicExtractor) {
        to(topicExtractor, Produced.with(keySerde, valueSerde, null));
    }

    @Override
    public void to(final TopicNameExtractor<K, V> topicExtractor,
                   final Produced<K, V> produced) {
        Objects.requireNonNull(topicExtractor, "topicExtractor can't be null");
        Objects.requireNonNull(produced, "produced can't be null");

        final ProducedInternal<K, V> producedInternal = new ProducedInternal<>(produced);
        if (producedInternal.keySerde() == null) {
            producedInternal.withKeySerde(keySerde);
        }
        if (producedInternal.valueSerde() == null) {
            producedInternal.withValueSerde(valueSerde);
        }
        to(topicExtractor, producedInternal);
    }

    private void to(final TopicNameExtractor<K, V> topicExtractor,
                    final ProducedInternal<K, V> produced) {
        final String name = new NamedInternal(produced.name()).orElseGenerateWithPrefix(builder, SINK_NAME);
        final StreamSinkNode<K, V> sinkNode = new StreamSinkNode<>(
            name,
            topicExtractor,
            produced
        );

        builder.addGraphNode(graphNode, sinkNode);
    }

    @Override
    public KTable<K, V> toTable() {
        return toTable(NamedInternal.empty(), Materialized.with(keySerde, valueSerde));
    }

    @Override
    public KTable<K, V> toTable(final Named named) {
        return toTable(named, Materialized.with(keySerde, valueSerde));
    }

    @Override
    public KTable<K, V> toTable(final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return toTable(NamedInternal.empty(), materialized);
    }

    @Override
    public KTable<K, V> toTable(final Named named,
                                final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(named, "named can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final NamedInternal namedInternal = new NamedInternal(named);
        final String name = namedInternal.orElseGenerateWithPrefix(builder, TO_KTABLE_NAME);

        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, TO_KTABLE_NAME);

        final Serde<K> keySerdeOverride = materializedInternal.keySerde() == null
            ? keySerde
            : materializedInternal.keySerde();
        final Serde<V> valueSerdeOverride = materializedInternal.valueSerde() == null
            ? valueSerde
            : materializedInternal.valueSerde();

        final Set<String> subTopologySourceNodes;
        final GraphNode tableParentNode;

        if (repartitionRequired) {
            final OptimizableRepartitionNodeBuilder<K, V> repartitionNodeBuilder = optimizableRepartitionNodeBuilder();
            final String sourceName = createRepartitionedSource(
                builder,
                keySerdeOverride,
                valueSerdeOverride,
                name,
                null,
                repartitionNodeBuilder
            );

            tableParentNode = repartitionNodeBuilder.build();
            builder.addGraphNode(graphNode, tableParentNode);
            subTopologySourceNodes = Collections.singleton(sourceName);
        } else {
            tableParentNode = graphNode;
            subTopologySourceNodes = this.subTopologySourceNodes;
        }

        final KTableSource<K, V> tableSource = new KTableSource<>(materializedInternal);
        final ProcessorParameters<K, V, ?, ?> processorParameters = new ProcessorParameters<>(tableSource, name);
        final GraphNode tableNode = new StreamToTableNode<>(
            name,
            processorParameters
        );
        tableNode.setOutputVersioned(materializedInternal.storeSupplier() instanceof VersionedBytesStoreSupplier);

        builder.addGraphNode(tableParentNode, tableNode);

        return new KTableImpl<K, V, V>(
            name,
            keySerdeOverride,
            valueSerdeOverride,
            subTopologySourceNodes,
            materializedInternal.queryableStoreName(),
            tableSource,
            tableNode,
            builder
        );
    }

    @Override
    public <KR> KGroupedStream<KR, V> groupBy(final KeyValueMapper<? super K, ? super V, KR> keySelector) {
        return groupBy(keySelector, Grouped.with(null, valueSerde));
    }

    @Override
    public <KR> KGroupedStream<KR, V> groupBy(final KeyValueMapper<? super K, ? super V, KR> keySelector,
                                              final Grouped<KR, V> grouped) {
        Objects.requireNonNull(keySelector, "keySelector can't be null");
        Objects.requireNonNull(grouped, "grouped can't be null");

        final GroupedInternal<KR, V> groupedInternal = new GroupedInternal<>(grouped);
        final ProcessorGraphNode<K, V> selectKeyMapNode = internalSelectKey(keySelector, new NamedInternal(groupedInternal.name()));
        selectKeyMapNode.keyChangingOperation(true);

        builder.addGraphNode(graphNode, selectKeyMapNode);

        return new KGroupedStreamImpl<>(
            selectKeyMapNode.nodeName(),
            subTopologySourceNodes,
            groupedInternal,
            true,
            selectKeyMapNode,
            builder);
    }

    @Override
    public KGroupedStream<K, V> groupByKey() {
        return groupByKey(Grouped.with(keySerde, valueSerde));
    }

    @Override
    public KGroupedStream<K, V> groupByKey(final Grouped<K, V> grouped) {
        Objects.requireNonNull(grouped, "grouped can't be null");

        final GroupedInternal<K, V> groupedInternal = new GroupedInternal<>(grouped);

        return new KGroupedStreamImpl<>(
            name,
            subTopologySourceNodes,
            groupedInternal,
            repartitionRequired,
            graphNode,
            builder);
    }

    @Override
    public <VO, VR> KStream<K, VR> join(final KStream<K, VO> otherStream,
                                        final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                        final JoinWindows windows) {
        return join(otherStream, toValueJoinerWithKey(joiner), windows);
    }

    @Override
    public <VO, VR> KStream<K, VR> join(final KStream<K, VO> otherStream,
                                        final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
                                        final JoinWindows windows) {
        return join(otherStream, joiner, windows, StreamJoined.with(null, null, null));
    }

    @Override
    public <VO, VR> KStream<K, VR> join(final KStream<K, VO> otherStream,
                                        final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                        final JoinWindows windows,
                                        final StreamJoined<K, V, VO> streamJoined) {

        return join(otherStream, toValueJoinerWithKey(joiner), windows, streamJoined);
    }

    @Override
    public <VO, VR> KStream<K, VR> join(final KStream<K, VO> otherStream,
                                        final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
                                        final JoinWindows windows,
                                        final StreamJoined<K, V, VO> streamJoined) {

        return doJoin(
                otherStream,
                joiner,
                windows,
                streamJoined,
                new KStreamImplJoin(builder, false, false));
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(final KStream<K, VO> otherStream,
                                            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                            final JoinWindows windows) {
        return leftJoin(otherStream, toValueJoinerWithKey(joiner), windows);
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(final KStream<K, VO> otherStream,
                                            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
                                            final JoinWindows windows) {
        return leftJoin(otherStream, joiner, windows, StreamJoined.with(null, null, null));
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(final KStream<K, VO> otherStream,
                                            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                            final JoinWindows windows,
                                            final StreamJoined<K, V, VO> streamJoined) {
        return doJoin(
            otherStream,
            toValueJoinerWithKey(joiner),
            windows,
            streamJoined,
            new KStreamImplJoin(builder, true, false));
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(final KStream<K, VO> otherStream,
                                            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
                                            final JoinWindows windows,
                                            final StreamJoined<K, V, VO> streamJoined) {
        return doJoin(
                otherStream,
                joiner,
                windows,
                streamJoined,
                new KStreamImplJoin(builder, true, false));
    }

    @Override
    public <VO, VR> KStream<K, VR> outerJoin(final KStream<K, VO> otherStream,
                                             final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                             final JoinWindows windows) {
        return outerJoin(otherStream, toValueJoinerWithKey(joiner), windows);
    }

    @Override
    public <VO, VR> KStream<K, VR> outerJoin(final KStream<K, VO> otherStream,
                                             final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
                                             final JoinWindows windows) {
        return outerJoin(otherStream, joiner, windows, StreamJoined.with(null, null, null));
    }

    @Override
    public <VO, VR> KStream<K, VR> outerJoin(final KStream<K, VO> otherStream,
                                             final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                             final JoinWindows windows,
                                             final StreamJoined<K, V, VO> streamJoined) {

        return outerJoin(otherStream, toValueJoinerWithKey(joiner), windows, streamJoined);
    }

    @Override
    public <VO, VR> KStream<K, VR> outerJoin(final KStream<K, VO> otherStream,
                                             final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
                                             final JoinWindows windows,
                                             final StreamJoined<K, V, VO> streamJoined) {

        return doJoin(otherStream, joiner, windows, streamJoined, new KStreamImplJoin(builder, true, true));
    }

    private <VO, VR> KStream<K, VR> doJoin(final KStream<K, VO> otherStream,
                                           final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
                                           final JoinWindows windows,
                                           final StreamJoined<K, V, VO> streamJoined,
                                           final KStreamImplJoin join) {
        Objects.requireNonNull(otherStream, "otherStream can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(windows, "windows can't be null");
        Objects.requireNonNull(streamJoined, "streamJoined can't be null");

        KStreamImpl<K, V> joinThis = this;
        KStreamImpl<K, VO> joinOther = (KStreamImpl<K, VO>) otherStream;

        final StreamJoinedInternal<K, V, VO> streamJoinedInternal = new StreamJoinedInternal<>(streamJoined, builder);
        final NamedInternal name = new NamedInternal(streamJoinedInternal.name());
        if (joinThis.repartitionRequired) {
            final String joinThisName = joinThis.name;
            final String leftJoinRepartitionTopicName = name.suffixWithOrElseGet("-left", joinThisName);
            joinThis = joinThis.repartitionForJoin(leftJoinRepartitionTopicName, streamJoinedInternal.keySerde(), streamJoinedInternal.valueSerde());
        }

        if (joinOther.repartitionRequired) {
            final String joinOtherName = joinOther.name;
            final String rightJoinRepartitionTopicName = name.suffixWithOrElseGet("-right", joinOtherName);
            joinOther = joinOther.repartitionForJoin(rightJoinRepartitionTopicName, streamJoinedInternal.keySerde(), streamJoinedInternal.otherValueSerde());
        }

        joinThis.ensureCopartitionWith(Collections.singleton(joinOther));

        return join.join(
            joinThis,
            joinOther,
            joiner,
            windows,
            streamJoined);
    }

    /**
     * Repartition a stream. This is required on join operations occurring after
     * an operation that changes the key, i.e, selectKey, map(..), flatMap(..).
     */
    private KStreamImpl<K, V> repartitionForJoin(final String repartitionName,
                                                 final Serde<K> keySerdeOverride,
                                                 final Serde<V> valueSerdeOverride) {
        final Serde<K> repartitionKeySerde = keySerdeOverride != null ? keySerdeOverride : keySerde;
        final Serde<V> repartitionValueSerde = valueSerdeOverride != null ? valueSerdeOverride : valueSerde;
        final OptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder =
            OptimizableRepartitionNode.optimizableRepartitionNodeBuilder();
        // we still need to create the repartitioned source each time
        // as it increments the counter which
        // is needed to maintain topology compatibility
        final String repartitionedSourceName = createRepartitionedSource(
            builder,
            repartitionKeySerde,
            repartitionValueSerde,
            repartitionName,
            null,
            optimizableRepartitionNodeBuilder);

        if (repartitionNode == null || !name.equals(repartitionName)) {
            repartitionNode = optimizableRepartitionNodeBuilder.build();
            builder.addGraphNode(graphNode, repartitionNode);
        }

        return new KStreamImpl<>(
            repartitionedSourceName,
            repartitionKeySerde,
            repartitionValueSerde,
            Collections.singleton(repartitionedSourceName),
            false,
            repartitionNode,
            builder);
    }

    static <K1, V1, RN extends BaseRepartitionNode<K1, V1>> String createRepartitionedSource(final InternalStreamsBuilder builder,
                                                                                             final Serde<K1> keySerde,
                                                                                             final Serde<V1> valueSerde,
                                                                                             final String repartitionTopicNamePrefix,
                                                                                             final StreamPartitioner<K1, V1> streamPartitioner,
                                                                                             final BaseRepartitionNodeBuilder<K1, V1, RN> baseRepartitionNodeBuilder) {

        final String repartitionTopicName = repartitionTopicNamePrefix.endsWith(REPARTITION_TOPIC_SUFFIX) ?
            repartitionTopicNamePrefix :
            repartitionTopicNamePrefix + REPARTITION_TOPIC_SUFFIX;

        // Always need to generate the names to burn index counter for compatibility
        final String genSinkName = builder.newProcessorName(SINK_NAME);
        final String genNullKeyFilterProcessorName = builder.newProcessorName(FILTER_NAME);
        final String genSourceName = builder.newProcessorName(SOURCE_NAME);

        final String sinkName;
        final String sourceName;
        final String nullKeyFilterProcessorName;

        if (repartitionTopicNamePrefix.matches("KSTREAM.*-[0-9]{10}")) {
            sinkName = genSinkName;
            sourceName = genSourceName;
            nullKeyFilterProcessorName = genNullKeyFilterProcessorName;
        } else {
            sinkName = repartitionTopicName + "-sink";
            sourceName = repartitionTopicName + "-source";
            nullKeyFilterProcessorName = repartitionTopicName + "-filter";
        }

        final ProcessorParameters<K1, V1, ?, ?> processorParameters = new ProcessorParameters<>(
            new KStreamFilter<>((k, v) -> true, false),
            nullKeyFilterProcessorName
        );

        baseRepartitionNodeBuilder.withKeySerde(keySerde)
                                  .withValueSerde(valueSerde)
                                  .withSourceName(sourceName)
                                  .withRepartitionTopic(repartitionTopicName)
                                  .withSinkName(sinkName)
                                  .withProcessorParameters(processorParameters)
                                  .withStreamPartitioner(streamPartitioner)
                                  // reusing the source name for the graph node name
                                  // adding explicit variable as it simplifies logic
                                  .withNodeName(sourceName);

        return sourceName;
    }

    @Override
    public <VO, VR> KStream<K, VR> join(final KTable<K, VO> table,
                                        final ValueJoiner<? super V, ? super VO, ? extends VR> joiner) {
        return join(table, toValueJoinerWithKey(joiner));
    }

    @Override
    public <VO, VR> KStream<K, VR> join(final KTable<K, VO> table,
                                        final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner) {
        return join(table, joiner, Joined.with(null, null, null));
    }

    @Override
    public <VO, VR> KStream<K, VR> join(final KTable<K, VO> table,
                                        final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                        final Joined<K, V, VO> joined) {
        Objects.requireNonNull(table, "table can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(joined, "joined can't be null");
        return join(table, toValueJoinerWithKey(joiner), joined);
    }

    @Override
    public <VO, VR> KStream<K, VR> join(final KTable<K, VO> table,
                                        final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
                                        final Joined<K, V, VO> joined) {
        Objects.requireNonNull(table, "table can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(joined, "joined can't be null");

        final JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<>(joined);
        final String name = joinedInternal.name();

        if (repartitionRequired) {
            final KStreamImpl<K, V> thisStreamRepartitioned = repartitionForJoin(
                    name != null ? name : this.name,
                    joinedInternal.keySerde(),
                    joinedInternal.leftValueSerde()
            );
            return thisStreamRepartitioned.doStreamTableJoin(table, joiner, joinedInternal, false);
        } else {
            return doStreamTableJoin(table, joiner, joinedInternal, false);
        }
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(final KTable<K, VO> table, final ValueJoiner<? super V, ? super VO, ? extends VR> joiner) {
        return leftJoin(table, toValueJoinerWithKey(joiner));
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(final KTable<K, VO> table, final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner) {
        return leftJoin(table, joiner, Joined.with(null, null, null));
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(final KTable<K, VO> table,
                                            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                            final Joined<K, V, VO> joined) {
        Objects.requireNonNull(table, "table can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(joined, "joined can't be null");

        return leftJoin(table, toValueJoinerWithKey(joiner), joined);
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(final KTable<K, VO> table,
                                            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
                                            final Joined<K, V, VO> joined) {
        Objects.requireNonNull(table, "table can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(joined, "joined can't be null");
        final JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<>(joined);
        final String name = joinedInternal.name();

        if (repartitionRequired) {
            final KStreamImpl<K, V> thisStreamRepartitioned = repartitionForJoin(
                    name != null ? name : this.name,
                    joinedInternal.keySerde(),
                    joinedInternal.leftValueSerde()
            );
            return thisStreamRepartitioned.doStreamTableJoin(table, joiner, joinedInternal, true);
        } else {
            return doStreamTableJoin(table, joiner, joinedInternal, true);
        }
    }

    @Override
    public <KG, VG, VR> KStream<K, VR> join(final GlobalKTable<KG, VG> globalTable,
                                            final KeyValueMapper<? super K, ? super V, ? extends KG> keySelector,
                                            final ValueJoiner<? super V, ? super VG, ? extends VR> joiner) {
        return join(globalTable, keySelector, toValueJoinerWithKey(joiner));
    }

    @Override
    public <KG, VG, VR> KStream<K, VR> join(final GlobalKTable<KG, VG> globalTable,
                                            final KeyValueMapper<? super K, ? super V, ? extends KG> keySelector,
                                            final ValueJoinerWithKey<? super K, ? super V, ? super VG, ? extends VR> joiner) {
        return globalTableJoin(globalTable, keySelector, joiner, false, NamedInternal.empty());
    }

    @Override
    public <KG, VG, VR> KStream<K, VR> join(final GlobalKTable<KG, VG> globalTable,
                                            final KeyValueMapper<? super K, ? super V, ? extends KG> keySelector,
                                            final ValueJoiner<? super V, ? super VG, ? extends VR> joiner,
                                            final Named named) {
        return join(globalTable, keySelector, toValueJoinerWithKey(joiner), named);
    }

    @Override
    public <KG, VG, VR> KStream<K, VR> join(final GlobalKTable<KG, VG> globalTable,
                                            final KeyValueMapper<? super K, ? super V, ? extends KG> keySelector,
                                            final ValueJoinerWithKey<? super K, ? super V, ? super VG, ? extends VR> joiner,
                                            final Named named) {
        return globalTableJoin(globalTable, keySelector, joiner, false, named);
    }

    @Override
    public <KG, VG, VR> KStream<K, VR> leftJoin(final GlobalKTable<KG, VG> globalTable,
                                                final KeyValueMapper<? super K, ? super V, ? extends KG> keySelector,
                                                final ValueJoiner<? super V, ? super VG, ? extends VR> joiner) {
        return leftJoin(globalTable, keySelector, toValueJoinerWithKey(joiner));
    }

    @Override
    public <KG, VG, VR> KStream<K, VR> leftJoin(final GlobalKTable<KG, VG> globalTable,
                                                final KeyValueMapper<? super K, ? super V, ? extends KG> keySelector,
                                                final ValueJoinerWithKey<? super K, ? super V, ? super VG, ? extends VR> joiner) {
        return globalTableJoin(globalTable, keySelector, joiner, true, NamedInternal.empty());
    }

    @Override
    public <KG, VG, VR> KStream<K, VR> leftJoin(final GlobalKTable<KG, VG> globalTable,
                                                final KeyValueMapper<? super K, ? super V, ? extends KG> keySelector,
                                                final ValueJoiner<? super V, ? super VG, ? extends VR> joiner,
                                                final Named named) {
        return leftJoin(globalTable, keySelector, toValueJoinerWithKey(joiner), named);
    }

    @Override
    public <KG, VG, VR> KStream<K, VR> leftJoin(final GlobalKTable<KG, VG> globalTable,
                                                final KeyValueMapper<? super K, ? super V, ? extends KG> keySelector,
                                                final ValueJoinerWithKey<? super K, ? super V, ? super VG, ? extends VR> joiner,
                                                final Named named) {
        return globalTableJoin(globalTable, keySelector, joiner, true, named);
    }

    private <KG, VG, VR> KStream<K, VR> globalTableJoin(final GlobalKTable<KG, VG> globalTable,
                                                        final KeyValueMapper<? super K, ? super V, ? extends KG> keySelector,
                                                        final ValueJoinerWithKey<? super K, ? super V, ? super VG, ? extends VR> joiner,
                                                        final boolean leftJoin,
                                                        final Named named) {
        Objects.requireNonNull(globalTable, "globalTable can't be null");
        Objects.requireNonNull(keySelector, "keySelector can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(named, "named can't be null");

        final KTableValueGetterSupplier<KG, VG> valueGetterSupplier =
            ((GlobalKTableImpl<KG, VG>) globalTable).valueGetterSupplier();
        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, LEFTJOIN_NAME);
        final ProcessorSupplier<K, V, K, VR> processorSupplier = new KStreamGlobalKTableJoin<>(
            valueGetterSupplier,
            joiner,
            keySelector,
            leftJoin);
        final ProcessorParameters<K, V, ?, ?> processorParameters = new ProcessorParameters<>(processorSupplier, name);
        final StreamTableJoinNode<K, V> streamTableJoinNode =
            new StreamTableJoinNode<>(name, processorParameters, new String[] {}, null, null);

        if (leftJoin) {
            streamTableJoinNode.labels().add(GraphNode.Label.NULL_KEY_RELAXED_JOIN);
        }
        builder.addGraphNode(graphNode, streamTableJoinNode);

        // do not have serde for joined result
        return new KStreamImpl<>(
            name,
            keySerde,
            null,
            subTopologySourceNodes,
            repartitionRequired,
            streamTableJoinNode,
            builder);
    }

    @SuppressWarnings("unchecked")
    private <VO, VR> KStream<K, VR> doStreamTableJoin(final KTable<K, VO> table,
                                                      final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
                                                      final JoinedInternal<K, V, VO> joinedInternal,
                                                      final boolean leftJoin) {
        Objects.requireNonNull(table, "table can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

        final Set<String> allSourceNodes = ensureCopartitionWith(Collections.singleton((AbstractStream<K, VO>) table));

        final NamedInternal renamed = new NamedInternal(joinedInternal.name());

        final String name = renamed.orElseGenerateWithPrefix(builder, leftJoin ? LEFTJOIN_NAME : JOIN_NAME);

        Optional<StoreBuilder<?>> bufferStoreBuilder = Optional.empty();

        if (joinedInternal.gracePeriod() != null) {
            if (!((KTableImpl<K, ?, VO>) table).graphNode.isOutputVersioned().orElse(true)) {
                throw new IllegalArgumentException("KTable must be versioned to use a grace period in a stream table join.");
            }
            final String bufferName = name + "-Buffer";
            bufferStoreBuilder = Optional.of(new RocksDBTimeOrderedKeyValueBuffer.Builder<>(bufferName, joinedInternal.gracePeriod(), name));
        }

        final ProcessorSupplier<K, V, K, ? extends VR> processorSupplier = new KStreamKTableJoin<>(
            ((KTableImpl<K, ?, VO>) table).valueGetterSupplier(),
            joiner,
            leftJoin,
            Optional.ofNullable(joinedInternal.gracePeriod()),
            bufferStoreBuilder
        );

        final ProcessorParameters<K, V, ?, ?> processorParameters = new ProcessorParameters<>(processorSupplier, name);
        final StreamTableJoinNode<K, V> streamTableJoinNode = new StreamTableJoinNode<>(
            name,
            processorParameters,
            ((KTableImpl<K, ?, VO>) table).valueGetterSupplier().storeNames(),
            this.name,
            joinedInternal.gracePeriod()
        );

        builder.addGraphNode(graphNode, streamTableJoinNode);
        if (leftJoin) {
            streamTableJoinNode.labels().add(GraphNode.Label.NULL_KEY_RELAXED_JOIN);
        }

        // do not have serde for joined result
        return new KStreamImpl<>(
            name,
            joinedInternal.keySerde() != null ? joinedInternal.keySerde() : keySerde,
            null,
            allSourceNodes,
            false,
            streamTableJoinNode,
            builder);
    }

    @Override
    public <KOut, VOut> KStream<KOut, VOut> process(
        final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
        final String... stateStoreNames
    ) {
        return process(
            processorSupplier,
            Named.as(builder.newProcessorName(PROCESSOR_NAME)),
            stateStoreNames
        );
    }

    @Override
    public <KOut, VOut> KStream<KOut, VOut> process(
        final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
        final Named named,
        final String... stateStoreNames
    ) {
        Objects.requireNonNull(processorSupplier, "processorSupplier can't be null");
        Objects.requireNonNull(named, "named can't be null");
        Objects.requireNonNull(stateStoreNames, "stateStoreNames can't be a null array");
        ApiUtils.checkSupplier(processorSupplier);
        for (final String stateStoreName : stateStoreNames) {
            Objects.requireNonNull(stateStoreName, "stateStoreNames can't be null");
        }

        final String name = new NamedInternal(named).name();
        final ProcessorToStateConnectorNode<? super K, ? super V> processNode = new
            ProcessorToStateConnectorNode<>(
            name,
            new ProcessorParameters<>(processorSupplier, name),
            stateStoreNames);

        builder.addGraphNode(graphNode, processNode);

        // cannot inherit key and value serde
        return new KStreamImpl<>(
            name,
            null,
            null,
            subTopologySourceNodes,
            true,
            processNode,
            builder);
    }

    @Override
    public <VOut> KStream<K, VOut> processValues(
        final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
        final String... stateStoreNames
    ) {
        return processValues(
            processorSupplier,
            Named.as(builder.newProcessorName(PROCESSVALUES_NAME)),
            stateStoreNames
        );
    }

    @Override
    public <VOut> KStream<K, VOut> processValues(
        final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
        final Named named,
        final String... stateStoreNames
    ) {
        Objects.requireNonNull(processorSupplier, "processorSupplier can't be null");
        Objects.requireNonNull(named, "named can't be null");
        Objects.requireNonNull(stateStoreNames, "stateStoreNames can't be a null array");
        ApiUtils.checkSupplier(processorSupplier);
        for (final String stateStoreName : stateStoreNames) {
            Objects.requireNonNull(stateStoreName, "stateStoreNames can't be null");
        }

        final String name = new NamedInternal(named).name();
        final ProcessorToStateConnectorNode<? super K, ? super V> processNode = new ProcessorToStateConnectorNode<>(
            name,
            new ProcessorParameters<>(processorSupplier, name),
            stateStoreNames);

        builder.addGraphNode(graphNode, processNode);
        // cannot inherit value serde
        return new KStreamImpl<>(
            name,
            keySerde,
            null,
            subTopologySourceNodes,
            repartitionRequired,
            processNode,
            builder);
    }
}
