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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamSinkNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamStreamJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamTableJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.internals.StaticTopicNameExtractor;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class KStreamImpl<K, V> extends AbstractStream<K, V> implements KStream<K, V> {

    static final String SOURCE_NAME = "KSTREAM-SOURCE-";

    static final String SINK_NAME = "KSTREAM-SINK-";

    static final String REPARTITION_TOPIC_SUFFIX = "-repartition";

    private static final String BRANCH_NAME = "KSTREAM-BRANCH-";

    private static final String BRANCHCHILD_NAME = "KSTREAM-BRANCHCHILD-";

    private static final String FILTER_NAME = "KSTREAM-FILTER-";

    private static final String PEEK_NAME = "KSTREAM-PEEK-";

    private static final String FLATMAP_NAME = "KSTREAM-FLATMAP-";

    private static final String FLATMAPVALUES_NAME = "KSTREAM-FLATMAPVALUES-";

    private static final String JOINTHIS_NAME = "KSTREAM-JOINTHIS-";

    private static final String JOINOTHER_NAME = "KSTREAM-JOINOTHER-";

    private static final String JOIN_NAME = "KSTREAM-JOIN-";

    private static final String LEFTJOIN_NAME = "KSTREAM-LEFTJOIN-";

    private static final String MAP_NAME = "KSTREAM-MAP-";

    private static final String MAPVALUES_NAME = "KSTREAM-MAPVALUES-";

    private static final String MERGE_NAME = "KSTREAM-MERGE-";

    private static final String OUTERTHIS_NAME = "KSTREAM-OUTERTHIS-";

    private static final String OUTEROTHER_NAME = "KSTREAM-OUTEROTHER-";

    private static final String PROCESSOR_NAME = "KSTREAM-PROCESSOR-";

    private static final String PRINTING_NAME = "KSTREAM-PRINTER-";

    private static final String KEY_SELECT_NAME = "KSTREAM-KEY-SELECT-";

    private static final String TRANSFORM_NAME = "KSTREAM-TRANSFORM-";

    private static final String TRANSFORMVALUES_NAME = "KSTREAM-TRANSFORMVALUES-";

    private static final String WINDOWED_NAME = "KSTREAM-WINDOWED-";

    private static final String FOREACH_NAME = "KSTREAM-FOREACH-";

    private final boolean repartitionRequired;

    KStreamImpl(final String name,
                final Serde<K> keySerde,
                final Serde<V> valueSerde,
                final Set<String> sourceNodes,
                final boolean repartitionRequired,
                final StreamsGraphNode streamsGraphNode,
                final InternalStreamsBuilder builder) {
        super(name, keySerde, valueSerde, sourceNodes, streamsGraphNode, builder);
        this.repartitionRequired = repartitionRequired;
    }

    @Override
    public KStream<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        final String name = builder.newProcessorName(FILTER_NAME);


        final ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamFilter<>(predicate, false), name);
        final ProcessorGraphNode<? super K, ? super V> filterProcessorNode = new ProcessorGraphNode<>(name, processorParameters);
        builder.addGraphNode(this.streamsGraphNode, filterProcessorNode);

        return new KStreamImpl<>(name,
                                 keySerde,
                                 valSerde,
                                 sourceNodes,
                                 repartitionRequired,
                                 filterProcessorNode,
                                 builder);
    }

    @Override
    public KStream<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        final String name = builder.newProcessorName(FILTER_NAME);

        final ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamFilter<>(predicate, true), name);
        final ProcessorGraphNode<? super K, ? super V> filterNotProcessorNode = new ProcessorGraphNode<>(name, processorParameters);

        builder.addGraphNode(this.streamsGraphNode, filterNotProcessorNode);

        return new KStreamImpl<>(name,
                                 keySerde,
                                 valSerde,
                                 sourceNodes,
                                 repartitionRequired,
                                 filterNotProcessorNode,
                                 builder);
    }

    @Override
    public <KR> KStream<KR, V> selectKey(final KeyValueMapper<? super K, ? super V, ? extends KR> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");

        final ProcessorGraphNode<K, V> selectKeyProcessorNode = internalSelectKey(mapper, NamedInternal.empty());

        selectKeyProcessorNode.keyChangingOperation(true);
        builder.addGraphNode(this.streamsGraphNode, selectKeyProcessorNode);

        // key serde cannot be preserved
        return new KStreamImpl<>(selectKeyProcessorNode.nodeName(), null, valSerde, sourceNodes, true, selectKeyProcessorNode, builder);
    }


    private <KR> ProcessorGraphNode<K, V> internalSelectKey(final KeyValueMapper<? super K, ? super V, ? extends KR> mapper,
                                                            final NamedInternal named) {
        final String name = named.orElseGenerateWithPrefix(builder, KEY_SELECT_NAME);
        final KStreamMap<K, V, KR, V> kStreamMap = new KStreamMap<>((key, value) -> new KeyValue<>(mapper.apply(key, value), value));

        final ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(kStreamMap, name);

        return new ProcessorGraphNode<>(name, processorParameters);
    }

    @Override
    public <KR, VR> KStream<KR, VR> map(final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        final String name = builder.newProcessorName(MAP_NAME);

        final ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamMap<>(mapper), name);

        final ProcessorGraphNode<? super K, ? super V> mapProcessorNode = new ProcessorGraphNode<>(name, processorParameters);

        mapProcessorNode.keyChangingOperation(true);
        builder.addGraphNode(this.streamsGraphNode, mapProcessorNode);

        // key and value serde cannot be preserved
        return new KStreamImpl<>(name,
                                 null,
                                 null,
                                 sourceNodes,
                                 true,
                                 mapProcessorNode,
                                 builder);
    }


    @Override
    public <VR> KStream<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper) {
        return mapValues(withKey(mapper));
    }

    @Override
    public <VR> KStream<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        final String name = builder.newProcessorName(MAPVALUES_NAME);

        final ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamMapValues<>(mapper), name);
        final ProcessorGraphNode<? super K, ? super V> mapValuesProcessorNode = new ProcessorGraphNode<>(name, processorParameters);

        mapValuesProcessorNode.setValueChangingOperation(true);
        builder.addGraphNode(this.streamsGraphNode, mapValuesProcessorNode);

        // value serde cannot be preserved
        return new KStreamImpl<>(name,
                                 keySerde,
                                 null,
                                 sourceNodes,
                                 repartitionRequired,
                                 mapValuesProcessorNode,
                                 builder);
    }

    @Override
    public void print(final Printed<K, V> printed) {
        Objects.requireNonNull(printed, "printed can't be null");
        final PrintedInternal<K, V> printedInternal = new PrintedInternal<>(printed);
        final String name = new NamedInternal(printedInternal.name()).orElseGenerateWithPrefix(builder, PRINTING_NAME);
        final ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(printedInternal.build(this.name), name);
        final ProcessorGraphNode<? super K, ? super V> printNode = new ProcessorGraphNode<>(name, processorParameters);

        builder.addGraphNode(this.streamsGraphNode, printNode);
    }

    @Override
    public <KR, VR> KStream<KR, VR> flatMap(final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        final String name = builder.newProcessorName(FLATMAP_NAME);

        final ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamFlatMap<>(mapper), name);
        final ProcessorGraphNode<? super K, ? super V> flatMapNode = new ProcessorGraphNode<>(name, processorParameters);
        flatMapNode.keyChangingOperation(true);

        builder.addGraphNode(this.streamsGraphNode, flatMapNode);

        // key and value serde cannot be preserved
        return new KStreamImpl<>(name,
                                 null,
                                 null,
                                 sourceNodes,
                                 true,
                                 flatMapNode,
                                 builder);
    }

    @Override
    public <VR> KStream<K, VR> flatMapValues(final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper) {
        return flatMapValues(withKey(mapper));
    }

    @Override
    public <VR> KStream<K, VR> flatMapValues(final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        final String name = builder.newProcessorName(FLATMAPVALUES_NAME);

        final ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamFlatMapValues<>(mapper), name);
        final ProcessorGraphNode<? super K, ? super V> flatMapValuesNode = new ProcessorGraphNode<>(name, processorParameters);

        flatMapValuesNode.setValueChangingOperation(true);
        builder.addGraphNode(this.streamsGraphNode, flatMapValuesNode);

        // value serde cannot be preserved
        return new KStreamImpl<>(name, keySerde, null, sourceNodes, this.repartitionRequired, flatMapValuesNode, builder);
    }

    @Override
    @SuppressWarnings("unchecked")
    public KStream<K, V>[] branch(final Predicate<? super K, ? super V>... predicates) {
        if (predicates.length == 0) {
            throw new IllegalArgumentException("you must provide at least one predicate");
        }
        for (final Predicate<? super K, ? super V> predicate : predicates) {
            Objects.requireNonNull(predicate, "predicates can't have null values");
        }

        final String branchName = builder.newProcessorName(BRANCH_NAME);

        final String[] childNames = new String[predicates.length];
        for (int i = 0; i < predicates.length; i++) {
            childNames[i] = builder.newProcessorName(BRANCHCHILD_NAME);
        }

        final ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamBranch(predicates.clone(), childNames), branchName);
        final ProcessorGraphNode<K, V> branchNode = new ProcessorGraphNode<>(branchName, processorParameters);
        builder.addGraphNode(this.streamsGraphNode, branchNode);

        final KStream<K, V>[] branchChildren = (KStream<K, V>[]) Array.newInstance(KStream.class, predicates.length);

        for (int i = 0; i < predicates.length; i++) {
            final ProcessorParameters innerProcessorParameters = new ProcessorParameters<>(new KStreamPassThrough<K, V>(), childNames[i]);
            final ProcessorGraphNode<K, V> branchChildNode = new ProcessorGraphNode<>(childNames[i], innerProcessorParameters);

            builder.addGraphNode(branchNode, branchChildNode);
            branchChildren[i] = new KStreamImpl<>(childNames[i], keySerde, valSerde, sourceNodes, repartitionRequired, branchChildNode, builder);
        }

        return branchChildren;
    }

    @Override
    public KStream<K, V> merge(final KStream<K, V> stream) {
        Objects.requireNonNull(stream);
        return merge(builder, stream);
    }

    private KStream<K, V> merge(final InternalStreamsBuilder builder,
                                final KStream<K, V> stream) {
        final KStreamImpl<K, V> streamImpl = (KStreamImpl<K, V>) stream;
        final String name = builder.newProcessorName(MERGE_NAME);
        final Set<String> allSourceNodes = new HashSet<>();

        final boolean requireRepartitioning = streamImpl.repartitionRequired || repartitionRequired;
        allSourceNodes.addAll(sourceNodes);
        allSourceNodes.addAll(streamImpl.sourceNodes);

        final ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamPassThrough<>(), name);


        final ProcessorGraphNode<? super K, ? super V> mergeNode = new ProcessorGraphNode<>(name, processorParameters);

        mergeNode.setMergeNode(true);
        builder.addGraphNode(Arrays.asList(this.streamsGraphNode, streamImpl.streamsGraphNode), mergeNode);

        // drop the serde as we cannot safely use either one to represent both streams
        return new KStreamImpl<>(name, null, null, allSourceNodes, requireRepartitioning, mergeNode, builder);
    }

    @Override
    public void foreach(final ForeachAction<? super K, ? super V> action) {
        Objects.requireNonNull(action, "action can't be null");
        final String name = builder.newProcessorName(FOREACH_NAME);

        final ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(
            new KStreamPeek<>(action, false),
            name
        );

        final ProcessorGraphNode<? super K, ? super V> foreachNode = new ProcessorGraphNode<>(name, processorParameters);
        builder.addGraphNode(this.streamsGraphNode, foreachNode);
    }

    @Override
    public KStream<K, V> peek(final ForeachAction<? super K, ? super V> action) {
        Objects.requireNonNull(action, "action can't be null");
        final String name = builder.newProcessorName(PEEK_NAME);

        final ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(
            new KStreamPeek<>(action, true),
            name
        );

        final ProcessorGraphNode<? super K, ? super V> peekNode = new ProcessorGraphNode<>(name, processorParameters);

        builder.addGraphNode(this.streamsGraphNode, peekNode);

        return new KStreamImpl<>(name, keySerde, valSerde, sourceNodes, repartitionRequired, peekNode, builder);
    }

    @Override
    public KStream<K, V> through(final String topic) {
        return through(topic, Produced.with(keySerde, valSerde, null));
    }

    @Override
    public KStream<K, V> through(final String topic, final Produced<K, V> produced) {
        Objects.requireNonNull(topic, "topic can't be null");
        Objects.requireNonNull(produced, "Produced can't be null");
        final ProducedInternal<K, V> producedInternal = new ProducedInternal<>(produced);
        if (producedInternal.keySerde() == null) {
            producedInternal.withKeySerde(keySerde);
        }
        if (producedInternal.valueSerde() == null) {
            producedInternal.withValueSerde(valSerde);
        }
        to(topic, producedInternal);
        return builder.stream(
            Collections.singleton(topic),
            new ConsumedInternal<>(
                producedInternal.keySerde(),
                producedInternal.valueSerde(),
                new FailOnInvalidTimestamp(),
                null
            )
        );
    }

    @Override
    public void to(final String topic) {
        to(topic, Produced.with(keySerde, valSerde, null));
    }

    @Override
    public void to(final String topic, final Produced<K, V> produced) {
        Objects.requireNonNull(topic, "topic can't be null");
        Objects.requireNonNull(produced, "Produced can't be null");
        final ProducedInternal<K, V> producedInternal = new ProducedInternal<>(produced);
        if (producedInternal.keySerde() == null) {
            producedInternal.withKeySerde(keySerde);
        }
        if (producedInternal.valueSerde() == null) {
            producedInternal.withValueSerde(valSerde);
        }
        to(new StaticTopicNameExtractor<>(topic), producedInternal);
    }

    @Override
    public void to(final TopicNameExtractor<K, V> topicExtractor) {
        to(topicExtractor, Produced.with(keySerde, valSerde, null));
    }

    @Override
    public void to(final TopicNameExtractor<K, V> topicExtractor, final Produced<K, V> produced) {
        Objects.requireNonNull(topicExtractor, "topic extractor can't be null");
        Objects.requireNonNull(produced, "Produced can't be null");
        final ProducedInternal<K, V> producedInternal = new ProducedInternal<>(produced);
        if (producedInternal.keySerde() == null) {
            producedInternal.withKeySerde(keySerde);
        }
        if (producedInternal.valueSerde() == null) {
            producedInternal.withValueSerde(valSerde);
        }
        to(topicExtractor, producedInternal);
    }

    private void to(final TopicNameExtractor<K, V> topicExtractor, final ProducedInternal<K, V> produced) {
        final String name = new NamedInternal(produced.name()).orElseGenerateWithPrefix(builder, SINK_NAME);
        final StreamSinkNode<K, V> sinkNode = new StreamSinkNode<>(
            name,
            topicExtractor,
            produced
        );

        builder.addGraphNode(this.streamsGraphNode, sinkNode);
    }

    private <K1, V1> KStream<K1, V1> doFlatTransform(final TransformerSupplier<? super K, ? super V, Iterable<KeyValue<K1, V1>>> transformerSupplier,
                                                     final String... stateStoreNames) {
        final String name = builder.newProcessorName(TRANSFORM_NAME);
        final StatefulProcessorNode<? super K, ? super V> transformNode = new StatefulProcessorNode<>(
            name,
            new ProcessorParameters<>(new KStreamFlatTransform<>(transformerSupplier), name),
            stateStoreNames
        );

        transformNode.keyChangingOperation(true);
        builder.addGraphNode(this.streamsGraphNode, transformNode);

        // cannot inherit key and value serde
        return new KStreamImpl<>(name, null, null, sourceNodes, true, transformNode, builder);
    }

    @Override
    public <KR, VR> KStream<KR, VR> transform(final TransformerSupplier<? super K, ? super V, KeyValue<KR, VR>> transformerSupplier,
                                              final String... stateStoreNames) {
        Objects.requireNonNull(transformerSupplier, "transformerSupplier can't be null");
        return doFlatTransform(new TransformerSupplierAdapter<>(transformerSupplier), stateStoreNames);
    }

    @Override
    public <K1, V1> KStream<K1, V1> flatTransform(final TransformerSupplier<? super K, ? super V, Iterable<KeyValue<K1, V1>>> transformerSupplier,
                                                  final String... stateStoreNames) {
        Objects.requireNonNull(transformerSupplier, "transformerSupplier can't be null");
        return doFlatTransform(transformerSupplier, stateStoreNames);
    }

    @Override
    public <VR> KStream<K, VR> transformValues(final ValueTransformerSupplier<? super V, ? extends VR> valueTransformerSupplier,
                                               final String... stateStoreNames) {
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformerSupplier can't be null");

        return doTransformValues(toValueTransformerWithKeySupplier(valueTransformerSupplier), stateStoreNames);
    }

    @Override
    public <VR> KStream<K, VR> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> valueTransformerSupplier,
                                               final String... stateStoreNames) {
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformerSupplier can't be null");

        return doTransformValues(valueTransformerSupplier, stateStoreNames);
    }

    private <VR> KStream<K, VR> doTransformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> valueTransformerWithKeySupplier,
                                                  final String... stateStoreNames) {
        final String name = builder.newProcessorName(TRANSFORMVALUES_NAME);

        final StatefulProcessorNode<? super K, ? super V> transformNode = new StatefulProcessorNode<>(
            name,
            new ProcessorParameters<>(new KStreamTransformValues<>(valueTransformerWithKeySupplier), name),
            stateStoreNames
        );

        transformNode.setValueChangingOperation(true);
        builder.addGraphNode(this.streamsGraphNode, transformNode);

        // cannot inherit value serde
        return new KStreamImpl<>(name, keySerde, null, sourceNodes, repartitionRequired, transformNode, builder);
    }

    @Override
    public <VR> KStream<K, VR> flatTransformValues(final ValueTransformerSupplier<? super V, Iterable<VR>> valueTransformerSupplier,
                                                   final String... stateStoreNames) {
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformerSupplier can't be null");

        return doFlatTransformValues(toValueTransformerWithKeySupplier(valueTransformerSupplier), stateStoreNames);
    }

    @Override
    public <VR> KStream<K, VR> flatTransformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, Iterable<VR>> valueTransformerSupplier,
                                                   final String... stateStoreNames) {
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformerSupplier can't be null");

        return doFlatTransformValues(valueTransformerSupplier, stateStoreNames);
    }

    private <VR> KStream<K, VR> doFlatTransformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, Iterable<VR>> valueTransformerWithKeySupplier,
                                                      final String... stateStoreNames) {
        final String name = builder.newProcessorName(TRANSFORMVALUES_NAME);

        final StatefulProcessorNode<? super K, ? super V> transformNode = new StatefulProcessorNode<>(
            name,
            new ProcessorParameters<>(new KStreamFlatTransformValues<>(valueTransformerWithKeySupplier), name),
            stateStoreNames
        );

        transformNode.setValueChangingOperation(true);
        builder.addGraphNode(this.streamsGraphNode, transformNode);

        // cannot inherit value serde
        return new KStreamImpl<>(name, keySerde, null, sourceNodes, repartitionRequired, transformNode, builder);
    }

    @Override
    public void process(final ProcessorSupplier<? super K, ? super V> processorSupplier,
                        final String... stateStoreNames) {

        Objects.requireNonNull(processorSupplier, "ProcessSupplier cant' be null");
        final String name = builder.newProcessorName(PROCESSOR_NAME);

        final StatefulProcessorNode<? super K, ? super V> processNode = new StatefulProcessorNode<>(
            name,
            new ProcessorParameters<>(processorSupplier, name),
            stateStoreNames
        );

        builder.addGraphNode(this.streamsGraphNode, processNode);
    }

    @Override
    public <VO, VR> KStream<K, VR> join(final KStream<K, VO> other,
                                        final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                        final JoinWindows windows) {
        return join(other, joiner, windows, Joined.with(null, null, null));
    }

    @Override
    public <VO, VR> KStream<K, VR> join(final KStream<K, VO> otherStream,
                                        final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                        final JoinWindows windows,
                                        final Joined<K, V, VO> joined) {

        return doJoin(otherStream,
                      joiner,
                      windows,
                      joined,
                      new KStreamImplJoin(false, false));

    }

    @Override
    public <VO, VR> KStream<K, VR> outerJoin(final KStream<K, VO> other,
                                             final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                             final JoinWindows windows) {
        return outerJoin(other, joiner, windows, Joined.with(null, null, null));
    }

    @Override
    public <VO, VR> KStream<K, VR> outerJoin(final KStream<K, VO> other,
                                             final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                             final JoinWindows windows,
                                             final Joined<K, V, VO> joined) {
        return doJoin(other, joiner, windows, joined, new KStreamImplJoin(true, true));
    }

    private <VO, VR> KStream<K, VR> doJoin(final KStream<K, VO> other,
                                           final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                           final JoinWindows windows,
                                           final Joined<K, V, VO> joined,
                                           final KStreamImplJoin join) {
        Objects.requireNonNull(other, "other KStream can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(windows, "windows can't be null");
        Objects.requireNonNull(joined, "joined can't be null");

        KStreamImpl<K, V> joinThis = this;
        KStreamImpl<K, VO> joinOther = (KStreamImpl<K, VO>) other;

        final JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<>(joined);
        final String name = joinedInternal.name();
        if (joinThis.repartitionRequired) {
            final String leftJoinRepartitionTopicName = name != null ? name + "-left" : joinThis.name;
            joinThis = joinThis.repartitionForJoin(leftJoinRepartitionTopicName, joined.keySerde(), joined.valueSerde());
        }

        if (joinOther.repartitionRequired) {
            final String rightJoinRepartitionTopicName = name != null ? name + "-right" : joinOther.name;
            joinOther = joinOther.repartitionForJoin(rightJoinRepartitionTopicName, joined.keySerde(), joined.otherValueSerde());
        }

        joinThis.ensureJoinableWith(joinOther);

        return join.join(
            joinThis,
            joinOther,
            joiner,
            windows,
            joined
        );
    }

    /**
     * Repartition a stream. This is required on join operations occurring after
     * an operation that changes the key, i.e, selectKey, map(..), flatMap(..).
     */
    private KStreamImpl<K, V> repartitionForJoin(final String repartitionName,
                                                 final Serde<K> keySerdeOverride,
                                                 final Serde<V> valueSerdeOverride) {
        final Serde<K> repartitionKeySerde = keySerdeOverride != null ? keySerdeOverride : keySerde;
        final Serde<V> repartitionValueSerde = valueSerdeOverride != null ? valueSerdeOverride : valSerde;
        final OptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder =
            OptimizableRepartitionNode.optimizableRepartitionNodeBuilder();
        final String repartitionedSourceName = createRepartitionedSource(builder,
                                                                         repartitionKeySerde,
                                                                         repartitionValueSerde,
                                                                         repartitionName,
                                                                         optimizableRepartitionNodeBuilder);

        final OptimizableRepartitionNode<K, V> optimizableRepartitionNode = optimizableRepartitionNodeBuilder.build();
        builder.addGraphNode(this.streamsGraphNode, optimizableRepartitionNode);

        return new KStreamImpl<>(repartitionedSourceName, repartitionKeySerde, repartitionValueSerde, Collections.singleton(repartitionedSourceName), false, optimizableRepartitionNode, builder);
    }

    static <K1, V1> String createRepartitionedSource(final InternalStreamsBuilder builder,
                                                     final Serde<K1> keySerde,
                                                     final Serde<V1> valSerde,
                                                     final String repartitionTopicNamePrefix,
                                                     final OptimizableRepartitionNodeBuilder<K1, V1> optimizableRepartitionNodeBuilder) {


        final String repartitionTopic = repartitionTopicNamePrefix + REPARTITION_TOPIC_SUFFIX;
        final String sinkName = builder.newProcessorName(SINK_NAME);
        final String nullKeyFilterProcessorName = builder.newProcessorName(FILTER_NAME);
        final String sourceName = builder.newProcessorName(SOURCE_NAME);

        final Predicate<K1, V1> notNullKeyPredicate = (k, v) -> k != null;

        final ProcessorParameters processorParameters = new ProcessorParameters<>(
            new KStreamFilter<>(notNullKeyPredicate, false),
            nullKeyFilterProcessorName
        );

        optimizableRepartitionNodeBuilder.withKeySerde(keySerde)
                                         .withValueSerde(valSerde)
                                         .withSourceName(sourceName)
                                         .withRepartitionTopic(repartitionTopic)
                                         .withSinkName(sinkName)
                                         .withProcessorParameters(processorParameters)
                                         // reusing the source name for the graph node name
                                         // adding explicit variable as it simplifies logic
                                         .withNodeName(sourceName);

        return sourceName;
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(final KStream<K, VO> other,
                                            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                            final JoinWindows windows) {
        return leftJoin(other, joiner, windows, Joined.with(null, null, null));
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(final KStream<K, VO> other,
                                            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                            final JoinWindows windows,
                                            final Joined<K, V, VO> joined) {
        Objects.requireNonNull(joined, "joined can't be null");
        return doJoin(
            other,
            joiner,
            windows,
            joined,
            new KStreamImplJoin(true, false)
        );

    }

    @Override
    public <VO, VR> KStream<K, VR> join(final KTable<K, VO> other,
                                        final ValueJoiner<? super V, ? super VO, ? extends VR> joiner) {
        return join(other, joiner, Joined.with(null, null, null));
    }

    @Override
    public <VO, VR> KStream<K, VR> join(final KTable<K, VO> other,
                                        final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                        final Joined<K, V, VO> joined) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(joined, "joined can't be null");

        final JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<>(joined);
        final String name = joinedInternal.name();
        if (repartitionRequired) {
            final KStreamImpl<K, V> thisStreamRepartitioned = repartitionForJoin(
                name != null ? name : this.name,
                joined.keySerde(),
                joined.valueSerde()
            );
            return thisStreamRepartitioned.doStreamTableJoin(other, joiner, joined, false);
        } else {
            return doStreamTableJoin(other, joiner, joined, false);
        }
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(final KTable<K, VO> other, final ValueJoiner<? super V, ? super VO, ? extends VR> joiner) {
        return leftJoin(other, joiner, Joined.with(null, null, null));
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(final KTable<K, VO> other,
                                            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                            final Joined<K, V, VO> joined) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(joined, "joined can't be null");
        final JoinedInternal<K, V, VO> joinedInternal = new JoinedInternal<>(joined);
        final String internalName = joinedInternal.name();
        if (repartitionRequired) {
            final KStreamImpl<K, V> thisStreamRepartitioned = repartitionForJoin(
                internalName != null ? internalName : name,
                joined.keySerde(),
                joined.valueSerde()
            );
            return thisStreamRepartitioned.doStreamTableJoin(other, joiner, joined, true);
        } else {
            return doStreamTableJoin(other, joiner, joined, true);
        }
    }

    @Override
    public <KG, VG, VR> KStream<K, VR> join(final GlobalKTable<KG, VG> globalTable,
                                            final KeyValueMapper<? super K, ? super V, ? extends KG> keyMapper,
                                            final ValueJoiner<? super V, ? super VG, ? extends VR> joiner) {
        return globalTableJoin(globalTable, keyMapper, joiner, false);
    }

    @Override
    public <KG, VG, VR> KStream<K, VR> leftJoin(final GlobalKTable<KG, VG> globalTable,
                                                final KeyValueMapper<? super K, ? super V, ? extends KG> keyMapper,
                                                final ValueJoiner<? super V, ? super VG, ? extends VR> joiner) {
        return globalTableJoin(globalTable, keyMapper, joiner, true);
    }

    private <KG, VG, VR> KStream<K, VR> globalTableJoin(final GlobalKTable<KG, VG> globalTable,
                                                        final KeyValueMapper<? super K, ? super V, ? extends KG> keyMapper,
                                                        final ValueJoiner<? super V, ? super VG, ? extends VR> joiner,
                                                        final boolean leftJoin) {
        Objects.requireNonNull(globalTable, "globalTable can't be null");
        Objects.requireNonNull(keyMapper, "keyMapper can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

        final KTableValueGetterSupplier<KG, VG> valueGetterSupplier = ((GlobalKTableImpl<KG, VG>) globalTable).valueGetterSupplier();
        final String name = builder.newProcessorName(LEFTJOIN_NAME);

        final ProcessorSupplier<K, V> processorSupplier = new KStreamGlobalKTableJoin<>(
            valueGetterSupplier,
            joiner,
            keyMapper,
            leftJoin
        );
        final ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(processorSupplier, name);

        final StreamTableJoinNode<K, V> streamTableJoinNode = new StreamTableJoinNode<>(name,
                                                                                        processorParameters,
                                                                                        new String[] {},
                                                                                        null);
        builder.addGraphNode(this.streamsGraphNode, streamTableJoinNode);

        // do not have serde for joined result
        return new KStreamImpl<>(name, keySerde, null, sourceNodes, repartitionRequired, streamTableJoinNode, builder);
    }

    @SuppressWarnings("unchecked")
    private <VO, VR> KStream<K, VR> doStreamTableJoin(final KTable<K, VO> other,
                                                      final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                                      final Joined<K, V, VO> joined,
                                                      final boolean leftJoin) {
        Objects.requireNonNull(other, "other KTable can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

        final Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K, VO>) other);

        final String name = builder.newProcessorName(leftJoin ? LEFTJOIN_NAME : JOIN_NAME);
        final ProcessorSupplier<K, V> processorSupplier = new KStreamKTableJoin<>(
            ((KTableImpl<K, ?, VO>) other).valueGetterSupplier(),
            joiner,
            leftJoin
        );

        final ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(processorSupplier, name);
        final StreamTableJoinNode<K, V> streamTableJoinNode = new StreamTableJoinNode<>(
            name,
            processorParameters,
            ((KTableImpl) other).valueGetterSupplier().storeNames(),
            this.name
        );

        builder.addGraphNode(this.streamsGraphNode, streamTableJoinNode);

        // do not have serde for joined result
        return new KStreamImpl<>(name, joined.keySerde() != null ? joined.keySerde() : keySerde, null, allSourceNodes, false, streamTableJoinNode, builder);

    }

    @Override
    public <KR> KGroupedStream<KR, V> groupBy(final KeyValueMapper<? super K, ? super V, KR> selector) {
        return groupBy(selector, Grouped.with(null, valSerde));
    }

    @Override
    @Deprecated
    public <KR> KGroupedStream<KR, V> groupBy(final KeyValueMapper<? super K, ? super V, KR> selector,
                                              final org.apache.kafka.streams.kstream.Serialized<KR, V> serialized) {
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(serialized, "serialized can't be null");
        final SerializedInternal<KR, V> serializedInternal = new SerializedInternal<>(serialized);

        return groupBy(selector, Grouped.with(serializedInternal.keySerde(), serializedInternal.valueSerde()));
    }

    @Override
    public <KR> KGroupedStream<KR, V> groupBy(final KeyValueMapper<? super K, ? super V, KR> selector,
                                              final Grouped<KR, V> grouped) {
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(grouped, "grouped can't be null");
        final GroupedInternal<KR, V> groupedInternal = new GroupedInternal<>(grouped);
        final ProcessorGraphNode<K, V> selectKeyMapNode = internalSelectKey(selector, new NamedInternal(groupedInternal.name()));
        selectKeyMapNode.keyChangingOperation(true);

        builder.addGraphNode(this.streamsGraphNode, selectKeyMapNode);

        return new KGroupedStreamImpl<>(
            selectKeyMapNode.nodeName(),
            sourceNodes,
            groupedInternal,
            true,
            selectKeyMapNode,
            builder);
    }

    @Override
    public KGroupedStream<K, V> groupByKey() {
        return groupByKey(Grouped.with(keySerde, valSerde));
    }

    @Override
    @Deprecated
    public KGroupedStream<K, V> groupByKey(final org.apache.kafka.streams.kstream.Serialized<K, V> serialized) {
        final SerializedInternal<K, V> serializedInternal = new SerializedInternal<>(serialized);
        return groupByKey(Grouped.with(serializedInternal.keySerde(), serializedInternal.valueSerde()));
    }

    @Override
    public KGroupedStream<K, V> groupByKey(final Grouped<K, V> grouped) {
        final GroupedInternal<K, V> groupedInternal = new GroupedInternal<>(grouped);

        return new KGroupedStreamImpl<>(
            name,
            sourceNodes,
            groupedInternal,
            repartitionRequired,
            streamsGraphNode,
            builder);
    }

    @SuppressWarnings("deprecation") // continuing to support Windows#maintainMs/segmentInterval in fallback mode
    private static <K, V> StoreBuilder<WindowStore<K, V>> joinWindowStoreBuilder(final String joinName,
                                                                                 final JoinWindows windows,
                                                                                 final Serde<K> keySerde,
                                                                                 final Serde<V> valueSerde) {
        return Stores.windowStoreBuilder(
            Stores.persistentWindowStore(
                joinName + "-store",
                Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                Duration.ofMillis(windows.size()),
                true
            ),
            keySerde,
            valueSerde
        );
    }

    private class KStreamImplJoin {

        private final boolean leftOuter;
        private final boolean rightOuter;


        KStreamImplJoin(final boolean leftOuter,
                        final boolean rightOuter) {
            this.leftOuter = leftOuter;
            this.rightOuter = rightOuter;
        }

        public <K1, R, V1, V2> KStream<K1, R> join(final KStream<K1, V1> lhs,
                                                   final KStream<K1, V2> other,
                                                   final ValueJoiner<? super V1, ? super V2, ? extends R> joiner,
                                                   final JoinWindows windows,
                                                   final Joined<K1, V1, V2> joined) {
            final String thisWindowStreamName = builder.newProcessorName(WINDOWED_NAME);
            final String otherWindowStreamName = builder.newProcessorName(WINDOWED_NAME);
            final String joinThisName = rightOuter ? builder.newProcessorName(OUTERTHIS_NAME) : builder.newProcessorName(JOINTHIS_NAME);
            final String joinOtherName = leftOuter ? builder.newProcessorName(OUTEROTHER_NAME) : builder.newProcessorName(JOINOTHER_NAME);
            final String joinMergeName = builder.newProcessorName(MERGE_NAME);

            final StreamsGraphNode thisStreamsGraphNode = ((AbstractStream) lhs).streamsGraphNode;
            final StreamsGraphNode otherStreamsGraphNode = ((AbstractStream) other).streamsGraphNode;


            final StoreBuilder<WindowStore<K1, V1>> thisWindowStore =
                joinWindowStoreBuilder(joinThisName, windows, joined.keySerde(), joined.valueSerde());
            final StoreBuilder<WindowStore<K1, V2>> otherWindowStore =
                joinWindowStoreBuilder(joinOtherName, windows, joined.keySerde(), joined.otherValueSerde());

            final KStreamJoinWindow<K1, V1> thisWindowedStream = new KStreamJoinWindow<>(thisWindowStore.name());

            final ProcessorParameters<K1, V1> thisWindowStreamProcessorParams = new ProcessorParameters<>(thisWindowedStream, thisWindowStreamName);
            final ProcessorGraphNode<K1, V1> thisWindowedStreamsNode = new ProcessorGraphNode<>(thisWindowStreamName, thisWindowStreamProcessorParams);
            builder.addGraphNode(thisStreamsGraphNode, thisWindowedStreamsNode);

            final KStreamJoinWindow<K1, V2> otherWindowedStream = new KStreamJoinWindow<>(otherWindowStore.name());

            final ProcessorParameters<K1, V2> otherWindowStreamProcessorParams = new ProcessorParameters<>(otherWindowedStream, otherWindowStreamName);
            final ProcessorGraphNode<K1, V2> otherWindowedStreamsNode = new ProcessorGraphNode<>(otherWindowStreamName, otherWindowStreamProcessorParams);
            builder.addGraphNode(otherStreamsGraphNode, otherWindowedStreamsNode);

            final KStreamKStreamJoin<K1, R, V1, V2> joinThis = new KStreamKStreamJoin<>(
                otherWindowStore.name(),
                windows.beforeMs,
                windows.afterMs,
                joiner,
                leftOuter
            );

            final KStreamKStreamJoin<K1, R, V2, V1> joinOther = new KStreamKStreamJoin<>(
                thisWindowStore.name(),
                windows.afterMs,
                windows.beforeMs,
                reverseJoiner(joiner),
                rightOuter
            );

            final KStreamPassThrough<K1, R> joinMerge = new KStreamPassThrough<>();

            final StreamStreamJoinNode.StreamStreamJoinNodeBuilder<K1, V1, V2, R> joinBuilder = StreamStreamJoinNode.streamStreamJoinNodeBuilder();

            final ProcessorParameters<K1, V1> joinThisProcessorParams = new ProcessorParameters<>(joinThis, joinThisName);
            final ProcessorParameters<K1, V2> joinOtherProcessorParams = new ProcessorParameters<>(joinOther, joinOtherName);
            final ProcessorParameters<K1, R> joinMergeProcessorParams = new ProcessorParameters<>(joinMerge, joinMergeName);

            joinBuilder.withJoinMergeProcessorParameters(joinMergeProcessorParams)
                       .withJoinThisProcessorParameters(joinThisProcessorParams)
                       .withJoinOtherProcessorParameters(joinOtherProcessorParams)
                       .withThisWindowStoreBuilder(thisWindowStore)
                       .withOtherWindowStoreBuilder(otherWindowStore)
                       .withThisWindowedStreamProcessorParameters(thisWindowStreamProcessorParams)
                       .withOtherWindowedStreamProcessorParameters(otherWindowStreamProcessorParams)
                       .withValueJoiner(joiner)
                       .withNodeName(joinMergeName);

            final StreamsGraphNode joinGraphNode = joinBuilder.build();

            builder.addGraphNode(Arrays.asList(thisStreamsGraphNode, otherStreamsGraphNode), joinGraphNode);

            final Set<String> allSourceNodes = new HashSet<>(((KStreamImpl<K1, V1>) lhs).sourceNodes);
            allSourceNodes.addAll(((KStreamImpl<K1, V2>) other).sourceNodes);

            // do not have serde for joined result;
            // also for key serde we do not inherit from either since we cannot tell if these two serdes are different
            return new KStreamImpl<>(joinMergeName, joined.keySerde(), null, allSourceNodes, false, joinGraphNode, builder);
        }
    }

}
