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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StatelessProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamSinkNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamStreamJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamTableJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.internals.StaticTopicNameExtractor;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class KStreamImpl<K, V> extends AbstractStream<K> implements KStream<K, V> {

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

    public KStreamImpl(final InternalStreamsBuilder builder,
                       final String name,
                       final Set<String> sourceNodes,
                       final boolean repartitionRequired,
                       final StreamsGraphNode streamsGraphNode) {
        super(builder, name, sourceNodes, streamsGraphNode);
        this.repartitionRequired = repartitionRequired;
    }

    @Override
    public KStream<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        String name = builder.newProcessorName(FILTER_NAME);

        ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamFilter<>(predicate, false), name);
        StatelessProcessorNode<K, V> filterProcessorNode = new StatelessProcessorNode<>(name,
                                                                                        processorParameters,
                                                                                        repartitionRequired);
        addGraphNode(filterProcessorNode);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamFilter<>(predicate, false), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, this.repartitionRequired, filterProcessorNode);
    }

    @Override
    public KStream<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        String name = builder.newProcessorName(FILTER_NAME);

        ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamFilter<>(predicate, true), name);

        StatelessProcessorNode<K, V> filterNotProcessorNode = new StatelessProcessorNode<>(name,
                                                                                           processorParameters,
                                                                                           repartitionRequired);

        addGraphNode(filterNotProcessorNode);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamFilter<>(predicate, true), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, this.repartitionRequired, filterNotProcessorNode);
    }

    @Override
    public <K1> KStream<K1, V> selectKey(final KeyValueMapper<? super K, ? super V, ? extends K1> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");

        StatelessProcessorNode<K, V> selectKeyProcessorNode = internalSelectKey(mapper);
        selectKeyProcessorNode.keyChangingOperation(true);
        addGraphNode(selectKeyProcessorNode);
        return new KStreamImpl<>(builder, selectKeyProcessorNode.nodeName(), sourceNodes, true, selectKeyProcessorNode);
    }

    private <K1> StatelessProcessorNode<K, V> internalSelectKey(final KeyValueMapper<? super K, ? super V, ? extends K1> mapper) {
        String name = builder.newProcessorName(KEY_SELECT_NAME);


        KStreamMap kStreamMap = new KStreamMap<>(
            (KeyValueMapper<K, V, KeyValue<K1, V>>) (key, value) -> new KeyValue<>(mapper.apply(key, value), value));


        ProcessorParameters<K1, V> processorParameters = new ProcessorParameters<>(kStreamMap, name);

        builder.internalTopologyBuilder.addProcessor(name, kStreamMap, this.name);

        return  new StatelessProcessorNode<>(name,
                                             processorParameters,
                                             repartitionRequired);

    }

    @Override
    public <K1, V1> KStream<K1, V1> map(final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends K1, ? extends V1>> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        String name = builder.newProcessorName(MAP_NAME);

        ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamMap<>(mapper), name);

        StatelessProcessorNode<K1, V1> mapProcessorNode = new StatelessProcessorNode<>(name,
                                                                                       processorParameters,
                                                                                       true);
        mapProcessorNode.keyChangingOperation(true);
        addGraphNode(mapProcessorNode);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamMap<>(mapper), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, true, mapProcessorNode);
    }


    @Override
    public <V1> KStream<K, V1> mapValues(final ValueMapper<? super V, ? extends V1> mapper) {
        return mapValues(withKey(mapper));
    }

    @Override
    public <VR> KStream<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        String name = builder.newProcessorName(MAPVALUES_NAME);

        ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamMapValues<>(mapper), name);

        StatelessProcessorNode<K, V> mapValuesProcessorNode = new StatelessProcessorNode<>(name,
                                                                                           processorParameters,
                                                                                           repartitionRequired);
        addGraphNode(mapValuesProcessorNode);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamMapValues<>(mapper), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, this.repartitionRequired, mapValuesProcessorNode);
    }

    @Override
    public void print(final Printed<K, V> printed) {
        Objects.requireNonNull(printed, "printed can't be null");
        final PrintedInternal<K, V> printedInternal = new PrintedInternal<>(printed);
        final String name = builder.newProcessorName(PRINTING_NAME);

        ProcessorParameters processorParameters = new ProcessorParameters<>(printedInternal.build(this.name), name);

        StatelessProcessorNode<K, V> printNode = new StatelessProcessorNode<>(name,
                                                                              processorParameters,
                                                                              false);
        addGraphNode(printNode);
        builder.internalTopologyBuilder.addProcessor(name, printedInternal.build(this.name), this.name);
    }

    @Override
    public <K1, V1> KStream<K1, V1> flatMap(
        final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends K1, ? extends V1>>> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        String name = builder.newProcessorName(FLATMAP_NAME);

        ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamFlatMap<>(mapper), name);

        StatelessProcessorNode<K1, V1> flatMapNode = new StatelessProcessorNode<>(name,
                                                                                  processorParameters,
                                                                                  true);
        flatMapNode.keyChangingOperation(true);

        addGraphNode(flatMapNode);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamFlatMap<>(mapper), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, true, flatMapNode);
    }

    @Override
    public <V1> KStream<K, V1> flatMapValues(final ValueMapper<? super V, ? extends Iterable<? extends V1>> mapper) {
        return flatMapValues(withKey(mapper));
    }

    @Override
    public <VR> KStream<K, VR> flatMapValues(final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        String name = builder.newProcessorName(FLATMAPVALUES_NAME);

        ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamFlatMapValues<>(mapper), name);

        StatelessProcessorNode<K, VR> flatMapValuesNode = new StatelessProcessorNode<>(name,
                                                                                       processorParameters,
                                                                                       repartitionRequired);
        addGraphNode(flatMapValuesNode);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamFlatMapValues<>(mapper), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, this.repartitionRequired, flatMapValuesNode);
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

        String branchName = builder.newProcessorName(BRANCH_NAME);

        String[] childNames = new String[predicates.length];
        for (int i = 0; i < predicates.length; i++) {
            childNames[i] = builder.newProcessorName(BRANCHCHILD_NAME);
        }

        builder.internalTopologyBuilder.addProcessor(branchName, new KStreamBranch(predicates.clone(), childNames), this.name);

        ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamBranch(predicates.clone(), childNames), branchName);

        StatelessProcessorNode<K, V> branchNode = new StatelessProcessorNode<>(branchName,
                                                                               processorParameters,
                                                                               false);
        addGraphNode(branchNode);

        KStream<K, V>[] branchChildren = (KStream<K, V>[]) Array.newInstance(KStream.class, predicates.length);

        for (int i = 0; i < predicates.length; i++) {
            ProcessorParameters innerProcessorParameters = new ProcessorParameters<>(new KStreamPassThrough<K, V>(), childNames[i]);

            StatelessProcessorNode<K, V> branchChildNode = new StatelessProcessorNode<>(childNames[i],
                                                                                        innerProcessorParameters,
                                                                                        repartitionRequired);
            branchNode.addChildNode(branchChildNode);
            builder.internalTopologyBuilder.addProcessor(childNames[i], new KStreamPassThrough<K, V>(), branchName);
            branchChildren[i] = new KStreamImpl<>(builder, childNames[i], sourceNodes, this.repartitionRequired, branchChildNode);
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
        KStreamImpl<K, V> streamImpl = (KStreamImpl<K, V>) stream;
        String name = builder.newProcessorName(MERGE_NAME);
        String[] parentNames = {this.name, streamImpl.name};
        Set<String> allSourceNodes = new HashSet<>();

        boolean requireRepartitioning = streamImpl.repartitionRequired || repartitionRequired;
        allSourceNodes.addAll(sourceNodes);
        allSourceNodes.addAll(streamImpl.sourceNodes);

        ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamPassThrough<>(), name);

        StatelessProcessorNode<K, V> mergeNode = new StatelessProcessorNode<>(name,
                                                                              processorParameters,
                                                                              requireRepartitioning,
                                                                              Arrays.asList(parentNames));

        addGraphNode(mergeNode);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamPassThrough<>(), parentNames);

        return new KStreamImpl<>(builder, name, allSourceNodes, requireRepartitioning, mergeNode);
    }

    @Override
    public KStream<K, V> through(final String topic, final Produced<K, V> produced) {
        final ProducedInternal<K, V> producedInternal = new ProducedInternal<>(produced);
        to(topic, producedInternal);
        return builder.stream(Collections.singleton(topic),
                              new ConsumedInternal<>(producedInternal.keySerde(),
                                                     producedInternal.valueSerde(),
                                                     new FailOnInvalidTimestamp(),
                                                     null));
    }

    @Override
    public void foreach(final ForeachAction<? super K, ? super V> action) {
        Objects.requireNonNull(action, "action can't be null");
        String name = builder.newProcessorName(FOREACH_NAME);

        ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamPeek<>(action, false), name);

        StatelessProcessorNode<K, V> foreachNode = new StatelessProcessorNode<>(name,
                                                                                processorParameters,
                                                                                repartitionRequired);
        addGraphNode(foreachNode);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamPeek<>(action, false), this.name);
    }

    @Override
    public KStream<K, V> peek(final ForeachAction<? super K, ? super V> action) {
        Objects.requireNonNull(action, "action can't be null");
        final String name = builder.newProcessorName(PEEK_NAME);

        ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamPeek<>(action, true), name);

        StatelessProcessorNode<K, V> peekNode = new StatelessProcessorNode<>(name,
                                                                             processorParameters,
                                                                             repartitionRequired);
        addGraphNode(peekNode);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamPeek<>(action, true), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, repartitionRequired, peekNode);
    }

    @Override
    public KStream<K, V> through(final String topic) {
        return through(topic, Produced.<K, V>with(null, null, null));
    }

    @Override
    public void to(final String topic) {
        to(topic, Produced.<K, V>with(null, null, null));
    }

    @Override
    public void to(final String topic, final Produced<K, V> produced) {
        Objects.requireNonNull(topic, "topic can't be null");
        Objects.requireNonNull(produced, "Produced can't be null");
        to(new StaticTopicNameExtractor<K, V>(topic), new ProducedInternal<>(produced));
    }

    @Override
    public void to(final TopicNameExtractor<K, V> topicExtractor) {
        to(topicExtractor, Produced.<K, V>with(null, null, null));
    }

    @Override
    public void to(final TopicNameExtractor<K, V> topicExtractor, final Produced<K, V> produced) {
        Objects.requireNonNull(topicExtractor, "topic extractor can't be null");
        Objects.requireNonNull(produced, "Produced can't be null");
        to(topicExtractor, new ProducedInternal<>(produced));
    }

    @SuppressWarnings("unchecked")
    private void to(final TopicNameExtractor<K, V> topicExtractor, final ProducedInternal<K, V> produced) {
        final String name = builder.newProcessorName(SINK_NAME);
        final Serializer<K> keySerializer = produced.keySerde() == null ? null : produced.keySerde().serializer();
        final Serializer<V> valSerializer = produced.valueSerde() == null ? null : produced.valueSerde().serializer();
        final StreamPartitioner<? super K, ? super V> partitioner = produced.streamPartitioner();

        if (partitioner == null && keySerializer instanceof WindowedSerializer) {
            final StreamPartitioner<K, V> windowedPartitioner = (StreamPartitioner<K, V>) new WindowedStreamPartitioner<Object, V>((WindowedSerializer) keySerializer);
            builder.internalTopologyBuilder.addSink(name, topicExtractor, keySerializer, valSerializer, windowedPartitioner, this.name);
        } else {
            builder.internalTopologyBuilder.addSink(name, topicExtractor, keySerializer, valSerializer, partitioner, this.name);
        }

        StreamSinkNode<K, V> sinkNode = new StreamSinkNode<>(
            name,
            topicExtractor,
            produced);
        addGraphNode(sinkNode);
    }

    @Override
    public <K1, V1> KStream<K1, V1> transform(final TransformerSupplier<? super K, ? super V, KeyValue<K1, V1>> transformerSupplier,
                                              final String... stateStoreNames) {
        Objects.requireNonNull(transformerSupplier, "transformerSupplier can't be null");
        String name = builder.newProcessorName(TRANSFORM_NAME);

        ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamTransform<>(transformerSupplier), name);

        StatefulProcessorNode<K1, V1> transformNode = new StatefulProcessorNode<>(name,
                                                                                  processorParameters,
                                                                                  stateStoreNames,
                                                                                  null,
                                                                                  null,
                                                                                  true);
        transformNode.keyChangingOperation(true);
        addGraphNode(transformNode);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamTransform<>(transformerSupplier), this.name);
        if (stateStoreNames != null && stateStoreNames.length > 0) {
            builder.internalTopologyBuilder.connectProcessorAndStateStores(name, stateStoreNames);
        }

        return new KStreamImpl<>(builder, name, sourceNodes, true, null);
    }

    @Override
    public <V1> KStream<K, V1> transformValues(final ValueTransformerSupplier<? super V, ? extends V1> valueTransformerSupplier,
                                               final String... stateStoreNames) {
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformSupplier can't be null");

        return doTransformValues(toValueTransformerWithKeySupplier(valueTransformerSupplier), stateStoreNames);
    }

    @Override
    public <VR> KStream<K, VR> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> valueTransformerSupplier,
                                               final String... stateStoreNames) {
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformSupplier can't be null");

        return doTransformValues(valueTransformerSupplier, stateStoreNames);
    }

    private <VR> KStream<K, VR> doTransformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> valueTransformerWithKeySupplier,
                                                  final String... stateStoreNames) {
        final String name = builder.newProcessorName(TRANSFORMVALUES_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamTransformValues<>(valueTransformerWithKeySupplier), this.name);
        if (stateStoreNames != null && stateStoreNames.length > 0) {
            builder.internalTopologyBuilder.connectProcessorAndStateStores(name, stateStoreNames);
        }




        ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamTransformValues<>(valueTransformerWithKeySupplier), name);

        StatefulProcessorNode<K, VR> transformNode = new StatefulProcessorNode<>(name,
                                                                                 processorParameters,
                                                                                 stateStoreNames,
                                                                                 null,
                                                                                 null,
                                                                                 repartitionRequired);
        addGraphNode(transformNode);

        return new KStreamImpl<>(builder, name, sourceNodes, this.repartitionRequired, transformNode);
    }

    @Override
    public void process(final ProcessorSupplier<? super K, ? super V> processorSupplier,
                        final String... stateStoreNames) {
        final String name = builder.newProcessorName(PROCESSOR_NAME);

        ProcessorParameters processorParameters = new ProcessorParameters<>(processorSupplier, name);

        StatefulProcessorNode<K, V> transformNode = new StatefulProcessorNode<>(name,
                                                                                processorParameters,
                                                                                stateStoreNames,
                                                                                null,
                                                                                null,
                                                                                false);
        addGraphNode(transformNode);

        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);
        if (stateStoreNames != null && stateStoreNames.length > 0) {
            builder.internalTopologyBuilder.connectProcessorAndStateStores(name, stateStoreNames);
        }
    }

    @Override
    public <V1, R> KStream<K, R> join(final KStream<K, V1> other,
                                      final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                      final JoinWindows windows) {
        return join(other, joiner, windows, Joined.<K, V, V1>with(null, null, null));
    }

    @Override
    public <VO, VR> KStream<K, VR> join(final KStream<K, VO> otherStream,
                                        final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                        final JoinWindows windows,
                                        final Joined<K, V, VO> joined) {
        return doJoin(otherStream, joiner, windows, joined,
                      new KStreamImplJoin(false, false));
    }

    @Override
    public <V1, R> KStream<K, R> outerJoin(final KStream<K, V1> other,
                                           final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                           final JoinWindows windows) {
        return outerJoin(other, joiner, windows, Joined.<K, V, V1>with(null, null, null));
    }

    @Override
    public <VO, VR> KStream<K, VR> outerJoin(final KStream<K, VO> other,
                                             final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                             final JoinWindows windows,
                                             final Joined<K, V, VO> joined) {
        return doJoin(other, joiner, windows, joined, new KStreamImplJoin(true, true));
    }

    private <V1, R> KStream<K, R> doJoin(final KStream<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                         final JoinWindows windows,
                                         final Joined<K, V, V1> joined,
                                         final KStreamImplJoin join) {
        Objects.requireNonNull(other, "other KStream can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(windows, "windows can't be null");
        Objects.requireNonNull(joined, "joined can't be null");

        KStreamImpl<K, V> joinThis = this;
        KStreamImpl<K, V1> joinOther = (KStreamImpl<K, V1>) other;

        if (joinThis.repartitionRequired) {
            joinThis = joinThis.repartitionForJoin(joined.keySerde(), joined.valueSerde());
        }

        if (joinOther.repartitionRequired) {
            joinOther = joinOther.repartitionForJoin(joined.keySerde(), joined.otherValueSerde());
        }

        joinThis.ensureJoinableWith(joinOther);

        return join.join(joinThis,
                         joinOther,
                         joiner,
                         windows,
                         joined);
    }

    /**
     * Repartition a stream. This is required on join operations occurring after
     * an operation that changes the key, i.e, selectKey, map(..), flatMap(..).
     *
     * @param keySerde Serdes for serializing the keys
     * @param valSerde Serdes for serilaizing the values
     * @return a new {@link KStreamImpl}
     */
    private KStreamImpl<K, V> repartitionForJoin(final Serde<K> keySerde,
                                                 final Serde<V> valSerde) {
        OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder repartitionNodeBuilder = OptimizableRepartitionNode.optimizableRepartitionNodeBuilder();
        String repartitionedSourceName = createRepartitionedSource(builder,
                                                                   keySerde,
                                                                   valSerde,
                                                                   null,
                                                                   name,
                                                                   repartitionNodeBuilder);

        OptimizableRepartitionNode<K, V> repartitionNode = repartitionNodeBuilder.build();
        addGraphNode(repartitionNode);

        return new KStreamImpl<>(builder, repartitionedSourceName, Collections
            .singleton(repartitionedSourceName), false, repartitionNode);
    }

    static <K1, V1> String createRepartitionedSource(final InternalStreamsBuilder builder,
                                                     final Serde<K1> keySerde,
                                                     final Serde<V1> valSerde,
                                                     final String topicNamePrefix,
                                                     final String name,
                                                     final OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder repartitionNodeBuilder) {

        Serializer<K1> keySerializer = keySerde != null ? keySerde.serializer() : null;
        Serializer<V1> valSerializer = valSerde != null ? valSerde.serializer() : null;
        Deserializer<K1> keyDeserializer = keySerde != null ? keySerde.deserializer() : null;
        Deserializer<V1> valDeserializer = valSerde != null ? valSerde.deserializer() : null;
        String baseName = topicNamePrefix != null ? topicNamePrefix : name;

        String repartitionTopic = baseName + REPARTITION_TOPIC_SUFFIX;
        String sinkName = builder.newProcessorName(SINK_NAME);
        String nullKeyFilterProcessorName = builder.newProcessorName(FILTER_NAME);
        String sourceName = builder.newProcessorName(SOURCE_NAME);

        Predicate<K1, V1> nullKeyPredicate = (k, v) -> k != null;

        ProcessorParameters processorParameters = new ProcessorParameters<>(new KStreamFilter<>(nullKeyPredicate, false), nullKeyFilterProcessorName);

        repartitionNodeBuilder.withKeySerde(keySerde)
            .withValueSerde(valSerde)
            .withSourceName(sourceName)
            .withRepartitionTopic(repartitionTopic)
            .withSinkName(sinkName)
            .withProcessorParameters(processorParameters)
            // reusing the source name for the graph node name
            // adding explicit variable as it simplifies logic
            .withNodeName(sourceName);

        builder.internalTopologyBuilder.addInternalTopic(repartitionTopic);
        builder.internalTopologyBuilder.addProcessor(nullKeyFilterProcessorName, new KStreamFilter<>(nullKeyPredicate, false), name);

        builder.internalTopologyBuilder.addSink(sinkName, repartitionTopic, keySerializer, valSerializer,
                                                null, nullKeyFilterProcessorName);
        builder.internalTopologyBuilder.addSource(null, sourceName, new FailOnInvalidTimestamp(),
                                                  keyDeserializer, valDeserializer, repartitionTopic);

        return sourceName;
    }

    @Override
    public <V1, R> KStream<K, R> leftJoin(final KStream<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                          final JoinWindows windows) {
        return leftJoin(other, joiner, windows, Joined.<K, V, V1>with(null, null, null));
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(final KStream<K, VO> other,
                                            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                            final JoinWindows windows,
                                            final Joined<K, V, VO> joined) {
        Objects.requireNonNull(joined, "joined can't be null");
        return doJoin(other,
                      joiner,
                      windows,
                      joined,
                      new KStreamImplJoin(true, false));
    }

    @Override
    public <V1, R> KStream<K, R> join(final KTable<K, V1> other,
                                      final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return join(other, joiner, Joined.<K, V, V1>with(null, null, null));
    }

    @Override
    public <VT, VR> KStream<K, VR> join(final KTable<K, VT> other,
                                        final ValueJoiner<? super V, ? super VT, ? extends VR> joiner,
                                        final Joined<K, V, VT> joined) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(joined, "joined can't be null");
        if (repartitionRequired) {
            final KStreamImpl<K, V> thisStreamRepartitioned = repartitionForJoin(joined.keySerde(), joined.valueSerde());
            return thisStreamRepartitioned.doStreamTableJoin(other, joiner, false);
        } else {
            return doStreamTableJoin(other, joiner, false);
        }
    }

    @Override
    public <K1, V1, R> KStream<K, R> leftJoin(final GlobalKTable<K1, V1> globalTable,
                                              final KeyValueMapper<? super K, ? super V, ? extends K1> keyMapper,
                                              final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return globalTableJoin(globalTable, keyMapper, joiner, true);
    }

    @Override
    public <K1, V1, V2> KStream<K, V2> join(final GlobalKTable<K1, V1> globalTable,
                                            final KeyValueMapper<? super K, ? super V, ? extends K1> keyMapper,
                                            final ValueJoiner<? super V, ? super V1, ? extends V2> joiner) {
        return globalTableJoin(globalTable, keyMapper, joiner, false);
    }

    private <K1, V1, V2> KStream<K, V2> globalTableJoin(final GlobalKTable<K1, V1> globalTable,
                                                        final KeyValueMapper<? super K, ? super V, ? extends K1> keyMapper,
                                                        final ValueJoiner<? super V, ? super V1, ? extends V2> joiner,
                                                        final boolean leftJoin) {
        Objects.requireNonNull(globalTable, "globalTable can't be null");
        Objects.requireNonNull(keyMapper, "keyMapper can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

        final KTableValueGetterSupplier<K1, V1> valueGetterSupplier = ((GlobalKTableImpl<K1, V1>) globalTable).valueGetterSupplier();
        final String name = builder.newProcessorName(LEFTJOIN_NAME);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamGlobalKTableJoin<>(valueGetterSupplier, joiner, keyMapper, leftJoin), this.name);

        ProcessorSupplier<K, V> processorSupplier = new KStreamGlobalKTableJoin<>(valueGetterSupplier,
                                                                                  joiner,
                                                                                  keyMapper,

                                                                                  leftJoin);
        ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(processorSupplier, name);

        StreamTableJoinNode<K, V> streamTableJoinNode = new StreamTableJoinNode<>(name,
                                                                                  processorParameters,
                                                                                  new String[]{});
        streamTableJoinNode.setGlobalKTableJoin(true);
        addGraphNode(streamTableJoinNode);

        return new KStreamImpl<>(builder, name, sourceNodes, false, streamTableJoinNode);
    }

    @SuppressWarnings("unchecked")
    private <V1, R> KStream<K, R> doStreamTableJoin(final KTable<K, V1> other,
                                                    final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                                    final boolean leftJoin) {
        Objects.requireNonNull(other, "other KTable can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

        final Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K>) other);

        final String name = builder.newProcessorName(leftJoin ? LEFTJOIN_NAME : JOIN_NAME);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamKTableJoin<>(((KTableImpl<K, ?, V1>) other).valueGetterSupplier(), joiner, leftJoin), this.name);
        builder.internalTopologyBuilder.connectProcessorAndStateStores(name, ((KTableImpl) other).valueGetterSupplier().storeNames());

        ProcessorSupplier<K, V> processorSupplier = new KStreamKTableJoin<>(((KTableImpl<K, ?, V1>) other).valueGetterSupplier(),
                                                                            joiner,
                                                                            leftJoin);

        ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(processorSupplier, name);
        StreamTableJoinNode<K, V> streamTableJoinNode = new StreamTableJoinNode<>(name,
                                                                                  processorParameters,
                                                                                  ((KTableImpl) other).valueGetterSupplier().storeNames());

        addGraphNode(streamTableJoinNode);

        return new KStreamImpl<>(builder, name, allSourceNodes, false, streamTableJoinNode);
    }

    @Override
    public <V1, R> KStream<K, R> leftJoin(final KTable<K, V1> other, final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return leftJoin(other, joiner, Joined.<K, V, V1>with(null, null, null));
    }

    @Override
    public <VT, VR> KStream<K, VR> leftJoin(final KTable<K, VT> other,
                                            final ValueJoiner<? super V, ? super VT, ? extends VR> joiner,
                                            final Joined<K, V, VT> joined) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(joined, "joined can't be null");
        if (repartitionRequired) {
            final KStreamImpl<K, V> thisStreamRepartitioned = this.repartitionForJoin(joined.keySerde(), joined.valueSerde());
            return thisStreamRepartitioned.doStreamTableJoin(other, joiner, true);
        } else {
            return doStreamTableJoin(other, joiner, true);
        }
    }

    @Override
    public <K1> KGroupedStream<K1, V> groupBy(final KeyValueMapper<? super K, ? super V, K1> selector) {
        return groupBy(selector, Serialized.<K1, V>with(null, null));
    }

    @Override
    public <KR> KGroupedStream<KR, V> groupBy(final KeyValueMapper<? super K, ? super V, KR> selector,
                                              final Serialized<KR, V> serialized) {
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(serialized, "serialized can't be null");
        final SerializedInternal<KR, V> serializedInternal = new SerializedInternal<>(serialized);
        final StatelessProcessorNode<K, V> graphNode = internalSelectKey(selector);
        graphNode.keyChangingOperation(true);

        addGraphNode(graphNode);
        return new KGroupedStreamImpl<>(builder,
                                        graphNode.nodeName(),
                                        sourceNodes,
                                        serializedInternal.keySerde(),
                                        serializedInternal.valueSerde(),
                                        true,
                                        graphNode);
    }

    @Override
    public KGroupedStream<K, V> groupByKey() {
        return groupByKey(Serialized.<K, V>with(null, null));
    }

    @Override
    public KGroupedStream<K, V> groupByKey(final Serialized<K, V> serialized) {
        final SerializedInternal<K, V> serializedInternal = new SerializedInternal<>(serialized);

        final StatelessProcessorNode<K, V> graphNode = new StatelessProcessorNode<>(
            this.name,
            null,
            repartitionRequired,
            Collections.<String>emptyList());

        addGraphNode(graphNode);

        return new KGroupedStreamImpl<>(builder,
                                        this.name,
                                        sourceNodes,
                                        serializedInternal.keySerde(),
                                        serializedInternal.valueSerde(),
                                        this.repartitionRequired,
                                        graphNode);

    }

    private static <K, V> StoreBuilder<WindowStore<K, V>> createWindowedStateStore(final JoinWindows windows,
                                                                                   final Serde<K> keySerde,
                                                                                   final Serde<V> valueSerde,
                                                                                   final String storeName) {
        return Stores.windowStoreBuilder(Stores.persistentWindowStore(storeName,
                                                                      windows.maintainMs(),
                                                                      windows.segments,
                                                                      windows.size(),
                                                                      true), keySerde, valueSerde);

    }

    private class KStreamImplJoin {

        private final boolean leftOuter;
        private final boolean rightOuter;

        KStreamImplJoin(final boolean leftOuter,
                        final boolean rightOuter) {
            this.leftOuter = leftOuter;
            this.rightOuter = rightOuter;
        }

        @SuppressWarnings("unchecked")
        public <K1, R, V1, V2> KStream<K1, R> join(final KStream<K1, V1> lhs,
                                                   final KStream<K1, V2> other,
                                                   final ValueJoiner<? super V1, ? super V2, ? extends R> joiner,
                                                   final JoinWindows windows,
                                                   final Joined<K1, V1, V2> joined) {
            String thisWindowStreamName = builder.newProcessorName(WINDOWED_NAME);
            String otherWindowStreamName = builder.newProcessorName(WINDOWED_NAME);
            String joinThisName = rightOuter ? builder.newProcessorName(OUTERTHIS_NAME) : builder.newProcessorName(JOINTHIS_NAME);
            String joinOtherName = leftOuter ? builder.newProcessorName(OUTEROTHER_NAME) : builder.newProcessorName(JOINOTHER_NAME);
            String joinMergeName = builder.newProcessorName(MERGE_NAME);

            final StoreBuilder<WindowStore<K1, V1>> thisWindow =
                createWindowedStateStore(windows, joined.keySerde(), joined.valueSerde(), joinThisName + "-store");

            final StoreBuilder<WindowStore<K1, V2>> otherWindow =
                createWindowedStateStore(windows, joined.keySerde(), joined.otherValueSerde(), joinOtherName + "-store");

            KStreamJoinWindow<K1, V1> thisWindowedStream = new KStreamJoinWindow<>(thisWindow.name(),
                                                                                   windows.beforeMs + windows.afterMs + 1,
                                                                                   windows.maintainMs());
            KStreamJoinWindow<K1, V2> otherWindowedStream = new KStreamJoinWindow<>(otherWindow.name(),
                                                                                    windows.beforeMs + windows.afterMs + 1,
                                                                                    windows.maintainMs());

            final KStreamKStreamJoin<K1, R, ? super V1, ? super V2> joinThis = new KStreamKStreamJoin<>(otherWindow.name(),
                                                                                                        windows.beforeMs,
                                                                                                        windows.afterMs,
                                                                                                        joiner,
                                                                                                        leftOuter);
            final KStreamKStreamJoin<K1, R, ? super V2, ? super V1> joinOther = new KStreamKStreamJoin<>(thisWindow.name(),
                                                                                                         windows.afterMs,
                                                                                                         windows.beforeMs,
                                                                                                         reverseJoiner(joiner),
                                                                                                         rightOuter);

            KStreamPassThrough<K1, R> joinMerge = new KStreamPassThrough<>();

            StreamStreamJoinNode.StreamStreamJoinNodeBuilder<K, V1, V2, R> joinBuilder = StreamStreamJoinNode.streamStreamJoinNodeBuilder();

            ProcessorParameters thisWindowStreamProcessorParams = new ProcessorParameters(thisWindowedStream, thisWindowStreamName);
            ProcessorParameters otherWindowStreamProcessorParams = new ProcessorParameters(otherWindowedStream, otherWindowStreamName);
            ProcessorParameters joinThisProcessorParams = new ProcessorParameters(joinThis, joinThisName);
            ProcessorParameters joinOtherProcessorParams = new ProcessorParameters(joinOther, joinOtherName);
            ProcessorParameters joinMergeProcessorParams = new ProcessorParameters(joinMerge, joinMergeName);

            joinBuilder.withJoinMergeProcessorParameters(joinMergeProcessorParams)
                .withJoinThisProcessorParameters(joinThisProcessorParams)
                .withJoinOtherProcessorParameters(joinOtherProcessorParams)
                .withThisWindowedStreamProcessorParameters(thisWindowStreamProcessorParams)
                .withOtherWindowedStreamProcessorParameters(otherWindowStreamProcessorParams)
                .withThisWindowStoreBuilder(thisWindow)
                .withOtherWindowStoreBuilder(otherWindow)
                .withLeftHandSideStreamName(((AbstractStream) lhs).name)
                .withOtherStreamName(((AbstractStream) other).name)
                .withValueJoiner(joiner)
                .withNodeName(joinMergeName);

            addGraphNode(joinBuilder.build());

            builder.internalTopologyBuilder.addProcessor(thisWindowStreamName, thisWindowedStream, ((AbstractStream) lhs).name);
            builder.internalTopologyBuilder.addProcessor(otherWindowStreamName, otherWindowedStream, ((AbstractStream) other).name);
            builder.internalTopologyBuilder.addProcessor(joinThisName, joinThis, thisWindowStreamName);
            builder.internalTopologyBuilder.addProcessor(joinOtherName, joinOther, otherWindowStreamName);
            builder.internalTopologyBuilder.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);
            builder.internalTopologyBuilder.addStateStore(thisWindow, thisWindowStreamName, joinOtherName);
            builder.internalTopologyBuilder.addStateStore(otherWindow, otherWindowStreamName, joinThisName);

            Set<String> allSourceNodes = new HashSet<>(((AbstractStream<K>) lhs).sourceNodes);
            allSourceNodes.addAll(((KStreamImpl<K1, V2>) other).sourceNodes);
            return new KStreamImpl<>(builder, joinMergeName, allSourceNodes, false, null);
        }
    }

}
