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
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.PrintForeachAction;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.Stores;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class KStreamImpl<K, V> extends AbstractStream<K> implements KStream<K, V> {

    // TODO: change to package-private after removing KStreamBuilder
    public static final String SOURCE_NAME = "KSTREAM-SOURCE-";

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

    private final KeyValueMapper<K, V, String> defaultKeyValueMapper;

    private final boolean repartitionRequired;

    public KStreamImpl(final InternalStreamsBuilder builder,
                       final String name,
                       final Set<String> sourceNodes,
                       final boolean repartitionRequired) {
        super(builder, name, sourceNodes);
        this.repartitionRequired = repartitionRequired;
        this.defaultKeyValueMapper = new KeyValueMapper<K, V, String>() {
            @Override
            public String apply(K key, V value) {
                return String.format("%s, %s", key, value);
            }
        };
    }

    @Override
    public KStream<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        String name = builder.newName(FILTER_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamFilter<>(predicate, false), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, this.repartitionRequired);
    }

    @Override
    public KStream<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        String name = builder.newName(FILTER_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamFilter<>(predicate, true), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, this.repartitionRequired);
    }

    @Override
    public <K1> KStream<K1, V> selectKey(final KeyValueMapper<? super K, ? super V, ? extends K1> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        return new KStreamImpl<>(builder, internalSelectKey(mapper), sourceNodes, true);
    }

    private <K1> String internalSelectKey(final KeyValueMapper<? super K, ? super V, ? extends K1> mapper) {
        String name = builder.newName(KEY_SELECT_NAME);
        builder.internalTopologyBuilder.addProcessor(
            name,
            new KStreamMap<>(
                new KeyValueMapper<K, V, KeyValue<K1, V>>() {
                    @Override
                    public KeyValue<K1, V> apply(K key, V value) {
                        return new KeyValue<>(mapper.apply(key, value), value);
                    }
                }
            ),
            this.name
        );
        return name;
    }

    @Override
    public <K1, V1> KStream<K1, V1> map(final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends K1, ? extends V1>> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        String name = builder.newName(MAP_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamMap<>(mapper), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, true);
    }


    @Override
    public <V1> KStream<K, V1> mapValues(final ValueMapper<? super V, ? extends V1> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        String name = builder.newName(MAPVALUES_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamMapValues<>(mapper), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, this.repartitionRequired);
    }

    @Override
    public void print() {
        print(defaultKeyValueMapper, null, null, this.name);
    }

    @Override
    public void print(final String label) {
        print(defaultKeyValueMapper, null, null, label);
    }

    @Override
    public void print(final Serde<K> keySerde,
                      final Serde<V> valSerde) {
        print(defaultKeyValueMapper, keySerde, valSerde, this.name);
    }

    @Override
    public void print(final Serde<K> keySerde,
                      final Serde<V> valSerde,
                      final String label) {
        print(defaultKeyValueMapper, keySerde, valSerde, label);
    }

    @Override
    public void print(final KeyValueMapper<? super K, ? super V, String> mapper) {
        print(mapper, null, null, this.name);
    }

    @Override
    public void print(final KeyValueMapper<? super K, ? super V, String> mapper,
                      final String label) {
        print(mapper, null, null, label);
    }

    @Override
    public void print(final KeyValueMapper<? super K, ? super V, String> mapper,
                      final Serde<K> keySerde,
                      final Serde<V> valSerde) {
        print(mapper, keySerde, valSerde, this.name);
    }

    @Override
    public void print(final KeyValueMapper<? super K, ? super V, String> mapper,
                      final Serde<K> keySerde,
                      final Serde<V> valSerde,
                      final String label) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(label, "label can't be null");
        String name = builder.newName(PRINTING_NAME);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamPrint<>(new PrintForeachAction<>(null, mapper, label), keySerde, valSerde), this.name);
    }

    @Override
    public void writeAsText(final String filePath) {
        writeAsText(filePath, this.name, null, null, defaultKeyValueMapper);
    }

    @Override
    public void writeAsText(final String filePath,
                            final String label) {
        writeAsText(filePath, label, null, null, defaultKeyValueMapper);
    }

    @Override
    public void writeAsText(final String filePath,
                            final Serde<K> keySerde,
                            final Serde<V> valSerde) {
        writeAsText(filePath, this.name, keySerde, valSerde, defaultKeyValueMapper);
    }

    @Override
    public void writeAsText(final String filePath,
                            final String label,
                            final Serde<K> keySerde,
                            final Serde<V> valSerde) {
        writeAsText(filePath, label, keySerde, valSerde, defaultKeyValueMapper);
    }

    @Override
    public void writeAsText(final String filePath,
                            final KeyValueMapper<? super K, ? super V, String> mapper) {
        writeAsText(filePath, this.name, null, null, mapper);
    }

    @Override
    public void writeAsText(final String filePath,
                            final String label,
                            final KeyValueMapper<? super K, ? super V, String> mapper) {
        writeAsText(filePath, label, null, null, mapper);
    }

    @Override
    public void writeAsText(final String filePath,
                            final Serde<K> keySerde,
                            final Serde<V> valSerde,
                            final KeyValueMapper<? super K, ? super V, String> mapper) {
        writeAsText(filePath, this.name, keySerde, valSerde, mapper);
    }

    @Override
    public void writeAsText(final String filePath,
                            final String label,
                            final Serde<K> keySerde,
                            final Serde<V> valSerde, KeyValueMapper<? super K, ? super V, String> mapper) {
        Objects.requireNonNull(filePath, "filePath can't be null");
        Objects.requireNonNull(label, "label can't be null");
        Objects.requireNonNull(mapper, "mapper can't be null");
        if (filePath.trim().isEmpty()) {
            throw new TopologyException("filePath can't be an empty string");
        }
        final String name = builder.newName(PRINTING_NAME);
        try {
            PrintWriter printWriter = new PrintWriter(filePath, StandardCharsets.UTF_8.name());
            builder.internalTopologyBuilder.addProcessor(name, new KStreamPrint<>(new PrintForeachAction<>(printWriter, mapper, label), keySerde, valSerde), this.name);
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            throw new TopologyException("Unable to write stream to file at [" + filePath + "] " + e.getMessage());
        }
    }

    @Override
    public <K1, V1> KStream<K1, V1> flatMap(final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends K1, ? extends V1>>> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        String name = builder.newName(FLATMAP_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamFlatMap<>(mapper), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, true);
    }

    @Override
    public <V1> KStream<K, V1> flatMapValues(final ValueMapper<? super V, ? extends Iterable<? extends V1>> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        String name = builder.newName(FLATMAPVALUES_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamFlatMapValues<>(mapper), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, this.repartitionRequired);
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
        String branchName = builder.newName(BRANCH_NAME);

        builder.internalTopologyBuilder.addProcessor(branchName, new KStreamBranch(predicates.clone()), this.name);

        KStream<K, V>[] branchChildren = (KStream<K, V>[]) Array.newInstance(KStream.class, predicates.length);
        for (int i = 0; i < predicates.length; i++) {
            String childName = builder.newName(BRANCHCHILD_NAME);

            builder.internalTopologyBuilder.addProcessor(childName, new KStreamPassThrough<K, V>(), branchName);

            branchChildren[i] = new KStreamImpl<>(builder, childName, sourceNodes, this.repartitionRequired);
        }

        return branchChildren;
    }

    public static <K, V> KStream<K, V> merge(final InternalStreamsBuilder builder,
                                             final KStream<K, V>[] streams) {
        if (streams == null || streams.length == 0) {
            throw new IllegalArgumentException("Parameter <streams> must not be null or has length zero");
        }

        String name = builder.newName(MERGE_NAME);
        String[] parentNames = new String[streams.length];
        Set<String> allSourceNodes = new HashSet<>();
        boolean requireRepartitioning = false;

        for (int i = 0; i < streams.length; i++) {
            KStreamImpl<K, V> stream = (KStreamImpl<K, V>) streams[i];

            parentNames[i] = stream.name;
            requireRepartitioning |= stream.repartitionRequired;
            allSourceNodes.addAll(stream.sourceNodes);
        }

        builder.internalTopologyBuilder.addProcessor(name, new KStreamPassThrough<>(), parentNames);

        return new KStreamImpl<>(builder, name, allSourceNodes, requireRepartitioning);
    }

    @Override
    public KStream<K, V> through(final Serde<K> keySerde,
                                 final Serde<V> valSerde,
                                 final StreamPartitioner<? super K, ? super V> partitioner, String topic) {
        to(keySerde, valSerde, partitioner, topic);

        return builder.stream(null, new FailOnInvalidTimestamp(), keySerde, valSerde, topic);
    }

    @Override
    public void foreach(final ForeachAction<? super K, ? super V> action) {
        Objects.requireNonNull(action, "action can't be null");
        String name = builder.newName(FOREACH_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamPeek<>(action, false), this.name);
    }

    @Override
    public KStream<K, V> peek(final ForeachAction<? super K, ? super V> action) {
        Objects.requireNonNull(action, "action can't be null");
        final String name = builder.newName(PEEK_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamPeek<>(action, true), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, repartitionRequired);
    }

    @Override
    public KStream<K, V> through(final Serde<K> keySerde,
                                 final Serde<V> valSerde,
                                 final String topic) {
        return through(keySerde, valSerde, null, topic);
    }

    @Override
    public KStream<K, V> through(final StreamPartitioner<? super K, ? super V> partitioner,
                                 final String topic) {
        return through(null, null, partitioner, topic);
    }

    @Override
    public KStream<K, V> through(final String topic) {
        return through(null, null, null, topic);
    }

    @Override
    public void to(final String topic) {
        to(null, null, null, topic);
    }

    @Override
    public void to(final StreamPartitioner<? super K, ? super V> partitioner,
                   final String topic) {
        to(null, null, partitioner, topic);
    }

    @Override
    public void to(final Serde<K> keySerde,
                   final Serde<V> valSerde,
                   final String topic) {
        to(keySerde, valSerde, null, topic);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void to(final Serde<K> keySerde,
                   final Serde<V> valSerde,
                   final StreamPartitioner<? super K, ? super V> partitioner,
                   final String topic) {
        Objects.requireNonNull(topic, "topic can't be null");
        final String name = builder.newName(SINK_NAME);

        final Serializer<K> keySerializer = keySerde == null ? null : keySerde.serializer();
        final Serializer<V> valSerializer = valSerde == null ? null : valSerde.serializer();

        if (partitioner == null && keySerializer != null && keySerializer instanceof WindowedSerializer) {
            final WindowedSerializer<Object> windowedSerializer = (WindowedSerializer<Object>) keySerializer;
            final StreamPartitioner<K, V> windowedPartitioner = (StreamPartitioner<K, V>) new WindowedStreamPartitioner<Object, V>(topic, windowedSerializer);
            builder.internalTopologyBuilder.addSink(name, topic, keySerializer, valSerializer, windowedPartitioner, this.name);
        } else {
            builder.internalTopologyBuilder.addSink(name, topic, keySerializer, valSerializer, partitioner, this.name);
        }
    }

    @Override
    public <K1, V1> KStream<K1, V1> transform(final TransformerSupplier<? super K, ? super V, KeyValue<K1, V1>> transformerSupplier,
                                              final String... stateStoreNames) {
        Objects.requireNonNull(transformerSupplier, "transformerSupplier can't be null");
        String name = builder.newName(TRANSFORM_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamTransform<>(transformerSupplier), this.name);
        if (stateStoreNames != null && stateStoreNames.length > 0) {
            builder.internalTopologyBuilder.connectProcessorAndStateStores(name, stateStoreNames);
        }

        return new KStreamImpl<>(builder, name, sourceNodes, true);
    }

    @Override
    public <V1> KStream<K, V1> transformValues(final ValueTransformerSupplier<? super V, ? extends V1> valueTransformerSupplier,
                                               final String... stateStoreNames) {
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformSupplier can't be null");
        String name = builder.newName(TRANSFORMVALUES_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamTransformValues<>(valueTransformerSupplier), this.name);
        if (stateStoreNames != null && stateStoreNames.length > 0) {
            builder.internalTopologyBuilder.connectProcessorAndStateStores(name, stateStoreNames);
        }

        return new KStreamImpl<>(builder, name, sourceNodes, this.repartitionRequired);
    }

    @Override
    public void process(final ProcessorSupplier<? super K, ? super V> processorSupplier,
                        final String... stateStoreNames) {
        final String name = builder.newName(PROCESSOR_NAME);

        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);
        if (stateStoreNames != null && stateStoreNames.length > 0) {
            builder.internalTopologyBuilder.connectProcessorAndStateStores(name, stateStoreNames);
        }
    }

    @Override
    public <V1, R> KStream<K, R> join(final KStream<K, V1> other,
                                      final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                      final JoinWindows windows,
                                      final Serde<K> keySerde,
                                      final Serde<V> thisValueSerde,
                                      final Serde<V1> otherValueSerde) {
        return doJoin(other, joiner, windows, keySerde, thisValueSerde, otherValueSerde,
            new KStreamImplJoin(false, false));
    }

    @Override
    public <V1, R> KStream<K, R> join(final KStream<K, V1> other,
                                      final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                      final JoinWindows windows) {
        return join(other, joiner, windows, null, null, null);
    }

    @Override
    public <V1, R> KStream<K, R> outerJoin(final KStream<K, V1> other,
                                           final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                           final JoinWindows windows,
                                           final Serde<K> keySerde,
                                           final Serde<V> thisValueSerde,
                                           final Serde<V1> otherValueSerde) {
        return doJoin(other, joiner, windows, keySerde, thisValueSerde, otherValueSerde, new KStreamImplJoin(true, true));
    }

    @Override
    public <V1, R> KStream<K, R> outerJoin(final KStream<K, V1> other,
                                           final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                           final JoinWindows windows) {
        return outerJoin(other, joiner, windows, null, null, null);
    }

    private <V1, R> KStream<K, R> doJoin(final KStream<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                         final JoinWindows windows,
                                         final Serde<K> keySerde,
                                         final Serde<V> thisValueSerde,
                                         final Serde<V1> otherValueSerde,
                                         final KStreamImplJoin join) {
        Objects.requireNonNull(other, "other KStream can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(windows, "windows can't be null");

        KStreamImpl<K, V> joinThis = this;
        KStreamImpl<K, V1> joinOther = (KStreamImpl<K, V1>) other;

        if (joinThis.repartitionRequired) {
            joinThis = joinThis.repartitionForJoin(keySerde, thisValueSerde);
        }

        if (joinOther.repartitionRequired) {
            joinOther = joinOther.repartitionForJoin(keySerde, otherValueSerde);
        }

        joinThis.ensureJoinableWith(joinOther);

        return join.join(joinThis,
            joinOther,
            joiner,
            windows,
            keySerde,
            thisValueSerde,
            otherValueSerde);
    }

    /**
     * Repartition a stream. This is required on join operations occurring after
     * an operation that changes the key, i.e, selectKey, map(..), flatMap(..).
     * @param keySerde      Serdes for serializing the keys
     * @param valSerde      Serdes for serilaizing the values
     * @return a new {@link KStreamImpl}
     */
    private KStreamImpl<K, V> repartitionForJoin(final Serde<K> keySerde,
                                                 final Serde<V> valSerde) {
        String repartitionedSourceName = createReparitionedSource(this, keySerde, valSerde, null);
        return new KStreamImpl<>(builder, repartitionedSourceName, Collections
            .singleton(repartitionedSourceName), false);
    }

    static <K1, V1> String createReparitionedSource(final AbstractStream<K1> stream,
                                                    final Serde<K1> keySerde,
                                                    final Serde<V1> valSerde,
                                                    final String topicNamePrefix) {
        Serializer<K1> keySerializer = keySerde != null ? keySerde.serializer() : null;
        Serializer<V1> valSerializer = valSerde != null ? valSerde.serializer() : null;
        Deserializer<K1> keyDeserializer = keySerde != null ? keySerde.deserializer() : null;
        Deserializer<V1> valDeserializer = valSerde != null ? valSerde.deserializer() : null;
        String baseName = topicNamePrefix != null ? topicNamePrefix : stream.name;

        String repartitionTopic = baseName + REPARTITION_TOPIC_SUFFIX;
        String sinkName = stream.builder.newName(SINK_NAME);
        String filterName = stream.builder.newName(FILTER_NAME);
        String sourceName = stream.builder.newName(SOURCE_NAME);

        stream.builder.internalTopologyBuilder.addInternalTopic(repartitionTopic);
        stream.builder.internalTopologyBuilder.addProcessor(filterName, new KStreamFilter<>(new Predicate<K1, V1>() {
            @Override
            public boolean test(final K1 key, final V1 value) {
                return key != null;
            }
        }, false), stream.name);

        stream.builder.internalTopologyBuilder.addSink(sinkName, repartitionTopic, keySerializer, valSerializer,
            null, filterName);
        stream.builder.internalTopologyBuilder.addSource(null, sourceName, new FailOnInvalidTimestamp(),
            keyDeserializer, valDeserializer, repartitionTopic);

        return sourceName;
    }

    @Override
    public <V1, R> KStream<K, R> leftJoin(final KStream<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                          final JoinWindows windows,
                                          final Serde<K> keySerde,
                                          final Serde<V> thisValSerde,
                                          final Serde<V1> otherValueSerde) {
        return doJoin(other,
            joiner,
            windows,
            keySerde,
            thisValSerde,
            otherValueSerde,
            new KStreamImplJoin(true, false));
    }

    @Override
    public <V1, R> KStream<K, R> leftJoin(final KStream<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                          final JoinWindows windows) {
        return leftJoin(other, joiner, windows, null, null, null);
    }

    @Override
    public <V1, R> KStream<K, R> join(final KTable<K, V1> other,
                                      final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return join(other, joiner, null, null);
    }

    @Override
    public <V1, R> KStream<K, R> join(final KTable<K, V1> other,
                                      final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                      final Serde<K> keySerde,
                                      final Serde<V> valueSerde) {
        if (repartitionRequired) {
            final KStreamImpl<K, V> thisStreamRepartitioned = repartitionForJoin(keySerde, valueSerde);
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
        final String name = builder.newName(LEFTJOIN_NAME);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamGlobalKTableJoin<>(valueGetterSupplier, joiner, keyMapper, leftJoin), this.name);
        return new KStreamImpl<>(builder, name, sourceNodes, false);
    }

    private <V1, R> KStream<K, R> doStreamTableJoin(final KTable<K, V1> other,
                                                    final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                                    final boolean leftJoin) {
        Objects.requireNonNull(other, "other KTable can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

        final Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K>) other);

        final String name = builder.newName(leftJoin ? LEFTJOIN_NAME : JOIN_NAME);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamKTableJoin<>(((KTableImpl<K, ?, V1>) other).valueGetterSupplier(), joiner, leftJoin), this.name);
        builder.internalTopologyBuilder.connectProcessorAndStateStores(name, ((KTableImpl<K, ?, V1>) other).internalStoreName());
        builder.internalTopologyBuilder.connectProcessors(this.name, ((KTableImpl<K, ?, V1>) other).name);

        return new KStreamImpl<>(builder, name, allSourceNodes, false);
    }

    @Override
    public <V1, R> KStream<K, R> leftJoin(final KTable<K, V1> other, final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return leftJoin(other, joiner, null, null);
    }

    public <V1, R> KStream<K, R> leftJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                          final Serde<K> keySerde,
                                          final Serde<V> valueSerde) {
        if (repartitionRequired) {
            final KStreamImpl<K, V> thisStreamRepartitioned = this.repartitionForJoin(keySerde, valueSerde);
            return thisStreamRepartitioned.doStreamTableJoin(other, joiner, true);
        } else {
            return doStreamTableJoin(other, joiner, true);
        }
    }

    @Override
    public <K1> KGroupedStream<K1, V> groupBy(final KeyValueMapper<? super K, ? super V, K1> selector) {
        return groupBy(selector, null, null);
    }

    @Override
    public <K1> KGroupedStream<K1, V> groupBy(final KeyValueMapper<? super K, ? super V, K1> selector,
                                              final Serde<K1> keySerde,
                                              final Serde<V> valSerde) {
        Objects.requireNonNull(selector, "selector can't be null");
        String selectName = internalSelectKey(selector);
        return new KGroupedStreamImpl<>(builder,
                                        selectName,
                                        sourceNodes,
                                        keySerde,
                                        valSerde, true);
    }

    @Override
    public KGroupedStream<K, V> groupByKey() {
        return groupByKey(null, null);
    }

    @Override
    public KGroupedStream<K, V> groupByKey(final Serde<K> keySerde,
                                           final Serde<V> valSerde) {
        return new KGroupedStreamImpl<>(builder,
                                        this.name,
                                        sourceNodes,
                                        keySerde,
                                        valSerde,
                                        this.repartitionRequired);
    }

    private static <K, V> StateStoreSupplier createWindowedStateStore(final JoinWindows windows,
                                                                      final Serde<K> keySerde,
                                                                      final Serde<V> valueSerde,
                                                                      final String storeName) {
        return Stores.create(storeName)
            .withKeys(keySerde)
            .withValues(valueSerde)
            .persistent()
            .windowed(windows.size(), windows.maintainMs(), windows.segments, true)
            .build();
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
                                                   final Serde<K1> keySerde,
                                                   final Serde<V1> lhsValueSerde,
                                                   final Serde<V2> otherValueSerde) {
            String thisWindowStreamName = builder.newName(WINDOWED_NAME);
            String otherWindowStreamName = builder.newName(WINDOWED_NAME);
            String joinThisName = rightOuter ? builder.newName(OUTERTHIS_NAME) : builder.newName(JOINTHIS_NAME);
            String joinOtherName = leftOuter ? builder.newName(OUTEROTHER_NAME) : builder.newName(JOINOTHER_NAME);
            String joinMergeName = builder.newName(MERGE_NAME);

            StateStoreSupplier thisWindow =
                createWindowedStateStore(windows, keySerde, lhsValueSerde, joinThisName + "-store");

            StateStoreSupplier otherWindow =
                createWindowedStateStore(windows, keySerde, otherValueSerde, joinOtherName + "-store");


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


            builder.internalTopologyBuilder.addProcessor(thisWindowStreamName, thisWindowedStream, ((AbstractStream) lhs).name);
            builder.internalTopologyBuilder.addProcessor(otherWindowStreamName, otherWindowedStream, ((AbstractStream) other).name);
            builder.internalTopologyBuilder.addProcessor(joinThisName, joinThis, thisWindowStreamName);
            builder.internalTopologyBuilder.addProcessor(joinOtherName, joinOther, otherWindowStreamName);
            builder.internalTopologyBuilder.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);
            builder.internalTopologyBuilder.addStateStore(thisWindow, thisWindowStreamName, joinOtherName);
            builder.internalTopologyBuilder.addStateStore(otherWindow, otherWindowStreamName, joinThisName);

            Set<String> allSourceNodes = new HashSet<>(((AbstractStream<K>) lhs).sourceNodes);
            allSourceNodes.addAll(((KStreamImpl<K1, V2>) other).sourceNodes);
            return new KStreamImpl<>(builder, joinMergeName, allSourceNodes, false);
        }
    }

}
