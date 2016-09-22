/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.Stores;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class KStreamImpl<K, V> extends AbstractStream<K> implements KStream<K, V> {

    private static final String BRANCH_NAME = "KSTREAM-BRANCH-";

    private static final String BRANCHCHILD_NAME = "KSTREAM-BRANCHCHILD-";

    public static final String FILTER_NAME = "KSTREAM-FILTER-";

    private static final String FLATMAP_NAME = "KSTREAM-FLATMAP-";

    private static final String FLATMAPVALUES_NAME = "KSTREAM-FLATMAPVALUES-";

    public static final String JOINTHIS_NAME = "KSTREAM-JOINTHIS-";

    public static final String JOINOTHER_NAME = "KSTREAM-JOINOTHER-";

    public static final String LEFTJOIN_NAME = "KSTREAM-LEFTJOIN-";

    private static final String MAP_NAME = "KSTREAM-MAP-";

    private static final String MAPVALUES_NAME = "KSTREAM-MAPVALUES-";

    public static final String MERGE_NAME = "KSTREAM-MERGE-";

    public static final String OUTERTHIS_NAME = "KSTREAM-OUTERTHIS-";

    public static final String OUTEROTHER_NAME = "KSTREAM-OUTEROTHER-";

    private static final String PROCESSOR_NAME = "KSTREAM-PROCESSOR-";

    private static final String PRINTING_NAME = "KSTREAM-PRINTER-";

    private static final String KEY_SELECT_NAME = "KSTREAM-KEY-SELECT-";

    public static final String SINK_NAME = "KSTREAM-SINK-";

    public static final String SOURCE_NAME = "KSTREAM-SOURCE-";

    private static final String TRANSFORM_NAME = "KSTREAM-TRANSFORM-";

    private static final String TRANSFORMVALUES_NAME = "KSTREAM-TRANSFORMVALUES-";

    private static final String WINDOWED_NAME = "KSTREAM-WINDOWED-";

    private static final String FOREACH_NAME = "KSTREAM-FOREACH-";

    public static final String REPARTITION_TOPIC_SUFFIX = "-repartition";

    private final boolean repartitionRequired;

    public KStreamImpl(KStreamBuilder topology, String name, Set<String> sourceNodes,
                       boolean repartitionRequired) {
        super(topology, name, sourceNodes);
        this.repartitionRequired = repartitionRequired;
    }

    @Override
    public KStream<K, V> filter(Predicate<K, V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        String name = topology.newName(FILTER_NAME);

        topology.addProcessor(name, new KStreamFilter<>(predicate, false), this.name);

        return new KStreamImpl<>(topology, name, sourceNodes, this.repartitionRequired);
    }

    @Override
    public KStream<K, V> filterNot(final Predicate<K, V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        String name = topology.newName(FILTER_NAME);

        topology.addProcessor(name, new KStreamFilter<>(predicate, true), this.name);

        return new KStreamImpl<>(topology, name, sourceNodes, this.repartitionRequired);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K1> KStream<K1, V> selectKey(final KeyValueMapper<K, V, K1> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        return new KStreamImpl<>(topology, internalSelectKey(mapper), sourceNodes, true);
    }

    private <K1> String internalSelectKey(final KeyValueMapper<K, V, K1> mapper) {
        String name = topology.newName(KEY_SELECT_NAME);
        topology.addProcessor(name, new KStreamMap<>(new KeyValueMapper<K, V, KeyValue<K1, V>>() {
            @Override
            public KeyValue<K1, V> apply(K key, V value) {
                return new KeyValue<>(mapper.apply(key, value), value);
            }
        }), this.name);
        return name;
    }

    @Override
    public <K1, V1> KStream<K1, V1> map(KeyValueMapper<K, V, KeyValue<K1, V1>> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        String name = topology.newName(MAP_NAME);

        topology.addProcessor(name, new KStreamMap<>(mapper), this.name);

        return new KStreamImpl<>(topology, name, sourceNodes, true);
    }


    @Override
    public <V1> KStream<K, V1> mapValues(ValueMapper<V, V1> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        String name = topology.newName(MAPVALUES_NAME);

        topology.addProcessor(name, new KStreamMapValues<>(mapper), this.name);

        return new KStreamImpl<>(topology, name, sourceNodes, this.repartitionRequired);
    }

    @Override
    public void print() {
        print(null, null, null);
    }

    @Override
    public void print(String streamName) {
        print(null, null, streamName);
    }

    @Override
    public void print(Serde<K> keySerde, Serde<V> valSerde) {
        print(keySerde, valSerde, null);
    }

    @Override
    public void print(Serde<K> keySerde, Serde<V> valSerde, String streamName) {
        String name = topology.newName(PRINTING_NAME);
        streamName = (streamName == null) ? this.name : streamName;
        topology.addProcessor(name, new KeyValuePrinter<>(keySerde, valSerde, streamName), this.name);
    }


    @Override
    public void writeAsText(String filePath) {
        writeAsText(filePath, null, null, null);
    }

    @Override
    public void writeAsText(String filePath, String streamName) {
        writeAsText(filePath, streamName, null, null);
    }


    @Override
    public void writeAsText(String filePath, Serde<K> keySerde, Serde<V> valSerde) {
        writeAsText(filePath, null, keySerde, valSerde);
    }

    /**
     * @throws TopologyBuilderException if file is not found
     */
    @Override
    public void writeAsText(String filePath, String streamName, Serde<K> keySerde, Serde<V> valSerde) {
        Objects.requireNonNull(filePath, "filePath can't be null");
        String name = topology.newName(PRINTING_NAME);
        streamName = (streamName == null) ? this.name : streamName;
        try {

            PrintStream printStream = new PrintStream(new FileOutputStream(filePath));
            topology.addProcessor(name, new KeyValuePrinter<>(printStream, keySerde, valSerde, streamName), this.name);

        } catch (FileNotFoundException e) {
            String message = "Unable to write stream to file at [" + filePath + "] " + e.getMessage();
            throw new TopologyBuilderException(message);
        }

    }

    @Override
    public <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K, V, Iterable<KeyValue<K1, V1>>> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        String name = topology.newName(FLATMAP_NAME);

        topology.addProcessor(name, new KStreamFlatMap<>(mapper), this.name);

        return new KStreamImpl<>(topology, name, sourceNodes, true);
    }

    @Override
    public <V1> KStream<K, V1> flatMapValues(ValueMapper<V, Iterable<V1>> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        String name = topology.newName(FLATMAPVALUES_NAME);

        topology.addProcessor(name, new KStreamFlatMapValues<>(mapper), this.name);

        return new KStreamImpl<>(topology, name, sourceNodes, this.repartitionRequired);
    }

    @Override
    @SuppressWarnings("unchecked")
    public KStream<K, V>[] branch(Predicate<K, V>... predicates) {
        if (predicates.length == 0) {
            throw new IllegalArgumentException("you must provide at least one predicate");
        }
        for (Predicate<K, V> predicate : predicates) {
            Objects.requireNonNull(predicate, "predicates can't have null values");
        }
        String branchName = topology.newName(BRANCH_NAME);

        topology.addProcessor(branchName, new KStreamBranch(predicates.clone()), this.name);

        KStream<K, V>[] branchChildren = (KStream<K, V>[]) Array.newInstance(KStream.class, predicates.length);
        for (int i = 0; i < predicates.length; i++) {
            String childName = topology.newName(BRANCHCHILD_NAME);

            topology.addProcessor(childName, new KStreamPassThrough<K, V>(), branchName);

            branchChildren[i] = new KStreamImpl<>(topology, childName, sourceNodes, this.repartitionRequired);
        }

        return branchChildren;
    }

    public static <K, V> KStream<K, V> merge(KStreamBuilder topology, KStream<K, V>[] streams) {
        if (streams == null || streams.length == 0) {
            throw new IllegalArgumentException("Parameter <streams> must not be null or has length zero");
        }

        String name = topology.newName(MERGE_NAME);
        String[] parentNames = new String[streams.length];
        Set<String> allSourceNodes = new HashSet<>();
        boolean requireRepartitioning = false;

        for (int i = 0; i < streams.length; i++) {
            KStreamImpl stream = (KStreamImpl) streams[i];

            parentNames[i] = stream.name;
            requireRepartitioning |= stream.repartitionRequired;
            allSourceNodes.addAll(stream.sourceNodes);
        }

        topology.addProcessor(name, new KStreamPassThrough<>(), parentNames);

        return new KStreamImpl<>(topology, name, allSourceNodes, requireRepartitioning);
    }

    @Override
    public KStream<K, V> through(Serde<K> keySerde, Serde<V> valSerde, StreamPartitioner<K, V> partitioner, String topic) {
        to(keySerde, valSerde, partitioner, topic);

        return topology.stream(keySerde, valSerde, topic);
    }

    @Override
    public void foreach(ForeachAction<K, V> action) {
        Objects.requireNonNull(action, "action can't be null");
        String name = topology.newName(FOREACH_NAME);

        topology.addProcessor(name, new KStreamForeach<>(action), this.name);
    }

    @Override
    public KStream<K, V> through(Serde<K> keySerde, Serde<V> valSerde, String topic) {
        return through(keySerde, valSerde, null, topic);
    }

    @Override
    public KStream<K, V> through(StreamPartitioner<K, V> partitioner, String topic) {
        return through(null, null, partitioner, topic);
    }

    @Override
    public KStream<K, V> through(String topic) {
        return through(null, null, null, topic);
    }

    @Override
    public void to(String topic) {
        to(null, null, null, topic);
    }

    @Override
    public void to(StreamPartitioner<K, V> partitioner, String topic) {
        to(null, null, partitioner, topic);
    }

    @Override
    public void to(Serde<K> keySerde, Serde<V> valSerde, String topic) {
        to(keySerde, valSerde, null, topic);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void to(Serde<K> keySerde, Serde<V> valSerde, StreamPartitioner<K, V> partitioner, String topic) {
        Objects.requireNonNull(topic, "topic can't be null");
        String name = topology.newName(SINK_NAME);

        Serializer<K> keySerializer = keySerde == null ? null : keySerde.serializer();
        Serializer<V> valSerializer = valSerde == null ? null : valSerde.serializer();
        
        if (partitioner == null && keySerializer != null && keySerializer instanceof WindowedSerializer) {
            WindowedSerializer<Object> windowedSerializer = (WindowedSerializer<Object>) keySerializer;
            partitioner = (StreamPartitioner<K, V>) new WindowedStreamPartitioner<Object, V>(windowedSerializer);
        }

        topology.addSink(name, topic, keySerializer, valSerializer, partitioner, this.name);
    }

    @Override
    public <K1, V1> KStream<K1, V1> transform(TransformerSupplier<K, V, KeyValue<K1, V1>> transformerSupplier, String... stateStoreNames) {
        Objects.requireNonNull(transformerSupplier, "transformerSupplier can't be null");
        String name = topology.newName(TRANSFORM_NAME);

        topology.addProcessor(name, new KStreamTransform<>(transformerSupplier), this.name);
        topology.connectProcessorAndStateStores(name, stateStoreNames);

        return new KStreamImpl<>(topology, name, sourceNodes, true);
    }

    @Override
    public <V1> KStream<K, V1> transformValues(ValueTransformerSupplier<V, V1> valueTransformerSupplier, String... stateStoreNames) {
        Objects.requireNonNull(valueTransformerSupplier, "valueTransformSupplier can't be null");
        String name = topology.newName(TRANSFORMVALUES_NAME);

        topology.addProcessor(name, new KStreamTransformValues<>(valueTransformerSupplier), this.name);
        topology.connectProcessorAndStateStores(name, stateStoreNames);

        return new KStreamImpl<>(topology, name, sourceNodes, this.repartitionRequired);
    }

    @Override
    public void process(final ProcessorSupplier<K, V> processorSupplier, String... stateStoreNames) {
        String name = topology.newName(PROCESSOR_NAME);

        topology.addProcessor(name, processorSupplier, this.name);
        topology.connectProcessorAndStateStores(name, stateStoreNames);
    }

    @Override
    public <V1, R> KStream<K, R> join(
            KStream<K, V1> other,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows,
            Serde<K> keySerde,
            Serde<V> thisValueSerde,
            Serde<V1> otherValueSerde) {

        return join(other, joiner, windows, keySerde, thisValueSerde, otherValueSerde, false);
    }

    @Override
    public <V1, R> KStream<K, R> join(
            KStream<K, V1> other,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows) {

        return join(other, joiner, windows, null, null, null, false);
    }

    @Override
    public <V1, R> KStream<K, R> outerJoin(
            KStream<K, V1> other,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows,
            Serde<K> keySerde,
            Serde<V> thisValueSerde,
            Serde<V1> otherValueSerde) {

        return join(other, joiner, windows, keySerde, thisValueSerde, otherValueSerde, true);
    }

    @Override
    public <V1, R> KStream<K, R> outerJoin(
            KStream<K, V1> other,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows) {

        return join(other, joiner, windows, null, null, null, true);
    }

    @SuppressWarnings("unchecked")
    private <V1, R> KStream<K, R> join(
            KStream<K, V1> other,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows,
            Serde<K> keySerde,
            Serde<V> thisValueSerde,
            Serde<V1> otherValueSerde,
            boolean outer) {

        return doJoin(other,
            joiner,
            windows,
            keySerde,
            thisValueSerde,
            otherValueSerde,
            new DefaultJoin(outer));
    }

    private <V1, R> KStream<K, R> doJoin(KStream<K, V1> other,
                                         ValueJoiner<V, V1, R> joiner,
                                         JoinWindows windows,
                                         Serde<K> keySerde,
                                         Serde<V> thisValueSerde,
                                         Serde<V1> otherValueSerde,
                                         KStreamImplJoin join) {
        Objects.requireNonNull(other, "other KStream can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(windows, "windows can't be null");

        KStreamImpl<K, V> joinThis = this;
        KStreamImpl<K, V1> joinOther = (KStreamImpl) other;

        if (joinThis.repartitionRequired) {
            joinThis = joinThis.repartitionForJoin(keySerde, thisValueSerde, null);
        }

        if (joinOther.repartitionRequired) {
            joinOther = joinOther.repartitionForJoin(keySerde, otherValueSerde, null);
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
     * @param topicNamePrefix  prefix of topic name created for repartitioning, can be null,
     *                         in which case the prefix will be auto-generated internally.
     * @return a new {@link KStreamImpl}
     */
    private KStreamImpl<K, V> repartitionForJoin(Serde<K> keySerde,
                                                 Serde<V> valSerde,
                                                 final String topicNamePrefix) {

        String repartitionedSourceName = createReparitionedSource(this, keySerde, valSerde, topicNamePrefix);
        return new KStreamImpl<>(topology, repartitionedSourceName, Collections
            .singleton(repartitionedSourceName), false);
    }

    static <K1, V1> String createReparitionedSource(AbstractStream<K1> stream,
                                                    Serde<K1> keySerde,
                                                    Serde<V1> valSerde,
                                                    final String topicNamePrefix) {
        Serializer<K1> keySerializer = keySerde != null ? keySerde.serializer() : null;
        Serializer<V1> valSerializer = valSerde != null ? valSerde.serializer() : null;
        Deserializer<K1> keyDeserializer = keySerde != null ? keySerde.deserializer() : null;
        Deserializer<V1> valDeserializer = valSerde != null ? valSerde.deserializer() : null;
        String baseName = topicNamePrefix != null ? topicNamePrefix : stream.name;

        String repartitionTopic = baseName + REPARTITION_TOPIC_SUFFIX;
        String sinkName = stream.topology.newName(SINK_NAME);
        String filterName = stream.topology.newName(FILTER_NAME);
        String sourceName = stream.topology.newName(SOURCE_NAME);

        stream.topology.addInternalTopic(repartitionTopic);
        stream.topology.addProcessor(filterName, new KStreamFilter<>(new Predicate<K1, V1>() {
            @Override
            public boolean test(final K1 key, final V1 value) {
                return key != null;
            }
        }, false), stream.name);

        stream.topology.addSink(sinkName, repartitionTopic, keySerializer,
                         valSerializer, filterName);
        stream.topology.addSource(sourceName, keyDeserializer, valDeserializer,
                           repartitionTopic);

        return sourceName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V1, R> KStream<K, R> leftJoin(
            KStream<K, V1> other,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows,
            Serde<K> keySerde,
            Serde<V> thisValSerde,
            Serde<V1> otherValueSerde) {

        return doJoin(other,
                      joiner,
                      windows,
                      keySerde,
                      thisValSerde,
                      otherValueSerde,
                      new LeftJoin());
    }

    @Override
    public <V1, R> KStream<K, R> leftJoin(
            KStream<K, V1> other,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows) {

        return leftJoin(other, joiner, windows, null, null, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V1, R> KStream<K, R> leftJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner) {
        return leftJoin(other, joiner, null, null);

    }

    public <V1, R> KStream<K, R> leftJoin(KTable<K, V1> other,
                                          ValueJoiner<V, V1, R> joiner,
                                          Serde<K> keySerde,
                                          Serde<V> valueSerde) {
        Objects.requireNonNull(other, "other KTable can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

        if (repartitionRequired) {
            KStreamImpl<K, V> thisStreamRepartitioned = this.repartitionForJoin(keySerde,
                                                                                valueSerde, null);
            return thisStreamRepartitioned.doStreamTableLeftJoin(other, joiner);
        } else {
            return doStreamTableLeftJoin(other, joiner);
        }

    }

    private <V1, R> KStream<K, R> doStreamTableLeftJoin(final KTable<K, V1> other,
                                                        final ValueJoiner<V, V1, R> joiner) {
        Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K>) other);

        String name = topology.newName(LEFTJOIN_NAME);

        topology.addProcessor(name, new KStreamKTableLeftJoin<>((KTableImpl<K, ?, V1>) other, joiner), this.name);
        topology.connectProcessors(this.name, ((KTableImpl<K, ?, V1>) other).name);

        return new KStreamImpl<>(topology, name, allSourceNodes, false);
    }

    @Override
    public <K1> KGroupedStream<K1, V> groupBy(KeyValueMapper<K, V, K1> selector) {
        return groupBy(selector, null, null);
    }

    @Override
    public <K1> KGroupedStream<K1, V> groupBy(KeyValueMapper<K, V, K1> selector,
                                                   Serde<K1> keySerde,
                                                   Serde<V> valSerde) {

        Objects.requireNonNull(selector, "selector can't be null");
        String selectName = internalSelectKey(selector);
        return new KGroupedStreamImpl<>(topology,
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
    public KGroupedStream<K, V> groupByKey(Serde<K> keySerde,
                                           Serde<V> valSerde) {
        return new KGroupedStreamImpl<>(topology,
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

    private interface KStreamImplJoin {

        <K1, R, V1, V2> KStream<K1, R> join(KStream<K1, V1> lhs,
                                            KStream<K1, V2> other,
                                            ValueJoiner<V1, V2, R> joiner,
                                            JoinWindows windows,
                                            Serde<K1> keySerde,
                                            Serde<V1> lhsValueSerde,
                                            Serde<V2> otherValueSerde);
    }

    private class DefaultJoin implements KStreamImplJoin {

        private final boolean outer;

        DefaultJoin(final boolean outer) {
            this.outer = outer;
        }

        @Override
        public <K1, R, V1, V2> KStream<K1, R> join(KStream<K1, V1> lhs,
                                                   KStream<K1, V2> other,
                                                   ValueJoiner<V1, V2, R> joiner,
                                                   JoinWindows windows,
                                                   Serde<K1> keySerde,
                                                   Serde<V1> lhsValueSerde,
                                                   Serde<V2> otherValueSerde) {
            String thisWindowStreamName = topology.newName(WINDOWED_NAME);
            String otherWindowStreamName = topology.newName(WINDOWED_NAME);
            String joinThisName = outer ? topology.newName(OUTERTHIS_NAME) : topology.newName(JOINTHIS_NAME);
            String joinOtherName = outer ? topology.newName(OUTEROTHER_NAME) : topology.newName(JOINOTHER_NAME);
            String joinMergeName = topology.newName(MERGE_NAME);

            StateStoreSupplier thisWindow =
                createWindowedStateStore(windows, keySerde, lhsValueSerde,  joinThisName + "-store");

            StateStoreSupplier otherWindow =
                createWindowedStateStore(windows, keySerde, otherValueSerde, joinOtherName + "-store");


            KStreamJoinWindow<K1, V1> thisWindowedStream = new KStreamJoinWindow<>(thisWindow.name(),
                                                                                   windows.before + windows.after + 1,
                                                                                   windows.maintainMs());
            KStreamJoinWindow<K1, V2> otherWindowedStream = new KStreamJoinWindow<>(otherWindow.name(),
                                                                                    windows.before + windows.after + 1,
                                                                                    windows.maintainMs());

            KStreamKStreamJoin<K1, R, V1, V2> joinThis = new KStreamKStreamJoin<>(otherWindow.name(),
                                                                                  windows.before,
                                                                                  windows.after,
                                                                                  joiner,
                                                                                  outer);
            KStreamKStreamJoin<K1, R, V2, V1> joinOther = new KStreamKStreamJoin<>(thisWindow.name(),
                                                                                   windows.after,
                                                                                   windows.before,
                                                                                   reverseJoiner(joiner),
                                                                                   outer);

            KStreamPassThrough<K1, R> joinMerge = new KStreamPassThrough<>();


            topology.addProcessor(thisWindowStreamName, thisWindowedStream, ((AbstractStream) lhs).name);
            topology.addProcessor(otherWindowStreamName, otherWindowedStream, ((AbstractStream) other).name);
            topology.addProcessor(joinThisName, joinThis, thisWindowStreamName);
            topology.addProcessor(joinOtherName, joinOther, otherWindowStreamName);
            topology.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);
            topology.addStateStore(thisWindow, thisWindowStreamName, otherWindowStreamName);
            topology.addStateStore(otherWindow, thisWindowStreamName, otherWindowStreamName);

            Set<String> allSourceNodes = new HashSet<>(((AbstractStream) lhs).sourceNodes);
            allSourceNodes.addAll(((KStreamImpl<K1, V2>) other).sourceNodes);
            return new KStreamImpl<>(topology, joinMergeName, allSourceNodes, false);
        }
    }


    private class LeftJoin implements KStreamImplJoin {

        @Override
        public <K1, R, V1, V2> KStream<K1, R> join(KStream<K1, V1> lhs,
                                                   KStream<K1, V2> other,
                                                   ValueJoiner<V1, V2, R> joiner,
                                                   JoinWindows windows,
                                                   Serde<K1> keySerde,
                                                   Serde<V1> lhsValueSerde,
                                                   Serde<V2> otherValueSerde) {
            String otherWindowStreamName = topology.newName(WINDOWED_NAME);
            String joinThisName = topology.newName(LEFTJOIN_NAME);

            StateStoreSupplier otherWindow =
                createWindowedStateStore(windows, keySerde, otherValueSerde, joinThisName + "-store");

            KStreamJoinWindow<K1, V1>
                otherWindowedStream = new KStreamJoinWindow<>(otherWindow.name(), windows.before + windows.after + 1, windows.maintainMs());
            KStreamKStreamJoin<K1, R, V1, V2>
                joinThis = new KStreamKStreamJoin<>(otherWindow.name(), windows.before, windows.after, joiner, true);



            topology.addProcessor(otherWindowStreamName, otherWindowedStream, ((AbstractStream) other).name);
            topology.addProcessor(joinThisName, joinThis, ((AbstractStream) lhs).name);
            topology.addStateStore(otherWindow, joinThisName, otherWindowStreamName);

            Set<String> allSourceNodes = new HashSet<>(((AbstractStream) lhs).sourceNodes);
            allSourceNodes.addAll(((KStreamImpl<K1, V2>) other).sourceNodes);
            return new KStreamImpl<>(topology, joinThisName, allSourceNodes, false);
        }
    }


}
