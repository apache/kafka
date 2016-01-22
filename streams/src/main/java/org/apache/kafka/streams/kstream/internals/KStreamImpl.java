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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.InsufficientTypeInfoException;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.type.TypeException;
import org.apache.kafka.streams.kstream.type.Types;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopologyException;
import org.apache.kafka.streams.state.internals.RocksDBWindowStoreSupplier;
import org.apache.kafka.streams.state.Serdes;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

public class KStreamImpl<K, V> extends AbstractStream<K> implements KStream<K, V> {

    public static final String AGGREGATE_NAME = "KSTREAM-AGGREGATE-";

    public static final String BRANCH_NAME = "KSTREAM-BRANCH-";

    public static final String BRANCHCHILD_NAME = "KSTREAM-BRANCHCHILD-";

    public static final String FILTER_NAME = "KSTREAM-FILTER-";

    public static final String FLATMAP_NAME = "KSTREAM-FLATMAP-";

    public static final String FLATMAPVALUES_NAME = "KSTREAM-FLATMAPVALUES-";

    public static final String JOINTHIS_NAME = "KSTREAM-JOINTHIS-";

    public static final String JOINOTHER_NAME = "KSTREAM-JOINOTHER-";

    public static final String LEFTJOIN_NAME = "KSTREAM-LEFTJOIN-";

    private static final String MAP_NAME = "KSTREAM-MAP-";

    private static final String MAPVALUES_NAME = "KSTREAM-MAPVALUES-";

    public static final String MERGE_NAME = "KSTREAM-MERGE-";

    public static final String OUTERTHIS_NAME = "KSTREAM-OUTERTHIS-";

    public static final String OUTEROTHER_NAME = "KSTREAM-OUTEROTHER-";

    private static final String PROCESSOR_NAME = "KSTREAM-PROCESSOR-";

    private static final String REDUCE_NAME = "KSTREAM-REDUCE-";

    private static final String SELECT_NAME = "KSTREAM-SELECT-";

    public static final String SINK_NAME = "KSTREAM-SINK-";

    public static final String SOURCE_NAME = "KSTREAM-SOURCE-";

    private static final String TRANSFORM_NAME = "KSTREAM-TRANSFORM-";

    private static final String TRANSFORMVALUES_NAME = "KSTREAM-TRANSFORMVALUES-";

    private static final String WINDOWED_NAME = "KSTREAM-WINDOWED-";


    public KStreamImpl(KStreamBuilder topology, String name, Set<String> sourceNodes, Type keyType, Type valueType) {
        super(topology, name, sourceNodes, keyType, valueType);
    }

    @Override
    public KStream<K, V> returns(Type keyType, Type valueType) {
        try {
            return new KStreamImpl<>(topology, name, sourceNodes, resolve(keyType), resolve(valueType));
        } catch (TypeException ex) {
            throw new TopologyException("failed to resolve a type of the stream", ex);
        }
    }

    @Override
    public KStream<K, V> returnsValue(Type valueType) {
        try {
            return new KStreamImpl<>(topology, name, sourceNodes, keyType, resolve(valueType));
        } catch (TypeException ex) {
            throw new TopologyException("failed to resolve a type of the stream", ex);
        }
    }

    @Override
    public KStream<K, V> filter(Predicate<K, V> predicate) {
        String name = topology.newName(FILTER_NAME);

        topology.addProcessor(name, new KStreamFilter<>(predicate, false), this.name);

        return new KStreamImpl<>(topology, name, sourceNodes, keyType, valueType);
    }

    @Override
    public KStream<K, V> filterOut(final Predicate<K, V> predicate) {
        String name = topology.newName(FILTER_NAME);

        topology.addProcessor(name, new KStreamFilter<>(predicate, true), this.name);

        return new KStreamImpl<>(topology, name, sourceNodes, keyType, valueType);
    }

    @Override
    public <K1, V1> KStream<K1, V1> map(KeyValueMapper<K, V, KeyValue<K1, V1>> mapper) {
        String name = topology.newName(MAP_NAME);

        topology.addProcessor(name, new KStreamMap<>(mapper), this.name);

        Type mappedType = resolveReturnType(mapper);

        return new KStreamImpl<>(topology, name, null, getKeyTypeFromKeyValueType(mappedType), getValueTypeFromKeyValueType(mappedType));
    }

    @Override
    public <V1> KStream<K, V1> mapValues(ValueMapper<V, V1> mapper) {
        String name = topology.newName(MAPVALUES_NAME);

        topology.addProcessor(name, new KStreamMapValues<>(mapper), this.name);

        return new KStreamImpl<>(topology, name, sourceNodes, keyType, resolveReturnType(mapper));
    }

    @Override
    public <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K, V, Iterable<KeyValue<K1, V1>>> mapper) {
        String name = topology.newName(FLATMAP_NAME);

        topology.addProcessor(name, new KStreamFlatMap<>(mapper), this.name);

        Type iterableType = resolveReturnType(mapper);
        Type elementType = null;
        if (iterableType != null)
            elementType = resolveElementTypeFromIterable(iterableType);

        return new KStreamImpl<>(topology, name, null, getKeyTypeFromKeyValueType(elementType), getValueTypeFromKeyValueType(elementType));
    }

    @Override
    public <V1> KStream<K, V1> flatMapValues(ValueMapper<V, Iterable<V1>> mapper) {
        String name = topology.newName(FLATMAPVALUES_NAME);

        topology.addProcessor(name, new KStreamFlatMapValues<>(mapper), this.name);

        Type iterableType = resolveReturnType(mapper);
        Type elementType = null;
        if (iterableType != null)
            elementType = resolveElementTypeFromIterable(iterableType);

        return new KStreamImpl<>(topology, name, sourceNodes, keyType, elementType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public KStream<K, V>[] branch(Predicate<K, V>... predicates) {
        String branchName = topology.newName(BRANCH_NAME);

        topology.addProcessor(branchName, new KStreamBranch(predicates.clone()), this.name);

        KStream<K, V>[] branchChildren = (KStream<K, V>[]) Array.newInstance(KStream.class, predicates.length);
        for (int i = 0; i < predicates.length; i++) {
            String childName = topology.newName(BRANCHCHILD_NAME);

            topology.addProcessor(childName, new KStreamPassThrough<K, V>(), branchName);

            branchChildren[i] = new KStreamImpl<>(topology, childName, sourceNodes, keyType, valueType);
        }

        return branchChildren;
    }

    public static <K, V> KStream<K, V> merge(KStreamBuilder topology, KStream<K, V>[] streams) {
        String name = topology.newName(MERGE_NAME);
        String[] parentNames = new String[streams.length];
        Set<String> allSourceNodes = new HashSet<>();

        for (int i = 0; i < streams.length; i++) {
            KStreamImpl stream = (KStreamImpl) streams[i];

            parentNames[i] = stream.name;

            if (allSourceNodes != null) {
                if (stream.sourceNodes != null)
                    allSourceNodes.addAll(stream.sourceNodes);
                else
                    allSourceNodes = null;
            }

        }

        topology.addProcessor(name, new KStreamPassThrough<>(), parentNames);

        // inherit types from the first stream
        Type keyType = null;
        Type valueType = null;
        if (streams.length > 0) {
            keyType = ((KStreamImpl) streams[0]).keyType;
            valueType = ((KStreamImpl) streams[0]).valueType;
        }

        return new KStreamImpl<>(topology, name, allSourceNodes, keyType, valueType);
    }

    @Override
    public KStream<K, V> through(String topic) {
        to(topic);

        return topology.stream(keyType, valueType, topic);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void to(String topic) {

        Serializer<K> keySerializer;
        Type windowedRawKeyType = isWindowedKeyType(keyType);
        if (windowedRawKeyType != null) {
            keySerializer = new WindowedSerializer(getSerializer(windowedRawKeyType));
        } else {
            keySerializer = getSerializer(keyType);
        }

        Serializer<V> valSerializer = getSerializer(valueType);

        String name = topology.newName(SINK_NAME);
        StreamPartitioner<K, V> streamPartitioner = null;

        if (keySerializer instanceof WindowedSerializer) {
            WindowedSerializer<Object> windowedSerializer = (WindowedSerializer<Object>) keySerializer;
            streamPartitioner = (StreamPartitioner<K, V>) new WindowedStreamPartitioner<Object, V>(windowedSerializer);
        }

        topology.addSink(name, topic, keySerializer, valSerializer, streamPartitioner, this.name);
    }

    @Override
    public <K1, V1> KStream<K1, V1> transform(TransformerSupplier<K, V, KeyValue<K1, V1>> transformerSupplier, String... stateStoreNames) {
        String name = topology.newName(TRANSFORM_NAME);

        topology.addProcessor(name, new KStreamTransform<>(transformerSupplier), this.name);
        topology.connectProcessorAndStateStores(name, stateStoreNames);

        return new KStreamImpl<>(topology, name, null, null, null);
    }

    @Override
    public <V1> KStream<K, V1> transformValues(ValueTransformerSupplier<V, V1> valueTransformerSupplier, String... stateStoreNames) {
        String name = topology.newName(TRANSFORMVALUES_NAME);

        topology.addProcessor(name, new KStreamTransformValues<>(valueTransformerSupplier), this.name);
        topology.connectProcessorAndStateStores(name, stateStoreNames);

        return new KStreamImpl<>(topology, name, sourceNodes, keyType, null);
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
            JoinWindows windows) {

        return join(other, joiner, windows, false);
    }

    @Override
    public <V1, R> KStream<K, R> outerJoin(
            KStream<K, V1> other,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows) {

        return join(other, joiner, windows, true);
    }

    @SuppressWarnings("unchecked")
    private <V1, R> KStream<K, R> join(
            KStream<K, V1> other,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows,
            boolean outer) {

        KStreamImpl<K, V1> otherStream = (KStreamImpl<K, V1>) other;

        Serializer<K> keySerializer = getSerializer(this.keyType);
        Serializer<V> thisValueSerializer = getSerializer(this.valueType);
        Serializer<V1> otherValueSerializer = getSerializer(otherStream.valueType);
        Deserializer<K> keyDeserializer = getDeserializer(this.keyType);
        Deserializer<V> thisValueDeserializer = getDeserializer(this.valueType);
        Deserializer<V1> otherValueDeserializer = getDeserializer(otherStream.valueType);

        Set<String> allSourceNodes = ensureJoinableWith(otherStream);

        RocksDBWindowStoreSupplier<K, V> thisWindow =
                new RocksDBWindowStoreSupplier<>(
                        windows.name() + "-this",
                        windows.maintainMs(),
                        windows.segments,
                        true,
                        new Serdes<>("", keySerializer, keyDeserializer, thisValueSerializer, thisValueDeserializer),
                        null);

        RocksDBWindowStoreSupplier<K, V1> otherWindow =
                new RocksDBWindowStoreSupplier<>(
                        windows.name() + "-other",
                        windows.maintainMs(),
                        windows.segments,
                        true,
                        new Serdes<>("", keySerializer, keyDeserializer, otherValueSerializer, otherValueDeserializer),
                        null);

        KStreamJoinWindow<K, V> thisWindowedStream = new KStreamJoinWindow<>(thisWindow.name(), windows.before + windows.after + 1, windows.maintainMs());
        KStreamJoinWindow<K, V1> otherWindowedStream = new KStreamJoinWindow<>(otherWindow.name(), windows.before + windows.after + 1, windows.maintainMs());

        KStreamKStreamJoin<K, R, V, V1> joinThis = new KStreamKStreamJoin<>(otherWindow.name(), windows.before, windows.after, joiner, outer);
        KStreamKStreamJoin<K, R, V1, V> joinOther = new KStreamKStreamJoin<>(thisWindow.name(), windows.before, windows.after, reverseJoiner(joiner), outer);

        KStreamPassThrough<K, R> joinMerge = new KStreamPassThrough<>();

        String thisWindowStreamName = topology.newName(WINDOWED_NAME);
        String otherWindowStreamName = topology.newName(WINDOWED_NAME);
        String joinThisName = outer ? topology.newName(OUTERTHIS_NAME) : topology.newName(JOINTHIS_NAME);
        String joinOtherName = outer ? topology.newName(OUTEROTHER_NAME) : topology.newName(JOINOTHER_NAME);
        String joinMergeName = topology.newName(MERGE_NAME);

        topology.addProcessor(thisWindowStreamName, thisWindowedStream, this.name);
        topology.addProcessor(otherWindowStreamName, otherWindowedStream, otherStream.name);
        topology.addProcessor(joinThisName, joinThis, thisWindowStreamName);
        topology.addProcessor(joinOtherName, joinOther, otherWindowStreamName);
        topology.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);
        topology.addStateStore(thisWindow, thisWindowStreamName, otherWindowStreamName);
        topology.addStateStore(otherWindow, thisWindowStreamName, otherWindowStreamName);

        return new KStreamImpl<>(topology, joinMergeName, allSourceNodes, keyType, resolveReturnType(joiner));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V1, R> KStream<K, R> leftJoin(
            KStream<K, V1> other,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows) {

        KStreamImpl<K, V1> otherStream = (KStreamImpl<K, V1>) other;

        Serializer<K> keySerializer = getSerializer(this.keyType);
        Serializer<V1> otherValueSerializer = getSerializer(otherStream.valueType);
        Deserializer<K> keyDeserializer = getDeserializer(this.keyType);
        Deserializer<V1> otherValueDeserializer = getDeserializer(otherStream.valueType);

        Set<String> allSourceNodes = ensureJoinableWith(otherStream);

        RocksDBWindowStoreSupplier<K, V1> otherWindow =
                new RocksDBWindowStoreSupplier<>(
                        windows.name() + "-this",
                        windows.maintainMs(),
                        windows.segments,
                        true,
                        new Serdes<>("", keySerializer, keyDeserializer, otherValueSerializer, otherValueDeserializer),
                        null);

        KStreamJoinWindow<K, V1> otherWindowedStream = new KStreamJoinWindow<>(otherWindow.name(), windows.before + windows.after + 1, windows.maintainMs());
        KStreamKStreamJoin<K, R, V, V1> joinThis = new KStreamKStreamJoin<>(otherWindow.name(), windows.before, windows.after, joiner, true);

        String otherWindowStreamName = topology.newName(WINDOWED_NAME);
        String joinThisName = topology.newName(LEFTJOIN_NAME);

        topology.addProcessor(otherWindowStreamName, otherWindowedStream, otherStream.name);
        topology.addProcessor(joinThisName, joinThis, this.name);
        topology.addStateStore(otherWindow, joinThisName, otherWindowStreamName);

        return new KStreamImpl<>(topology, joinThisName, allSourceNodes, keyType, resolveReturnType(joiner));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V1, R> KStream<K, R> leftJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner) {
        Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K>) other);

        String name = topology.newName(LEFTJOIN_NAME);

        topology.addProcessor(name, new KStreamKTableLeftJoin<>((KTableImpl<K, ?, V1>) other, joiner), this.name);

        return new KStreamImpl<>(topology, name, allSourceNodes, keyType, resolveReturnType(joiner));
    }

    @Override
    public <W extends Window> KTable<Windowed<K>, V> reduceByKey(Reducer<V> reducer, Windows<W> windows) {
        Serializer<K> keySerializer = getSerializer(keyType);
        Serializer<V> valueSerializer = getSerializer(valueType);
        Deserializer<K> keyDeserializer = getDeserializer(keyType);
        Deserializer<V> valueDeserializer = getDeserializer(valueType);

        // TODO: this agg window operator is only used for casting K to Windowed<K> for
        // KTableProcessorSupplier, which is a bit awkward and better be removed in the future
        String reduceName = topology.newName(REDUCE_NAME);
        String selectName = topology.newName(SELECT_NAME);

        ProcessorSupplier<K, V> aggWindowSupplier = new KStreamAggWindow<>();
        ProcessorSupplier<Windowed<K>, Change<V>> aggregateSupplier = new KStreamReduce<>(windows, windows.name(), reducer);

        RocksDBWindowStoreSupplier<K, V> aggregateStore =
                new RocksDBWindowStoreSupplier<>(
                        windows.name(),
                        windows.maintainMs(),
                        windows.segments,
                        false,
                        new Serdes<>("", keySerializer, keyDeserializer, valueSerializer, valueDeserializer),
                        null);

        // aggregate the values with the aggregator and local store
        topology.addProcessor(selectName, aggWindowSupplier, this.name);
        topology.addProcessor(reduceName, aggregateSupplier, selectName);
        topology.addStateStore(aggregateStore, reduceName);

        // return the KTable representation with the intermediate topic as the sources
        return new KTableImpl<>(topology, reduceName, aggregateSupplier, sourceNodes, keyType, valueType);
    }

    @Override
    public <T, W extends Window> KTable<Windowed<K>, T> aggregateByKey(final Aggregator<K, V, T> aggregator,
                                                                       final Windows<W> windows) {
        // TODO: this agg window operator is only used for casting K to Windowed<K> for
        // KTableProcessorSupplier, which is a bit awkward and better be removed in the future
        final String aggregateName = topology.newName(AGGREGATE_NAME);
        String selectName = topology.newName(SELECT_NAME);

        ProcessorSupplier<K, V> aggWindowSupplier = new KStreamAggWindow<>();
        ProcessorSupplier<Windowed<K>, Change<V>> aggregateSupplier = new KStreamAggregate<>(windows, windows.name(), aggregator);

        // aggregate the values with the aggregator and local store
        topology.addProcessor(selectName, aggWindowSupplier, this.name);
        topology.addProcessor(aggregateName, aggregateSupplier, selectName);

        Type windowedKeyType;
        try {
            if (keyType == null)
                throw new InsufficientTypeInfoException("key type");

            windowedKeyType = Types.type(Windowed.class, keyType);
        } catch (TypeException ex) {
            throw new TopologyException("failed to define a windowed key type", ex);
        }

        final KTable<Windowed<K>, T> aggTable = new KTableImpl<>(topology, aggregateName, aggregateSupplier, sourceNodes, windowedKeyType, null);
        final Serializer<K> aggKeySerializer = getSerializer(keyType);
        final Deserializer<K> aggKeyDeserializer = getDeserializer(keyType);

        // return the KTable representation with the intermediate topic as the sources
        return new LazyKTableWrapper<Windowed<K>, T>(topology, windowedKeyType, null) {
            @Override
            protected KTable<Windowed<K>, T> create(Type keyType, Type valueType) {
                Serializer<T> aggValueSerializer = getSerializer(valueType);
                Deserializer<T> aggValueDeserializer = getDeserializer(valueType);

                RocksDBWindowStoreSupplier<K, T> aggregateStore =
                        new RocksDBWindowStoreSupplier<>(
                                windows.name(),
                                windows.maintainMs(),
                                windows.segments,
                                false,
                                new Serdes<>("", aggKeySerializer, aggKeyDeserializer, aggValueSerializer, aggValueDeserializer),
                                null);

                topology.addStateStore(aggregateStore, aggregateName);

                return aggTable;
            }
        };
    }
}
