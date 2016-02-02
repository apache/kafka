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
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.type.internal.Resolver;
import org.apache.kafka.streams.kstream.type.TypeException;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Set;

/**
 * The implementation class of KTable
 * @param <K> the key type
 * @param <S> the source's (parent's) value type
 * @param <V> the value type
 */
public class KTableImpl<K, S, V> extends AbstractStream<K> implements KTable<K, V> {

    private static final String REPARTITION_TOPIC_SUFFIX = "-repartition";

    private static final String AGGREGATE_NAME = "KTABLE-AGGREGATE-";

    private static final String FILTER_NAME = "KTABLE-FILTER-";

    public static final String JOINTHIS_NAME = "KTABLE-JOINTHIS-";

    public static final String JOINOTHER_NAME = "KTABLE-JOINOTHER-";

    public static final String LEFTTHIS_NAME = "KTABLE-LEFTTHIS-";

    public static final String LEFTOTHER_NAME = "KTABLE-LEFTOTHER-";

    private static final String MAPVALUES_NAME = "KTABLE-MAPVALUES-";

    public static final String MERGE_NAME = "KTABLE-MERGE-";

    public static final String OUTERTHIS_NAME = "KTABLE-OUTERTHIS-";

    public static final String OUTEROTHER_NAME = "KTABLE-OUTEROTHER-";

    private static final String REDUCE_NAME = "KTABLE-REDUCE-";

    private static final String SELECT_NAME = "KTABLE-SELECT-";

    public static final String SOURCE_NAME = "KTABLE-SOURCE-";

    private static final String TOSTREAM_NAME = "KTABLE-TOSTREAM-";


    public final ProcessorSupplier<K, ?> processorSupplier;

    private boolean sendOldValues = false;

    public KTableImpl(KStreamBuilder topology,
                      String name,
                      ProcessorSupplier<K, ?> processorSupplier,
                      Set<String> sourceNodes,
                      Type keyType,
                      Type valueType) {
        super(topology, name, sourceNodes, keyType, valueType);
        this.processorSupplier = processorSupplier;
    }

    @Override
    public KTable<K, V> returns(Type keyType, Type valueType) {
        try {
            return new KTableImpl<>(topology, name, processorSupplier, sourceNodes, Resolver.resolve(keyType), Resolver.resolve(valueType));
        } catch (TypeException ex) {
            throw new TopologyBuilderException("failed to resolve a type of the stream", ex);
        }
    }

    @Override
    public KTable<K, V> returnsValue(Type valueType) {
        try {
            return new KTableImpl<>(topology, name, processorSupplier, sourceNodes, keyType, Resolver.resolve(valueType));
        } catch (TypeException ex) {
            throw new TopologyBuilderException("failed to resolve a type of the stream", ex);
        }
    }

    @Override
    public KTable<K, V> filter(Predicate<K, V> predicate) {
        String name = topology.newName(FILTER_NAME);
        KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(this, predicate, false);
        topology.addProcessor(name, processorSupplier, this.name);

        return new KTableImpl<>(topology, name, processorSupplier, sourceNodes, keyType, valueType);
    }

    @Override
    public KTable<K, V> filterOut(final Predicate<K, V> predicate) {
        String name = topology.newName(FILTER_NAME);
        KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(this, predicate, true);

        topology.addProcessor(name, processorSupplier, this.name);

        return new KTableImpl<>(topology, name, processorSupplier, sourceNodes, keyType, valueType);
    }

    @Override
    public <V1> KTable<K, V1> mapValues(ValueMapper<V, V1> mapper) {
        String name = topology.newName(MAPVALUES_NAME);
        KTableProcessorSupplier<K, V, V1> processorSupplier = new KTableMapValues<>(this, mapper);

        topology.addProcessor(name, processorSupplier, this.name);

        return new KTableImpl<>(topology, name, processorSupplier, sourceNodes, keyType, resolveReturnType(mapper));
    }

    @Override
    public KTable<K, V> through(String topic) {
        to(topic);

        return topology.table(keyType, valueType, topic);
    }

    @Override
    public void to(String topic) {
        this.toStream().to(topic);
    }

    @Override
    public KStream<K, V> toStream() {
        String name = topology.newName(TOSTREAM_NAME);

        topology.addProcessor(name, new KStreamMapValues<K, Change<V>, V>(new ValueMapper<Change<V>, V>() {
            @Override
            public V apply(Change<V> change) {
                return change.newValue;
            }
        }), this.name);

        return new KStreamImpl<>(topology, name, sourceNodes, keyType, valueType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V1, R> KTable<K, R> join(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner) {
        KTableImpl<K, ?, V1> otherTable = (KTableImpl<K, ?, V1>) other;

        Set<String> allSourceNodes = ensureJoinableWith(otherTable);

        String joinThisName = topology.newName(JOINTHIS_NAME);
        String joinOtherName = topology.newName(JOINOTHER_NAME);
        String joinMergeName = topology.newName(MERGE_NAME);

        KTableKTableJoin<K, R, V, V1> joinThis = new KTableKTableJoin<>(this, otherTable, joiner);
        KTableKTableJoin<K, R, V1, V> joinOther = new KTableKTableJoin<>(otherTable, this, reverseJoiner(joiner));
        KTableKTableJoinMerger<K, R> joinMerge = new KTableKTableJoinMerger<>(
                new KTableImpl<K, V, R>(topology, joinThisName, joinThis,
                        this.sourceNodes, this.keyType, this.valueType),
                new KTableImpl<K, V1, R>(topology, joinOtherName, joinOther,
                        otherTable.sourceNodes, otherTable.keyType, otherTable.valueType)
        );

        topology.addProcessor(joinThisName, joinThis, this.name);
        topology.addProcessor(joinOtherName, joinOther, otherTable.name);
        topology.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);

        return new KTableImpl<>(topology, joinMergeName, joinMerge, allSourceNodes, keyType, resolveReturnType(joiner));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V1, R> KTable<K, R> outerJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner) {
        KTableImpl<K, ?, V1> otherTable = (KTableImpl<K, ?, V1>) other;

        Set<String> allSourceNodes = ensureJoinableWith(otherTable);

        String joinThisName = topology.newName(OUTERTHIS_NAME);
        String joinOtherName = topology.newName(OUTEROTHER_NAME);
        String joinMergeName = topology.newName(MERGE_NAME);

        KTableKTableOuterJoin<K, R, V, V1> joinThis = new KTableKTableOuterJoin<>(this, otherTable, joiner);
        KTableKTableOuterJoin<K, R, V1, V> joinOther = new KTableKTableOuterJoin<>(otherTable, this, reverseJoiner(joiner));
        KTableKTableJoinMerger<K, R> joinMerge = new KTableKTableJoinMerger<>(
                new KTableImpl<K, V, R>(topology, joinThisName, joinThis,
                        this.sourceNodes, this.keyType, this.valueType),
                new KTableImpl<K, V1, R>(topology, joinOtherName, joinOther,
                        otherTable.sourceNodes, otherTable.keyType, otherTable.valueType)
        );

        topology.addProcessor(joinThisName, joinThis, this.name);
        topology.addProcessor(joinOtherName, joinOther, otherTable.name);
        topology.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);

        return new KTableImpl<>(topology, joinMergeName, joinMerge, allSourceNodes, keyType, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V1, R> KTable<K, R> leftJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner) {
        KTableImpl<K, ?, V1> otherTable = (KTableImpl<K, ?, V1>) other;

        Set<String> allSourceNodes = ensureJoinableWith(otherTable);

        String joinThisName = topology.newName(LEFTTHIS_NAME);
        String joinOtherName = topology.newName(LEFTOTHER_NAME);
        String joinMergeName = topology.newName(MERGE_NAME);

        KTableKTableLeftJoin<K, R, V, V1> joinThis = new KTableKTableLeftJoin<>(this, otherTable, joiner);
        KTableKTableRightJoin<K, R, V1, V> joinOther = new KTableKTableRightJoin<>(otherTable, this, reverseJoiner(joiner));
        KTableKTableJoinMerger<K, R> joinMerge = new KTableKTableJoinMerger<>(
                new KTableImpl<K, V, R>(topology, joinThisName, joinThis,
                        this.sourceNodes, this.keyType, this.valueType),
                new KTableImpl<K, V1, R>(topology, joinOtherName, joinOther,
                        otherTable.sourceNodes, otherTable.keyType, otherTable.valueType)
        );

        topology.addProcessor(joinThisName, joinThis, this.name);
        topology.addProcessor(joinOtherName, joinOther, otherTable.name);
        topology.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);

        return new KTableImpl<>(topology, joinMergeName, joinMerge, allSourceNodes, keyType, resolveReturnType(joiner));
    }

    @Override
    public <K1, V1, T> KTable<K1, T> aggregate(final Initializer<T> initializer,
                                               final Aggregator<K1, V1, T> add,
                                               final Aggregator<K1, V1, T> remove,
                                               final KeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                               String name) {

        Type mappedType = resolveReturnType(selector);
        final Type selectKeyType = getKeyTypeFromKeyValueType(mappedType);
        final Type selectValueType = getValueTypeFromKeyValueType(mappedType);

        final KTableImpl<K, S, V> self = this;
        final String storeName = name;

        KTable<K1, T> lazyKTable = new LazyKTableWrapper<K1, T>(topology, selectKeyType, null) {
            @Override
            protected KTable<K1, T> create(Type aggKeyType, Type aggValueType) {

                Serializer<K1> keySerializer = getSerializer(selectKeyType);
                Serializer<V1> valueSerializer = getSerializer(selectValueType);
                Serializer<T> aggValueSerializer = getSerializer(aggValueType);
                Deserializer<K1> keyDeserializer = getDeserializer(selectKeyType);
                Deserializer<V1> valueDeserializer = getDeserializer(selectValueType);
                Deserializer<T> aggValueDeserializer = getDeserializer(aggValueType);

                String selectName = topology.newName(SELECT_NAME);
                String sinkName = topology.newName(KStreamImpl.SINK_NAME);
                String sourceName = topology.newName(KStreamImpl.SOURCE_NAME);
                String aggregateName = topology.newName(AGGREGATE_NAME);

                String topic = name + REPARTITION_TOPIC_SUFFIX;

                ChangedSerializer<V1> changedValueSerializer = new ChangedSerializer<>(valueSerializer);
                ChangedDeserializer<V1> changedValueDeserializer = new ChangedDeserializer<>(valueDeserializer);

                KTableProcessorSupplier<K, V, KeyValue<K1, V1>> selectSupplier = new KTableRepartitionMap<>(self, selector);

                ProcessorSupplier<K1, Change<V1>> aggregateSupplier = new KTableAggregate<>(storeName, initializer, add, remove);

                StateStoreSupplier aggregateStore = Stores.create(storeName)
                        .withKeys(keySerializer, keyDeserializer)
                        .withValues(aggValueSerializer, aggValueDeserializer)
                        .persistent()
                        .build();

                // select the aggregate key and values (old and new), it would require parent to send old values
                topology.addProcessor(selectName, selectSupplier, self.name);
                self.enableSendingOldValues();

                // send the aggregate key-value pairs to the intermediate topic for partitioning
                topology.addInternalTopic(topic);
                topology.addSink(sinkName, topic, keySerializer, changedValueSerializer, selectName);

                // read the intermediate topic
                topology.addSource(sourceName, keyDeserializer, changedValueDeserializer, topic);

                // aggregate the values with the aggregator and local store
                topology.addProcessor(aggregateName, aggregateSupplier, sourceName);
                topology.addStateStore(aggregateStore, aggregateName);

                // return the KTable representation with the intermediate topic as the sources
                return new KTableImpl<>(topology, aggregateName, aggregateSupplier, Collections.singleton(sourceName), aggKeyType, aggValueType);
            }
        };

        Type aggValueType = ensureConsistentTypes(selectValueType, resolveReturnType(add), resolveReturnType(remove));
        if (aggValueType != null) {
            return lazyKTable.returnsValue(aggValueType);
        } else {
            return lazyKTable;
        }
    }

    @Override
    public <K1, V1> KTable<K1, V1> reduce(final Reducer<V1> addReducer,
                                          final Reducer<V1> removeReducer,
                                          final KeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                          final String name) {

        Type addType = resolveReturnType(addReducer);
        Type removeType = resolveReturnType(removeReducer);
        Type mappedType = resolveReturnType(selector);
        final Type selectKeyType = getKeyTypeFromKeyValueType(mappedType);
        Type selectValueType = getValueTypeFromKeyValueType(mappedType);

        final KTableImpl<K, S, V> self = this;
        final String storeName = name;

        KTable<K1, V1> lazyKTable = new LazyKTableWrapper<K1, V1>(topology, selectKeyType, null) {
            @Override
            protected KTable<K1, V1> create(Type aggKeyType, Type aggValueType) {
                Serializer<K1> keySerializer = getSerializer(selectKeyType);
                Serializer<V1> valueSerializer = getSerializer(aggValueType);
                Deserializer<K1> keyDeserializer = getDeserializer(selectKeyType);
                Deserializer<V1> valueDeserializer = getDeserializer(aggValueType);

                String selectName = topology.newName(SELECT_NAME);
                String sinkName = topology.newName(KStreamImpl.SINK_NAME);
                String sourceName = topology.newName(KStreamImpl.SOURCE_NAME);
                String reduceName = topology.newName(REDUCE_NAME);

                String topic = name + REPARTITION_TOPIC_SUFFIX;

                ChangedSerializer<V1> changedValueSerializer = new ChangedSerializer<>(valueSerializer);
                ChangedDeserializer<V1> changedValueDeserializer = new ChangedDeserializer<>(valueDeserializer);

                KTableProcessorSupplier<K, V, KeyValue<K1, V1>> selectSupplier = new KTableRepartitionMap<>(self, selector);

                ProcessorSupplier<K1, Change<V1>> aggregateSupplier = new KTableReduce<>(storeName, addReducer, removeReducer);

                StateStoreSupplier aggregateStore = Stores.create(storeName)
                        .withKeys(keySerializer, keyDeserializer)
                        .withValues(valueSerializer, valueDeserializer)
                        .persistent()
                        .build();

                // select the aggregate key and values (old and new), it would require parent to send old values
                topology.addProcessor(selectName, selectSupplier, self.name);
                self.enableSendingOldValues();

                // send the aggregate key-value pairs to the intermediate topic for partitioning
                topology.addInternalTopic(topic);
                topology.addSink(sinkName, topic, keySerializer, changedValueSerializer, selectName);

                // read the intermediate topic
                topology.addSource(sourceName, keyDeserializer, changedValueDeserializer, topic);

                // aggregate the values with the aggregator and local store
                topology.addProcessor(reduceName, aggregateSupplier, sourceName);
                topology.addStateStore(aggregateStore, reduceName);

                // return the KTable representation with the intermediate topic as the sources
                return new KTableImpl<>(topology, reduceName, aggregateSupplier, Collections.singleton(sourceName), aggKeyType, aggValueType);
            }
        };

        Type aggValueType = ensureConsistentTypes(addType, removeType, selectValueType);
        if (aggValueType != null) {
            return lazyKTable.returnsValue(aggValueType);
        } else {
            return lazyKTable;
        }
    }

    public <K1, V1> KTable<K1, Long> count(KeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                           String name) {
        return this.aggregate(
                new Initializer<Long>() {
                    @Override
                    public Long apply() {
                        return 0L;
                    }
                },
                new Aggregator<K1, V1, Long>() {
                    @Override
                    public Long apply(K1 aggKey, V1 value, Long aggregate) {
                        return aggregate + 1L;
                    }
                }, new Aggregator<K1, V1, Long>() {
                    @Override
                    public Long apply(K1 aggKey, V1 value, Long aggregate) {
                        return aggregate - 1L;
                    }
                },
                selector,
                name);
    }


    @SuppressWarnings("unchecked")
    KTableValueGetterSupplier<K, V> valueGetterSupplier() {
        if (processorSupplier instanceof KTableSource) {
            KTableSource<K, V> source = (KTableSource<K, V>) processorSupplier;
            materialize(source);
            return new KTableSourceValueGetterSupplier<>(source.topic);
        } else {
            return ((KTableProcessorSupplier<K, S, V>) processorSupplier).view();
        }
    }

    @SuppressWarnings("unchecked")
    void enableSendingOldValues() {
        if (!sendOldValues) {
            if (processorSupplier instanceof KTableSource) {
                KTableSource<K, ?> source = (KTableSource<K, V>) processorSupplier;
                materialize(source);
                source.enableSendingOldValues();
            } else {
                ((KTableProcessorSupplier<K, S, V>) processorSupplier).enableSendingOldValues();
            }
            sendOldValues = true;
        }
    }

    boolean sendingOldValueEnabled() {
        return sendOldValues;
    }

    private void materialize(KTableSource<K, ?> source) {
        synchronized (source) {
            if (!source.isMaterialized()) {
                Serializer<K> keySerializer = getSerializer(keyType);
                Serializer<V> valSerializer = getSerializer(valueType);
                Deserializer<K> keyDeserializer = getDeserializer(keyType);
                Deserializer<V> valDeserializer = getDeserializer(valueType);

                StateStoreSupplier storeSupplier =
                        new KTableStoreSupplier<>(source.topic, keySerializer, keyDeserializer, valSerializer, valDeserializer, null);
                // mark this state as non internal hence it is read directly from a user topic
                topology.addStateStore(storeSupplier, false, name);
                source.materialize();
            }
        }
    }
}
