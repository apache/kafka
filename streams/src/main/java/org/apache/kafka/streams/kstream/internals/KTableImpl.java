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
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.AggregatorSupplier;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.KeyValueToLongMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
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

    private static final String FILTER_NAME = "KTABLE-FILTER-";

    private static final String MAPVALUES_NAME = "KTABLE-MAPVALUES-";

    private static final String TOSTREAM_NAME = "KTABLE-TOSTREAM-";

    private static final String SELECT_NAME = "KTABLE-SELECT-";

    private static final String AGGREGATE_NAME = "KTABLE-AGGREGATE-";

    public static final String SOURCE_NAME = "KTABLE-SOURCE-";

    public static final String JOINTHIS_NAME = "KTABLE-JOINTHIS-";

    public static final String JOINOTHER_NAME = "KTABLE-JOINOTHER-";

    public static final String OUTERTHIS_NAME = "KTABLE-OUTERTHIS-";

    public static final String OUTEROTHER_NAME = "KTABLE-OUTEROTHER-";

    public static final String LEFTTHIS_NAME = "KTABLE-LEFTTHIS-";

    public static final String LEFTOTHER_NAME = "KTABLE-LEFTOTHER-";

    public static final String MERGE_NAME = "KTABLE-MERGE-";

    public final ProcessorSupplier<K, ?> processorSupplier;

    private final Serializer<K> keySerializer;
    private final Serializer<V> valSerializer;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valDeserializer;

    private boolean sendOldValues = false;

    public KTableImpl(KStreamBuilder topology,
                      String name,
                      ProcessorSupplier<K, ?> processorSupplier,
                      Set<String> sourceNodes,
                      Type keyType,
                      Type valueType) {
        this(topology, name, processorSupplier, sourceNodes, null, null, null, null, keyType, valueType);
    }

    public KTableImpl(KStreamBuilder topology,
                      String name,
                      ProcessorSupplier<K, ?> processorSupplier,
                      Set<String> sourceNodes,
                      Serializer<K> keySerializer,
                      Serializer<V> valSerializer,
                      Deserializer<K> keyDeserializer,
                      Deserializer<V> valDeserializer,
                      Type keyType,
                      Type valueType) {
        super(topology, name, sourceNodes, keyType, valueType);
        this.processorSupplier = processorSupplier;
        this.keySerializer = keySerializer;
        this.valSerializer = valSerializer;
        this.keyDeserializer = keyDeserializer;
        this.valDeserializer = valDeserializer;
    }

    @Override
    public KTable<K, V> returns(Type keyType, Type valueType) {
        return new KTableImpl<>(topology, name, processorSupplier, sourceNodes, keySerializer, valSerializer, keyDeserializer, valDeserializer, keyType, valueType);
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

        return new KTableImpl<>(topology, name, processorSupplier, sourceNodes, keyType, null);
    }

    @Override
    public KTable<K, V> through(String topic,
                                Serializer<K> keySerializer,
                                Serializer<V> valSerializer,
                                Deserializer<K> keyDeserializer,
                                Deserializer<V> valDeserializer) {
        to(topic, keySerializer, valSerializer);

        return topology.table(keySerializer, valSerializer, keyDeserializer, valDeserializer, topic);
    }

    @Override
    public KTable<K, V> through(String topic) {
        return through(topic, null, null, null, null);
    }

    @Override
    public void to(String topic) {
        to(topic, null, null);
    }

    @Override
    public void to(String topic, Serializer<K> keySerializer, Serializer<V> valSerializer) {
        this.toStream().to(topic, keySerializer, valSerializer);
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

        return new KTableImpl<>(topology, joinMergeName, joinMerge, allSourceNodes, keyType, null);
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

        return new KTableImpl<>(topology, joinMergeName, joinMerge, allSourceNodes, keyType, null);
    }

    @Override
    public <K1, V1, V2> KTable<K1, V2> aggregate(AggregatorSupplier<K1, V1, V2> aggregatorSupplier,
                                                 KeyValueMapper<K, V, KeyValue<K1, V1>> selector,
                                                 Serializer<K1> keySerializer,
                                                 Serializer<V1> valueSerializer,
                                                 Serializer<V2> aggValueSerializer,
                                                 Deserializer<K1> keyDeserializer,
                                                 Deserializer<V1> valueDeserializer,
                                                 Deserializer<V2> aggValueDeserializer,
                                                 String name) {
        String selectName = topology.newName(SELECT_NAME);
        String sinkName = topology.newName(KStreamImpl.SINK_NAME);
        String sourceName = topology.newName(KStreamImpl.SOURCE_NAME);
        String aggregateName = topology.newName(AGGREGATE_NAME);

        String topic = name + REPARTITION_TOPIC_SUFFIX;

        ChangedSerializer<V1> changedValueSerializer = new ChangedSerializer<>(valueSerializer);
        ChangedDeserializer<V1> changedValueDeserializer = new ChangedDeserializer<>(valueDeserializer);

        KTableProcessorSupplier<K, V, KeyValue<K1, V1>> selectSupplier = new KTableRepartitionMap<>(this, selector);

        ProcessorSupplier<K1, Change<V1>> aggregateSupplier = new KTableAggregate<>(name, aggregatorSupplier.get());

        StateStoreSupplier aggregateStore = Stores.create(name)
                .withKeys(keySerializer, keyDeserializer)
                .withValues(aggValueSerializer, aggValueDeserializer)
                .localDatabase()
                .build();

        // select the aggregate key and values (old and new), it would require parent to send old values
        topology.addProcessor(selectName, selectSupplier, this.name);
        this.enableSendingOldValues();

        // send the aggregate key-value pairs to the intermediate topic for partitioning
        topology.addInternalTopic(topic);
        topology.addSink(sinkName, topic, keySerializer, changedValueSerializer, selectName);

        // read the intermediate topic
        topology.addSource(sourceName, keyDeserializer, changedValueDeserializer, topic);

        // aggregate the values with the aggregator and local store
        topology.addProcessor(aggregateName, aggregateSupplier, sourceName);
        topology.addStateStore(aggregateStore, aggregateName);

        // return the KTable representation with the intermediate topic as the sources
        return new KTableImpl<>(topology, aggregateName, aggregateSupplier, Collections.singleton(sourceName), null, null);
    }

    @Override
    public <K1> KTable<K1, Long> sum(final KeyValueMapper<K, V, K1> keySelector,
                                     final KeyValueToLongMapper<K, V> valueSelector,
                                     Serializer<K1> keySerializer,
                                     Deserializer<K1> keyDeserializer,
                                     String name) {

        Serializer<Long> longSerializer = new LongSerializer();
        Deserializer<Long> longDeserializer = new LongDeserializer();

        KeyValueMapper<K, V, KeyValue<K1, Long>> mapper = new KeyValueMapper<K, V, KeyValue<K1, Long>>() {
            @Override
            public KeyValue<K1, Long> apply(K key, V value) {
                K1 aggKey = keySelector.apply(key, value);
                Long aggValue = valueSelector.apply(key, value);

                return new KeyValue<>(aggKey, aggValue);
            }
        };

        return this.<K1, Long, Long>aggregate(new LongSumSupplier<K1>(), mapper,
                keySerializer, longSerializer, longSerializer,
                keyDeserializer, longDeserializer, longDeserializer,
                name);
    }

    @Override
    public <K1> KTable<K1, Long> count(final KeyValueMapper<K, V, K1> keySelector,
                                       Serializer<K1> keySerializer,
                                       Serializer<V> valueSerializer,
                                       Deserializer<K1> keyDeserializer,
                                       Deserializer<V> valueDeserializer,
                                       String name) {

        Serializer<Long> longSerializer = new LongSerializer();
        Deserializer<Long> longDeserializer = new LongDeserializer();

        KeyValueMapper<K, V, KeyValue<K1, V>> mapper = new KeyValueMapper<K, V, KeyValue<K1, V>>() {
            @Override
            public KeyValue<K1, V> apply(K key, V value) {
                K1 aggKey = keySelector.apply(key, value);

                return new KeyValue<>(aggKey, value);
            }
        };

        return this.<K1, V, Long>aggregate(new CountSupplier<K1, V>(), mapper,
                keySerializer, valueSerializer, longSerializer,
                keyDeserializer, valueDeserializer, longDeserializer,
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
                StateStoreSupplier storeSupplier =
                        new KTableStoreSupplier<>(source.topic, keySerializer, keyDeserializer, valSerializer, valDeserializer, null);
                // mark this state as non internal hence it is read directly from a user topic
                topology.addStateStore(storeSupplier, false, name);
                source.materialize();
            }
        }
    }
}
