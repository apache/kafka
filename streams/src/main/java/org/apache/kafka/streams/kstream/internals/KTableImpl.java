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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.StateStoreSupplier;

import java.util.Set;

/**
 * The implementation class of KTable
 * @param <K> the key type
 * @param <S> the source's (parent's) value type
 * @param <V> the value type
 */
public class KTableImpl<K, S, V> extends AbstractStream<K> implements KTable<K, V> {

    private static final String FILTER_NAME = "KTABLE-FILTER-";

    private static final String MAPVALUES_NAME = "KTABLE-MAPVALUES-";

    private static final String TOSTREAM_NAME = "KTABLE-TOSTREAM-";

    public static final String SOURCE_NAME = "KTABLE-SOURCE-";

    public static final String SINK_NAME = "KTABLE-SINK-";

    public static final String JOINTHIS_NAME = "KTABLE-JOINTHIS-";

    public static final String JOINOTHER_NAME = "KTABLE-JOINOTHER-";

    public static final String OUTERTHIS_NAME = "KTABLE-OUTERTHIS-";

    public static final String OUTEROTHER_NAME = "KTABLE-OUTEROTHER-";

    public static final String LEFTTHIS_NAME = "KTABLE-LEFTTHIS-";
    public static final String LEFTOTHER_NAME = "KTABLE-LEFTOTHER-";

    public static final String MERGE_NAME = "KTABLE-MERGE-";

    public final KTableProcessorSupplier<K, S, V> processorSupplier;

    private final Serializer<K> keySerializer;
    private final Serializer<V> valSerializer;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valDeserializer;

    public KTableImpl(KStreamBuilder topology,
                      String name,
                      KTableProcessorSupplier<K, S, V> processorSupplier,
                      Set<String> sourceNodes) {
        this(topology, name, processorSupplier, sourceNodes, null, null, null, null);
    }

    public KTableImpl(KStreamBuilder topology,
                      String name,
                      KTableProcessorSupplier<K, S, V> processorSupplier,
                      Set<String> sourceNodes,
                      Serializer<K> keySerializer,
                      Serializer<V> valSerializer,
                      Deserializer<K> keyDeserializer,
                      Deserializer<V> valDeserializer) {
        super(topology, name, sourceNodes);
        this.processorSupplier = processorSupplier;
        this.keySerializer = keySerializer;
        this.valSerializer = valSerializer;
        this.keyDeserializer = keyDeserializer;
        this.valDeserializer = valDeserializer;
    }

    @Override
    public KTable<K, V> filter(Predicate<K, V> predicate) {
        String name = topology.newName(FILTER_NAME);
        KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(this, predicate, false);
        topology.addProcessor(name, processorSupplier, this.name);

        return new KTableImpl<>(topology, name, processorSupplier, sourceNodes);
    }

    @Override
    public KTable<K, V> filterOut(final Predicate<K, V> predicate) {
        String name = topology.newName(FILTER_NAME);
        KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(this, predicate, true);

        topology.addProcessor(name, processorSupplier, this.name);

        return new KTableImpl<>(topology, name, processorSupplier, sourceNodes);
    }

    @Override
    public <V1> KTable<K, V1> mapValues(ValueMapper<V, V1> mapper) {
        String name = topology.newName(MAPVALUES_NAME);
        KTableProcessorSupplier<K, V, V1> processorSupplier = new KTableMapValues<>(this, mapper);

        topology.addProcessor(name, processorSupplier, this.name);

        return new KTableImpl<>(topology, name, processorSupplier, sourceNodes);
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
        String name = topology.newName(SINK_NAME);

        topology.addSink(name, topic, keySerializer, valSerializer, this.name);
    }

    @Override
    public KStream<K, V> toStream() {
        String name = topology.newName(TOSTREAM_NAME);

        topology.addProcessor(name, new KStreamPassThrough(), this.name);

        return new KStreamImpl<>(topology, name, sourceNodes);
    }

    KTableValueGetterSupplier<K, V> valueGetterSupplier() {
        if (processorSupplier instanceof KTableSource) {
            final KTableSource<K, V> source = (KTableSource<K, V>) processorSupplier;
            synchronized (source) {
                if (!source.isMaterialized()) {
                    StateStoreSupplier storeSupplier =
                        new KTableStoreSupplier<>(source.topic, keySerializer, keyDeserializer, valSerializer, valDeserializer, null);
                    // mark this state is non internal hence it is read directly from a user topic
                    topology.addStateStore(storeSupplier, false, name);
                    source.materialize();
                }
            }
            return new KTableSourceValueGetterSupplier<>(source.topic);
        } else {
            return processorSupplier.view();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V1, R> KTable<K, R> join(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner) {
        Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K>) other);

        String joinThisName = topology.newName(JOINTHIS_NAME);
        String joinOtherName = topology.newName(JOINOTHER_NAME);
        String joinMergeName = topology.newName(MERGE_NAME);

        KTableKTableJoin<K, R, V, V1> joinThis = new KTableKTableJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
        KTableKTableJoin<K, R, V1, V> joinOther = new KTableKTableJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        KTableMerge<K, R> joinMerge = new KTableMerge<>(
                new KTableImpl<K, V, R>(topology, joinThisName, null, this.sourceNodes),
                new KTableImpl<K, V1, R>(topology, joinOtherName, null, ((KTableImpl<K, ?, ?>) other).sourceNodes)
        );

        topology.addProcessor(joinThisName, joinThis, this.name);
        topology.addProcessor(joinOtherName, joinOther, ((KTableImpl) other).name);
        topology.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);

        return new KTableImpl<>(topology, joinMergeName, joinThis, allSourceNodes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V1, R> KTable<K, R> outerJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner) {
        Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K>) other);

        String joinThisName = topology.newName(OUTERTHIS_NAME);
        String joinOtherName = topology.newName(OUTEROTHER_NAME);
        String joinMergeName = topology.newName(MERGE_NAME);

        KTableKTableOuterJoin<K, R, V, V1> joinThis = new KTableKTableOuterJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
        KTableKTableOuterJoin<K, R, V1, V> joinOther = new KTableKTableOuterJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        KTableMerge<K, R> joinMerge = new KTableMerge<>(
                new KTableImpl<K, V, R>(topology, joinThisName, null, this.sourceNodes),
                new KTableImpl<K, V1, R>(topology, joinOtherName, null, ((KTableImpl<K, ?, ?>) other).sourceNodes)
        );

        topology.addProcessor(joinThisName, joinThis, this.name);
        topology.addProcessor(joinOtherName, joinOther, ((KTableImpl) other).name);
        topology.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);

        return new KTableImpl<>(topology, joinMergeName, joinThis, allSourceNodes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V1, R> KTable<K, R> leftJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner) {
        Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K>) other);

        String joinThisName = topology.newName(LEFTTHIS_NAME);
        String joinOtherName = topology.newName(LEFTOTHER_NAME);
        String joinMergeName = topology.newName(MERGE_NAME);

        KTableKTableLeftJoin<K, R, V, V1> joinThis = new KTableKTableLeftJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
        KTableKTableRightJoin<K, R, V1, V> joinOther = new KTableKTableRightJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        KTableMerge<K, R> joinMerge = new KTableMerge<>(
                new KTableImpl<K, V, R>(topology, joinThisName, null, this.sourceNodes),
                new KTableImpl<K, V1, R>(topology, joinOtherName, null, ((KTableImpl<K, ?, ?>) other).sourceNodes)
        );

        topology.addProcessor(joinThisName, joinThis, this.name);
        topology.addProcessor(joinOtherName, joinOther, ((KTableImpl) other).name);
        topology.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);

        return new KTableImpl<>(topology, joinMergeName, joinThis, allSourceNodes);
    }

}
