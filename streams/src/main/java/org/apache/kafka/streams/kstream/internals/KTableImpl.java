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
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.StateStoreSupplier;

import java.util.Collections;

/**
 * The implementation class of KTable
 * @param <K> the key type
 * @param <S> the source's (parent's) value type
 * @param <V> the value type
 */
public class KTableImpl<K, S, V> implements KTable<K, V> {

    private static final String FILTER_NAME = "KTABLE-FILTER-";

    private static final String MAPVALUES_NAME = "KTABLE-MAPVALUES-";

    private static final String TOSTREAM_NAME = "KTABLE-TOSTREAM-";

    public static final String SOURCE_NAME = "KTABLE-SOURCE-";

    protected final KStreamBuilder topology;
    public final String name;
    public final KTableProcessorSupplier<K, S, V> processorSupplier;
    private final String sourceNode;

    private final KTableImpl<K, ?, S> parent;
    private final String topic;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valSerializer;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valDeserializer;

    public KTableImpl(KStreamBuilder topology,
                      String name,
                      KTableProcessorSupplier<K, S, V> processorSupplier,
                      String sourceNode,
                      KTableImpl<K, ?, S> parent) {
        this(topology, name, processorSupplier, sourceNode, null, null, null, null, null, parent);
    }

    public KTableImpl(KStreamBuilder topology,
                      String name,
                      KTableProcessorSupplier<K, S, V> processorSupplier,
                      String sourceNode,
                      String topic,
                      Serializer<K> keySerializer,
                      Serializer<V> valSerializer,
                      Deserializer<K> keyDeserializer,
                      Deserializer<V> valDeserializer) {
        this(topology, name, processorSupplier, sourceNode, topic, keySerializer, valSerializer, keyDeserializer, valDeserializer, null);
    }

    private KTableImpl(KStreamBuilder topology,
                       String name,
                       KTableProcessorSupplier<K, S, V> processorSupplier,
                       String sourceNode,
                       String topic,
                       Serializer<K> keySerializer,
                       Serializer<V> valSerializer,
                       Deserializer<K> keyDeserializer,
                       Deserializer<V> valDeserializer,
                       KTableImpl<K, ?, S> parent) {
        this.topology = topology;
        this.name = name;
        this.processorSupplier = processorSupplier;
        this.sourceNode = sourceNode;
        this.topic = topic;
        this.keySerializer = keySerializer;
        this.valSerializer = valSerializer;
        this.keyDeserializer = keyDeserializer;
        this.valDeserializer = valDeserializer;
        this.parent = parent;
    }

    @Override
    public KTable<K, V> filter(Predicate<K, V> predicate) {
        String name = topology.newName(FILTER_NAME);
        KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(predicate, false);
        topology.addProcessor(name, processorSupplier, this.name);

        return new KTableImpl<>(topology, name, processorSupplier, sourceNode, this);
    }

    @Override
    public KTable<K, V> filterOut(final Predicate<K, V> predicate) {
        String name = topology.newName(FILTER_NAME);
        KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(predicate, true);

        topology.addProcessor(name, processorSupplier, this.name);

        return new KTableImpl<>(topology, name, processorSupplier, sourceNode, this);
    }

    @Override
    public <V1> KTable<K, V1> mapValues(ValueMapper<V, V1> mapper) {
        String name = topology.newName(MAPVALUES_NAME);
        KTableProcessorSupplier<K, V, V1> processorSupplier = new KTableMapValues<>(mapper);

        topology.addProcessor(name, processorSupplier, this.name);

        return new KTableImpl<>(topology, name, processorSupplier, sourceNode, this);
    }

    @Override
    public KTable<K, V> through(String topic,
                                Serializer<K> keySerializer,
                                Serializer<V> valSerializer,
                                Deserializer<K> keyDeserializer,
                                Deserializer<V> valDeserializer) {
        String sendName = topology.newName(KStreamImpl.SINK_NAME);

        topology.addSink(sendName, topic, keySerializer, valSerializer, this.name);

        return topology.table(keySerializer, valSerializer, keyDeserializer, valDeserializer, topic);
    }

    @Override
    public KTable<K, V> through(String topic) {
        return through(topic, null, null, null, null);
    }

    @Override
    public void to(String topic) {
        String name = topology.newName(KStreamImpl.SINK_NAME);

        topology.addSink(name, topic, this.name);
    }

    @Override
    public void to(String topic, Serializer<K> keySerializer, Serializer<V> valSerializer) {
        String name = topology.newName(KStreamImpl.SINK_NAME);

        topology.addSink(name, topic, keySerializer, valSerializer, this.name);
    }

    @Override
    public KStream<K, V> toStream() {
        String name = topology.newName(TOSTREAM_NAME);

        topology.addProcessor(name, new KStreamPassThrough(), this.name);

        return new KStreamImpl<>(topology, name, Collections.singleton(sourceNode));
    }

    KTableValueGetterSupplier<K, V> valueGetterSupplier() {
        if (parent != null) {
            return processorSupplier.view(parent.valueGetterSupplier());
        } else {
            KTableSource<K, V> source = (KTableSource<K, V>) processorSupplier;
            synchronized (source) {
                if (!source.isMaterialized()) {
                    StateStoreSupplier storeSupplier =
                            new KTableStoreSupplier(topic, keySerializer, keyDeserializer, valSerializer, valDeserializer, null);
                    // mark this state is non internal hence it is read directly from a user topic
                    topology.addStateStore(storeSupplier, false, name);
                    source.materialize();
                }
            }
            return new KTableSourceValueGetterSupplier<>(topic);
        }
    }

}
