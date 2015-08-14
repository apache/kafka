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

package org.apache.kafka.stream.kstream.internals;

import org.apache.kafka.stream.processor.KafkaProcessor;
import org.apache.kafka.stream.processor.PConfig;
import org.apache.kafka.stream.processor.PTopologyBuilder;
import org.apache.kafka.stream.processor.ProcessorContext;
import org.apache.kafka.stream.processor.KafkaSource;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.stream.KStreamWindowed;
import org.apache.kafka.stream.KeyValueMapper;
import org.apache.kafka.stream.Predicate;
import org.apache.kafka.stream.KStream;
import org.apache.kafka.stream.ValueMapper;
import org.apache.kafka.stream.Window;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class KStreamImpl<K, V> implements KStream<K, V> {

    private static final String FILTER_NAME = "KAFKA-FILTER-";

    private static final String MAP_NAME = "KAFKA-MAP-";

    private static final String MAPVALUES_NAME = "KAFKA-MAPVALUES-";

    private static final String FLATMAP_NAME = "KAFKA-FLATMAP-";

    private static final String FLATMAPVALUES_NAME = "KAFKA-FLATMAPVALUES-";

    private static final String PROCESSOR_NAME = "KAFKA-PROCESSOR-";

    private static final String BRANCH_NAME = "KAFKA-BRANCH-";

    public static final String SOURCE_NAME = "KAFKA-SOURCE-";

    public static final AtomicInteger INDEX = new AtomicInteger(1);

    protected PTopologyBuilder topology;
    protected String name;

    public KStreamImpl(PTopologyBuilder topology, String name) {
        this.topology = topology;
        this.name = name;
    }

    @Override
    public KStream<K, V> filter(Predicate<K, V> predicate) {
        String name = FILTER_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, KStreamFilter.class, new PConfig("Predicate", new KStreamFilter.PredicateOut<>(predicate)), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public KStream<K, V> filterOut(final Predicate<K, V> predicate) {
        String name = FILTER_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, KStreamFilter.class, new PConfig("Predicate", new KStreamFilter.PredicateOut<>(predicate, true)), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public <K1, V1> KStream<K1, V1> map(KeyValueMapper<K, V, K1, V1> mapper) {
        String name = MAP_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, KStreamMap.class, new PConfig("Mapper", mapper), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public <V1> KStream<K, V1> mapValues(ValueMapper<V, V1> mapper) {
        String name = MAPVALUES_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, KStreamMapValues.class, new PConfig("ValueMapper", mapper), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K, V, K1, ? extends Iterable<V1>> mapper) {
        String name = FLATMAP_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, KStreamFlatMap.class, new PConfig("Mapper", mapper), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V1> KStream<K, V1> flatMapValues(ValueMapper<V, ? extends Iterable<V1>> mapper) {
        String name = FLATMAPVALUES_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, KStreamFlatMapValues.class, new PConfig("ValueMapper", mapper), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public KStreamWindowed<K, V> with(Window<K, V> window) {
        KStreamWindow<K, V> windowed = new KStreamWindow<>(window);

        topology.addProcessor(windowed, processor);

        return new KStreamWindow.KStreamWindowedImpl<>(topology, windowed);
    }

    @Override
    @SuppressWarnings("unchecked")
    public KStream<K, V>[] branch(Predicate<K, V>... predicates) {
        String name = BRANCH_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, KStreamBranch.class, new PConfig("Predicates", Arrays.copyOf(predicates, predicates.length)), this.name);

        KStreamImpl branch = new KStreamImpl<>(topology, name);
        List<KStream<K, V>> avatars = new ArrayList<>();
        for (int i = 0; i < predicates.length; i++) {
            avatars.add(branch);
        }

        return (KStream<K, V>[]) avatars.toArray();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K1, V1> KStream<K1, V1> through(String topic,
                                            Serializer<K> keySerializer,
                                            Serializer<V> valSerializer,
                                            Deserializer<K1> keyDeserializer,
                                            Deserializer<V1> valDeserializer) {
        process(KStreamSend.class, new PConfig("Topic-Ser", new KStreamSend.TopicSer(topic, (Serializer<Object>) keySerializer, (Serializer<Object>) valSerializer)));

        String name = SOURCE_NAME + INDEX.getAndIncrement();

        KafkaSource<K1, V1> source = topology.addSource(name, keyDeserializer, valDeserializer, topic);

        return new KStreamSource<>(topology, source);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void sendTo(String topic, Serializer<K> keySerializer, Serializer<V> valSerializer) {
        process(KStreamSend.class, new PConfig("Topic-Ser", new KStreamSend.TopicSer(topic, (Serializer<Object>) keySerializer, (Serializer<Object>) valSerializer)));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K1, V1> KStream<K1, V1> process(final Class<? extends KafkaProcessor<K, V, K1, V1>> clazz, PConfig config) {
        String name = PROCESSOR_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, clazz, config, this.name);

        return new KStreamImpl(topology, name);
    }
}
