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

package org.apache.kafka.streaming.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streaming.kstream.KeyValueFlatMap;
import org.apache.kafka.streaming.processor.ProcessorFactory;
import org.apache.kafka.streaming.processor.TopologyBuilder;
import org.apache.kafka.streaming.kstream.KStreamWindowed;
import org.apache.kafka.streaming.kstream.KeyValueMapper;
import org.apache.kafka.streaming.kstream.Predicate;
import org.apache.kafka.streaming.kstream.KStream;
import org.apache.kafka.streaming.kstream.ValueMapper;
import org.apache.kafka.streaming.kstream.Window;

import java.util.ArrayList;
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

    private static final String BRANCHCHILD_NAME = "KAFKA-BRANCHCHILD-";

    private static final String WINDOWED_NAME = "KAFKA-WINDOWED-";

    public static final String JOIN_NAME = "KAFKA-JOIN-";

    public static final String JOINOTHER_NAME = "KAFKA-JOINOTHER-";

    public static final String SOURCE_NAME = "KAFKA-SOURCE-";

    public static final String SEND_NAME = "KAFKA-SEND-";

    public static final String WINDOW_NAME = "KAFKA-WINDOW-";

    public static final AtomicInteger INDEX = new AtomicInteger(1);

    protected TopologyBuilder topology;
    protected String name;

    public KStreamImpl(TopologyBuilder topology, String name) {
        this.topology = topology;
        this.name = name;
    }

    @Override
    public KStream<K, V> filter(Predicate<K, V> predicate) {
        String name = FILTER_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamFilter(predicate, false), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public KStream<K, V> filterOut(final Predicate<K, V> predicate) {
        String name = FILTER_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamFilter(predicate, true), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public <K1, V1> KStream<K1, V1> map(KeyValueMapper<K, V, K1, V1> mapper) {
        String name = MAP_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamMap(mapper), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public <V1> KStream<K, V1> mapValues(ValueMapper<V, V1> mapper) {
        String name = MAPVALUES_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamMapValues(mapper), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K1, V1> KStream<K1, V1> flatMap(KeyValueFlatMap<K, V, K1, V1> mapper) {
        String name = FLATMAP_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamFlatMap(mapper), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V1> KStream<K, V1> flatMapValues(ValueMapper<V, ? extends Iterable<V1>> mapper) {
        String name = FLATMAPVALUES_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamFlatMapValues(mapper), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public KStreamWindowed<K, V> with(Window<K, V> window) {
        String name = WINDOWED_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamWindow<>(window), this.name);

        return new KStreamWindowedImpl<>(topology, name, window);
    }

    @Override
    @SuppressWarnings("unchecked")
    public KStream<K, V>[] branch(Predicate<K, V>... predicates) {
        String branchName = BRANCH_NAME + INDEX.getAndIncrement();

        topology.addProcessor(branchName, new KStreamBranch(predicates.clone()), this.name);

        List<KStream<K, V>> branchChildren = new ArrayList<>();
        for (int i = 0; i < predicates.length; i++) {
            String childName = BRANCHCHILD_NAME + INDEX.getAndIncrement();

            topology.addProcessor(childName, new KStreamPassThrough(), branchName);

            branchChildren.add(new KStreamImpl<K, V>(topology, childName));
        }

        return (KStream<K, V>[]) branchChildren.toArray();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K1, V1> KStream<K1, V1> through(String topic,
                                            Serializer<K> keySerializer,
                                            Serializer<V> valSerializer,
                                            Deserializer<K1> keyDeserializer,
                                            Deserializer<V1> valDeserializer) {
        String sendName = SEND_NAME + INDEX.getAndIncrement();

        topology.addSink(sendName, topic, keySerializer, valSerializer, this.name);

        String sourceName = SOURCE_NAME + INDEX.getAndIncrement();

        topology.addSource(sourceName, keyDeserializer, valDeserializer, topic);

        return new KStreamImpl<>(topology, sourceName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void sendTo(String topic, Serializer<K> keySerializer, Serializer<V> valSerializer) {
        String name = SEND_NAME + INDEX.getAndIncrement();

        topology.addSink(name, topic, keySerializer, valSerializer, this.name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K1, V1> KStream<K1, V1> process(final ProcessorFactory processorFactory) {
        String name = PROCESSOR_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, processorFactory, this.name);

        return new KStreamImpl<>(topology, name);
    }
}
