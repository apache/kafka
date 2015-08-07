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

package org.apache.kafka.stream.topology.internals;

import org.apache.kafka.clients.processor.KafkaProcessor;
import org.apache.kafka.clients.processor.PTopology;
import org.apache.kafka.clients.processor.Processor;
import org.apache.kafka.clients.processor.ProcessorContext;
import org.apache.kafka.clients.processor.internals.KafkaSource;
import org.apache.kafka.stream.KStreamContext;
import org.apache.kafka.stream.KStream;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.stream.topology.KStreamWindowed;
import org.apache.kafka.stream.topology.KeyValueMapper;
import org.apache.kafka.stream.topology.Predicate;
import org.apache.kafka.stream.topology.ValueMapper;
import org.apache.kafka.stream.topology.Window;

public class KStreamImpl<K, V> implements KStream<K, V> {

    private static final String PROCESSOR_NAME = "KAFKA-PROCESS";

    protected PTopology topology;
    protected KafkaProcessor<?, ?, K, V> processor;
    protected KStreamContext context;
    protected KStreamMetadata metadata;

    public KStreamImpl(PTopology topology, KafkaProcessor<?, ?, K, V> processor) {
        this.topology = topology;
        this.processor = processor;
    }

    @SuppressWarnings("unchecked")
    @Override
    public KStream<K, V> filter(Predicate<K, V> predicate) {
        KStreamFilter<K, V> filter = new KStreamFilter<>(predicate);

        topology.addProcessor(filter, processor);

        return new KStreamImpl<>(topology, filter);
    }

    @SuppressWarnings("unchecked")
    @Override
    public KStream<K, V> filterOut(final Predicate<K, V> predicate) {
        KStreamFilter<K, V> filter = new KStreamFilter<>(predicate, true);

        topology.addProcessor(filter, processor);

        return new KStreamImpl<>(topology, filter);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K1, V1> KStream<K1, V1> map(KeyValueMapper<K, V, K1, V1> mapper) {
        KStreamMap<K, V, K1, V1> map = new KStreamMap<>(mapper);

        topology.addProcessor(map, processor);

        return new KStreamImpl<>(topology, map);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V1> KStream<K, V1> mapValues(ValueMapper<V, V1> mapper) {
        KStreamMapValues<K, V, V1> map = new KStreamMapValues<>(mapper);

        topology.addProcessor(map, processor);

        return new KStreamImpl<>(topology, map);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K, V, K1, ? extends Iterable<V1>> mapper) {
        KStreamFlatMap<K, V, K1, V1> map = new KStreamFlatMap<>(mapper);

        topology.addProcessor(map, processor);

        return new KStreamImpl<>(topology, map);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V1> KStream<K, V1> flatMapValues(ValueMapper<V, ? extends Iterable<V1>> mapper) {
        KStreamFlatMapValues<K, V, V1> map = new KStreamFlatMapValues<>(mapper);

        topology.addProcessor(map, processor);

        return new KStreamImpl<>(topology, map);
    }

    @Override
    public KStreamWindowed<K, V> with(Window<K, V> window) {
        return (KStreamWindowed<K, V>) chain(new KStreamWindowedImpl<>(window, topology));
    }

    @Override
    public KStream<K, V>[] branch(Predicate<K, V>... predicates) {
        KStreamBranch<K, V> branch = new KStreamBranch<>(predicates, topology, processor);
        return branch.branches();
    }

    @SuppressWarnings("unchecked")
    @Override
    public KStream<K, V> through(String topic) {
        return through(topic, null, null, null, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K1, V1> KStream<K1, V1> through(String topic,
                                            Serializer<K> keySerializer,
                                            Serializer<V> valSerializer,
                                            Deserializer<K1> keyDeserializer,
                                            Deserializer<V1> valDeserializer) {
        process(this.getSendProcessor(topic, keySerializer, valSerializer));

        KafkaSource<K1, V1> source = topology.addSource(keyDeserializer, valDeserializer, topic);

        return new KStreamSource<>(topology, source);
    }

    @Override
    public void sendTo(String topic) {
        process(this.<K, V>getSendProcessor(topic, null, null));
    }

    @Override
    public void sendTo(String topic, Serializer<K> keySerializer, Serializer<V> valSerializer) {
        process(this.getSendProcessor(topic, keySerializer, valSerializer));
    }

    @SuppressWarnings("unchecked")
    private <K1, V1> Processor<K1, V1> getSendProcessor(final String sendTopic, final Serializer<K> keySerializer, final Serializer<V> valSerializer) {
        return new Processor<K1, V1>() {
            private ProcessorContext context;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
            }

            @Override
            public void process(K1 key, V1 value) {
                this.context.send(sendTopic, key, value, (Serializer<Object>) keySerializer, (Serializer<Object>) valSerializer);
            }

            @Override
            public void close() {
            }
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K1, V1> KStream<K1, V1> process(final KafkaProcessor<K, V, K1, V1> current) {
        topology.addProcessor(current, processor);

        return new KStreamImpl(topology, current);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(final Processor<K, V> current) {
        KafkaProcessor<K, V, ?, ?> wrapper = new KafkaProcessor<K, V, Object, Object>(PROCESSOR_NAME) {
            @Override
            public void process(K key, V value) {
                current.process(key, value);
            }
        };

        topology.addProcessor(wrapper, processor);
    }
}
