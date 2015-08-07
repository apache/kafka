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

package org.apache.kafka.stream.topology;

import org.apache.kafka.clients.processor.PTopology;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.stream.KStream;
import org.apache.kafka.stream.topology.internals.KStreamSource;
import org.apache.kafka.stream.topology.internals.SourceProcessor;

/**
 * KStreamTopology is the class that allows an implementation of {@link KStreamTopology#build()} to create KStream instances.
 */
public abstract class KStreamTopology {

    private PTopology topology = new PTopology();

    /**
     * Initializes a stream processing topology. This method may be called multiple times.
     * An application constructs a processing logic using KStream API.
     * <p>
     * For example,
     * </p>
     * <pre>
     *   KStreamTopology topology = new KStreamTopology() {
     *     public void build() {
     *       KStream&lt;Integer, PageView&gt; pageViewStream = from("pageView").mapValues(...);
     *       KStream&lt;Integer, AdClick&gt; adClickStream = from("adClick").join(pageViewStream, ...).process(...);
     *     }
     *   }
     *
     *   KafkaStreaming streaming = new KafkaStreaming(build, streamingConfig)
     *   streaming.run();
     * </pre>
     */
    public abstract void build();

    /**
     * Creates a KStream instance for the specified topics. The stream is added to the default synchronization group.
     *
     * @param topics the topic names, if empty default to all the topics in the config
     * @return KStream
     */
    public KStream<?, ?> from(String... topics) {
        return from(null, null, topics);
    }

    /**
     * Creates a KStream instance for the specified topic. The stream is added to the default synchronization group.
     *
     * @param keyDeserializer key deserializer used to read this source KStream,
     *                        if not specified the default deserializer defined in the configs will be used
     * @param valDeserializer value deserializer used to read this source KStream,
     *                        if not specified the default deserializer defined in the configs will be used
     * @param topics          the topic names, if empty default to all the topics in the config
     * @return KStream
     */
    public <K, V> KStream<K, V> from(Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String... topics) {

        SourceProcessor<K, V> source = new SourceProcessor<>("KAFKA-SOURCE");

        topology.addProcessor(source, keyDeserializer, valDeserializer, topics);

        return new KStreamSource<>(topology, source);
    }

    public PTopology get() {
        this.topology.build();

        return this.topology;
    }
}
