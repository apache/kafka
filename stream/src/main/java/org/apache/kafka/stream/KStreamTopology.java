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

package org.apache.kafka.stream;

import org.apache.kafka.clients.processor.KafkaProcessor;
import org.apache.kafka.clients.processor.PTopology;
import org.apache.kafka.clients.processor.ProcessorProperties;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.stream.internals.KStreamSource;

/**
 * KStreamTopology is the class that allows an implementation of {@link KStreamTopology#build()} to create KStream instances.
 */
public abstract class KStreamTopology extends PTopology {

    public KStreamTopology() {
        super();
    }

    public KStreamTopology(ProcessorProperties properties) {
        super(properties);
    }

    /**
     * Creates a KStream instance for the specified topic. The stream is added to the default synchronization group.
     *
     * @param topics          the topic names, if empty default to all the topics in the config
     * @return KStream
     */
    public <K, V> KStream<K, V> from(String... topics) {
        if (properties == null)
            throw new KafkaException("No default deserializers specified in the config.");

        KafkaProcessor<K, V, K, V> source = addSource(
            (Deserializer<K>) properties.keyDeserializer(),
            (Deserializer<V>) properties.valueDeserializer(), topics);
        return new KStreamSource<>(this, source);
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
    public <K, V> KStream<K, V> from(Deserializer<? extends K> keyDeserializer, Deserializer<? extends V> valDeserializer, String... topics) {
        KafkaProcessor<K, V, K, V> source = addSource(keyDeserializer, valDeserializer, topics);
        return new KStreamSource<>(this, source);
    }
}
