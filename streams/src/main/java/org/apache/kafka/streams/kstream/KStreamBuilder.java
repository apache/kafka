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

package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KStreamBuilder is the class to create KStream instances.
 */
public class KStreamBuilder extends TopologyBuilder {

    private final AtomicInteger index = new AtomicInteger(0);

    public KStreamBuilder() {
        super();
    }

    /**
     * Creates a KStream instance for the specified topic.
     * The default deserializers specified in the config are used.
     *
     * @param topics          the topic names, if empty default to all the topics in the config
     * @return KStream
     */
    public <K, V> KStream<K, V> from(String... topics) {
        return from(null, null, topics);
    }

    /**
     * Creates a KStream instance for the specified topic.
     *
     * @param keyDeserializer key deserializer used to read this source KStream,
     *                        if not specified the default deserializer defined in the configs will be used
     * @param valDeserializer value deserializer used to read this source KStream,
     *                        if not specified the default deserializer defined in the configs will be used
     * @param topics          the topic names, if empty default to all the topics in the config
     * @return KStream
     */
    public <K, V> KStream<K, V> from(Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String... topics) {
        String name = newName(KStreamImpl.SOURCE_NAME);

        addSource(name, keyDeserializer, valDeserializer, topics);

        return new KStreamImpl<>(this, name, Collections.singleton(name));
    }

    /**
     * Creates a new stream by merging the given streams
     *
     * @param streams the streams to be merged
     * @return KStream
     */
    public <K, V> KStream<K, V> merge(KStream<K, V>... streams) {
        return KStreamImpl.merge(this, streams);
    }

    public String newName(String prefix) {
        return prefix + String.format("%010d", index.getAndIncrement());
    }
}
