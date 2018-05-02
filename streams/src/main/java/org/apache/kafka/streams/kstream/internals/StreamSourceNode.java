/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

class StreamSourceNode<K, V> extends StreamsGraphNode {

    private Collection<String> topics;
    private Pattern topicPattern;
    private final ConsumedInternal<K, V> consumedInternal;


    StreamSourceNode(final String parentProcessorNodeName,
                     final String processorNodeName,
                     final Collection<String> topics,
                     final ConsumedInternal<K, V> consumedInternal) {
        super(parentProcessorNodeName,
              processorNodeName,
              false);

        this.topics = topics;
        this.consumedInternal = consumedInternal;
    }

    StreamSourceNode(final String parentProcessorNodeName,
                     final String processorNodeName,
                     final Pattern topicPattern,
                     final ConsumedInternal<K, V> consumedInternal) {

        super(parentProcessorNodeName,
              processorNodeName,
              false);

        this.topicPattern = topicPattern;
        this.consumedInternal = consumedInternal;
    }

    List<String> getTopics() {
        return new ArrayList<>(topics);
    }

    Pattern getTopicPattern() {
        return topicPattern;
    }

    Serde<K> keySerde() {
        return consumedInternal.keySerde();
    }

    Deserializer<K> keyDeserializer() {
        return consumedInternal.keySerde() != null ? consumedInternal.keySerde().deserializer() : null;
    }

    TimestampExtractor timestampExtractor() {
        return consumedInternal.timestampExtractor();
    }

    Topology.AutoOffsetReset autoOffsetReset() {
        return consumedInternal.offsetResetPolicy();
    }

    @Override
    void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        //TODO will implement in follow-up pr
    }

}
