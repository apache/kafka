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

package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

public class StreamSourceNode<K, V> extends StreamsGraphNode {

    private Collection<String> topicNames;
    private Pattern topicPattern;
    private final ConsumedInternal<K, V> consumedInternal;


    public StreamSourceNode(final String nodeName,
                            final Collection<String> topicNames,
                            final ConsumedInternal<K, V> consumedInternal) {
        super(nodeName);

        this.topicNames = topicNames;
        this.consumedInternal = consumedInternal;
    }

    public StreamSourceNode(final String nodeName,
                            final Pattern topicPattern,
                            final ConsumedInternal<K, V> consumedInternal) {

        super(nodeName);

        this.topicPattern = topicPattern;
        this.consumedInternal = consumedInternal;
    }

    public Collection<String> getTopicNames() {
        return new ArrayList<>(topicNames);
    }

    public Pattern topicPattern() {
        return topicPattern;
    }

    public ConsumedInternal<K, V> consumedInternal() {
        return consumedInternal;
    }

    public Serde<K> keySerde() {
        return consumedInternal.keySerde();
    }

    public Serde<V> valueSerde() {
        return consumedInternal.valueSerde();
    }

    @Override
    public String toString() {
        return "StreamSourceNode{" +
               "topicNames=" + topicNames +
               ", topicPattern=" + topicPattern +
               ", consumedInternal=" + consumedInternal +
               "} " + super.toString();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {

        if (topicPattern != null) {
            topologyBuilder.addSource(consumedInternal.offsetResetPolicy(),
                                      nodeName(),
                                      consumedInternal.timestampExtractor(),
                                      consumedInternal.keyDeserializer(),
                                      consumedInternal.valueDeserializer(),
                                      topicPattern);
        } else {
            topologyBuilder.addSource(consumedInternal.offsetResetPolicy(),
                                      nodeName(),
                                      consumedInternal.timestampExtractor(),
                                      consumedInternal.keyDeserializer(),
                                      consumedInternal.valueDeserializer(),
                                      topicNames.toArray(new String[topicNames.size()]));

        }
    }

}
