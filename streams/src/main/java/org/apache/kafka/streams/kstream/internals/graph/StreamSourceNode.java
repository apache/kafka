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

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.Collection;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamSourceNode<K, V> extends SourceGraphNode<K, V> {

    private final Logger log = LoggerFactory.getLogger(StreamSourceNode.class);

    public StreamSourceNode(final String nodeName,
                            final Collection<String> topicNames,
                            final ConsumedInternal<K, V> consumedInternal) {
        super(nodeName, topicNames, consumedInternal);
    }

    public StreamSourceNode(final String nodeName,
                            final Pattern topicPattern,
                            final ConsumedInternal<K, V> consumedInternal) {

        super(nodeName, topicPattern, consumedInternal);
    }

    public void merge(final StreamSourceNode<?, ?> other) {
        final AutoOffsetReset resetPolicy = consumedInternal().offsetResetPolicy();
        final AutoOffsetReset otherResetPolicy = other.consumedInternal().offsetResetPolicy();
        if (resetPolicy != null && !resetPolicy.equals(otherResetPolicy)
            || otherResetPolicy != null && !otherResetPolicy.equals(resetPolicy)) {
            log.error("Tried to merge source nodes {} and {} which are subscribed to the same topic/pattern, but "
                          + "the offset reset policies do not match", this, other);
            throw new TopologyException("Can't configure different offset reset policies on the same input topic(s)");
        }
        for (final GraphNode otherChild : other.children()) {
            other.removeChild(otherChild);
            addChild(otherChild);
        }
    }

    @Override
    public String toString() {
        return "StreamSourceNode{" +
               "topicNames=" + (topicNames().isPresent() ? topicNames().get() : null) +
               ", topicPattern=" + (topicPattern().isPresent() ? topicPattern().get() : null) +
               ", consumedInternal=" + consumedInternal() +
               "} " + super.toString();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {

        if (topicPattern().isPresent()) {
            topologyBuilder.addSource(consumedInternal().offsetResetPolicy(),
                                      nodeName(),
                                      consumedInternal().timestampExtractor(),
                                      consumedInternal().keyDeserializer(),
                                      consumedInternal().valueDeserializer(),
                                      topicPattern().get());
        } else {
            topologyBuilder.addSource(consumedInternal().offsetResetPolicy(),
                                      nodeName(),
                                      consumedInternal().timestampExtractor(),
                                      consumedInternal().keyDeserializer(),
                                      consumedInternal().valueDeserializer(),
                                      topicNames().get().toArray(new String[0]));
        }
    }

}
