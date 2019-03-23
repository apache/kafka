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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.internals.ProducedInternal;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.kstream.internals.WindowedStreamPartitioner;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

public class StreamSinkNode<K, V> extends StreamsGraphNode {

    private final TopicNameExtractor<K, V> topicNameExtractor;
    private final ProducedInternal<K, V> producedInternal;

    public StreamSinkNode(final String nodeName,
                          final TopicNameExtractor<K, V> topicNameExtractor,
                          final ProducedInternal<K, V> producedInternal) {

        super(nodeName);

        this.topicNameExtractor = topicNameExtractor;
        this.producedInternal = producedInternal;
    }


    @Override
    public String toString() {
        return "StreamSinkNode{" +
               "topicNameExtractor=" + topicNameExtractor +
               ", producedInternal=" + producedInternal +
               "} " + super.toString();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        final Serializer<K> keySerializer = producedInternal.keySerde() == null ? null : producedInternal.keySerde().serializer();
        final Serializer<V> valSerializer = producedInternal.valueSerde() == null ? null : producedInternal.valueSerde().serializer();
        final StreamPartitioner<? super K, ? super V> partitioner = producedInternal.streamPartitioner();
        final String[] parentNames = parentNodeNames();

        if (partitioner == null && keySerializer instanceof WindowedSerializer) {
            @SuppressWarnings("unchecked")
            final StreamPartitioner<K, V> windowedPartitioner = (StreamPartitioner<K, V>) new WindowedStreamPartitioner<Object, V>((WindowedSerializer) keySerializer);
            topologyBuilder.addSink(nodeName(), topicNameExtractor, keySerializer, valSerializer, windowedPartitioner, parentNames);
        } else {
            topologyBuilder.addSink(nodeName(), topicNameExtractor, keySerializer, valSerializer, partitioner,  parentNames);
        }
    }

}
