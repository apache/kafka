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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

class StreamSinkNode<K, V> extends StreamsGraphNode {

    private final String topic;
    private final ProducedInternal<K, V> producedInternal;

    StreamSinkNode(final String parentProcessorNodeName,
                   final String processorNodeName,
                   final String topic,
                   final ProducedInternal<K, V> producedInternal) {

        super(parentProcessorNodeName,
              processorNodeName,
              false);

        this.topic = topic;
        this.producedInternal = producedInternal;
    }

    String topic() {
        return topic;
    }

    Serde<K> keySerde() {
        return producedInternal.keySerde();
    }

    Serializer<K> keySerializer() {
        return producedInternal.keySerde() != null ? producedInternal.keySerde().serializer() : null;
    }

    Serde<V> valueSerde() {
        return producedInternal.valueSerde();
    }

    Serializer<V> valueSerializer() {
        return producedInternal.valueSerde() != null ? producedInternal.valueSerde().serializer() : null;
    }

    StreamPartitioner<? super K, ? super V> streamPartitioner() {
        return producedInternal.streamPartitioner();
    }

    @Override
    void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        //TODO will implement in follow-up pr
    }

}
