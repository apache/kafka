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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public abstract class BaseRepartitionNode<K, V> extends StreamsGraphNode {

    protected final Serde<K> keySerde;
    protected final Serde<V> valueSerde;
    protected final String sinkName;
    protected final String sourceName;
    protected final String repartitionTopic;
    protected final ProcessorParameters processorParameters;


    BaseRepartitionNode(final String nodeName,
                        final String sourceName,
                        final ProcessorParameters processorParameters,
                        final Serde<K> keySerde,
                        final Serde<V> valueSerde,
                        final String sinkName,
                        final String repartitionTopic) {

        super(nodeName);

        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.sinkName = sinkName;
        this.sourceName = sourceName;
        this.repartitionTopic = repartitionTopic;
        this.processorParameters = processorParameters;
    }

    abstract Serializer<V> getValueSerializer();

    abstract Deserializer<V> getValueDeserializer();

    @Override
    public String toString() {
        return "BaseRepartitionNode{" +
               "keySerde=" + keySerde +
               ", valueSerde=" + valueSerde +
               ", sinkName='" + sinkName + '\'' +
               ", sourceName='" + sourceName + '\'' +
               ", repartitionTopic='" + repartitionTopic + '\'' +
               ", processorParameters=" + processorParameters +
               "} " + super.toString();
    }
}
