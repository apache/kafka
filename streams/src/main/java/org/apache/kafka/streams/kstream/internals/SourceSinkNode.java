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
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Objects;

/**
 * Used for through nodes and other cases where you need
 * a source node immediately followed by a sink node
 */
class SourceSinkNode extends StreamsGraphNode {

    final Serde keySerde;
    final Serde valueSerde;
    final ChangedDeserializer changedDeserializer;
    final ChangedSerializer changedSerializer;
    final String sinkTopic;
    final String sinkName;
    final String funcOrFilterName;
    final String sourceName;
    final ProcessorSupplier processorSupplier;


    public SourceSinkNode(final TopologyNodeType topologyNodeType,
                          final Serde keySerde,
                          final Serde valueSerde,
                          final String sinkTopic,
                          final String sinkName,
                          final String funcOrFilterName,
                          final String sourceName,
                          final ProcessorSupplier processorSupplier,
                          final ChangedDeserializer changedDeserializer,
                          final ChangedSerializer changedSerializer) {
        super(topologyNodeType);
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.sinkTopic = sinkTopic;
        this.sinkName = sinkName;
        this.funcOrFilterName = funcOrFilterName;
        this.sourceName = sourceName;
        this.processorSupplier = processorSupplier;
        this.changedDeserializer = changedDeserializer;
        this.changedSerializer = changedSerializer;
    }

    public static Builder builder() {
        return new Builder();
    }


    public static final class Builder {

        String name;
        Serde keySerde;
        Serde valueSerde;
        String sinkTopic;
        String sinkName;
        String filterName;
        String sourceName;
        TopologyNodeType topologyNodeType;
        ProcessorSupplier processorSupplier;
        ProcessDetails processDetails;
        ChangedSerializer changedSerializer;
        ChangedDeserializer changedDeserializer;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder withKeySerde(Serde keySerde) {
            this.keySerde = keySerde;
            return this;
        }

        public Builder withValueSerde(Serde valueSerde) {
            this.valueSerde = valueSerde;
            return this;
        }

        public Builder withSinkTopic(String repartitionTopic) {
            this.sinkTopic = repartitionTopic;
            return this;
        }

        public Builder withSinkName(String sinkName) {
            this.sinkName = sinkName;
            return this;
        }

        public Builder withFilterName(String filterName) {
            this.filterName = filterName;
            return this;
        }

        public Builder withSourceName(String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withTopologyNodeType(TopologyNodeType topologyNodeType) {
            this.topologyNodeType = topologyNodeType;
            return this;
        }

        public Builder withProcessorSupplier(ProcessorSupplier processorSupplier) {
            this.processorSupplier = processorSupplier;
            return this;
        }

        public Builder withProcessDetails(ProcessDetails processDetails) {
            this.processDetails = processDetails;
            return this;
        }

        public Builder withChangedSerialiazer(ChangedSerializer changedSerialiazer) {
            this.changedSerializer = changedSerialiazer;
            return this;
        }

        public Builder withChangedDeserializer(ChangedDeserializer changedDeserializer) {
            this.changedDeserializer = changedDeserializer;
            return this;
        }

        public SourceSinkNode build() {
            SourceSinkNode
                sourceSinkNode =
                new SourceSinkNode(topologyNodeType, keySerde, valueSerde, sinkTopic, sinkName, filterName, sourceName, processorSupplier, changedDeserializer, changedSerializer);
            sourceSinkNode.setName(name);
            sourceSinkNode.setProcessed(processDetails);
            return sourceSinkNode;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        SourceSinkNode that = (SourceSinkNode) o;
        return Objects.equals(keySerde, that.keySerde) &&
               Objects.equals(valueSerde, that.valueSerde) &&
               Objects.equals(sinkTopic, that.sinkTopic) &&
               Objects.equals(sinkName, that.sinkName) &&
               Objects.equals(funcOrFilterName, that.funcOrFilterName) &&
               Objects.equals(sourceName, that.sourceName) &&
               Objects.equals(processorSupplier, that.processorSupplier);
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), keySerde, valueSerde, sinkTopic, sinkName, funcOrFilterName, sourceName, processorSupplier);
    }
}
