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

public class RepartitionGraphNode extends StreamsGraphNode {

    final Serde keySerde;
    final Serde valueSerde;
    final String repartitionTopic;
    final String sinkName;
    final String filterName;
    final String sourceName;
    final ProcessorSupplier processorSupplier;


    public RepartitionGraphNode(final TopologyNodeType nodeType,
                                final Serde keySerde,
                                final Serde valueSerde,
                                final String repartitionTopic,
                                final String sinkName,
                                final String filterName,
                                final String sourceName,
                                final ProcessorSupplier processorSupplier) {
        super(nodeType);
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.repartitionTopic = repartitionTopic;
        this.sinkName = sinkName;
        this.filterName = filterName;
        this.sourceName = sourceName;
        this.processorSupplier = processorSupplier;
    }


    public static final class Builder {

        String name;
        Serde keySerde;
        Serde valueSerde;
        String repartitionTopic;
        String sinkName;
        String filterName;
        String sourceName;
        TopologyNodeType nodeType;
        ProcessorSupplier processorSupplier;

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

        public Builder withRepartitionTopic(String repartitionTopic) {
            this.repartitionTopic = repartitionTopic;
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

        public Builder withTopologyNodeType(TopologyNodeType nodeType) {
            this.nodeType = nodeType;
            return this;
        }

        public Builder withProcessorSupplier(ProcessorSupplier processorSupplier) {
            this.processorSupplier = processorSupplier;
            return this;
        }

        public RepartitionGraphNode build() {
            RepartitionGraphNode
                repartitionGraphNode =
                new RepartitionGraphNode(nodeType, keySerde, valueSerde, repartitionTopic, sinkName, filterName, sourceName, processorSupplier);
            repartitionGraphNode.setName(name);
            return repartitionGraphNode;
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
        RepartitionGraphNode that = (RepartitionGraphNode) o;
        return Objects.equals(keySerde, that.keySerde) &&
               Objects.equals(valueSerde, that.valueSerde) &&
               Objects.equals(repartitionTopic, that.repartitionTopic) &&
               Objects.equals(sinkName, that.sinkName) &&
               Objects.equals(filterName, that.filterName) &&
               Objects.equals(sourceName, that.sourceName) &&
               Objects.equals(processorSupplier, that.processorSupplier);
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), keySerde, valueSerde, repartitionTopic, sinkName, filterName, sourceName, processorSupplier);
    }
}
