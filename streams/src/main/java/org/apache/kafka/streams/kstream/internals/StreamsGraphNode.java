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

import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.Collection;
import java.util.LinkedHashSet;

abstract class StreamsGraphNode {

    private StreamsGraphNode parentNode;
    private final Collection<StreamsGraphNode> childNodes = new LinkedHashSet<>();
    private final String processorNodeName;
    private String parentProcessorNodeName;
    private boolean repartitionRequired;
    private boolean triggersRepartitioning;
    private Integer id;
    private StreamsTopologyGraph streamsTopologyGraph;

    StreamsGraphNode(final String parentProcessorNodeName,
                     final String processorNodeName,
                     final boolean repartitionRequired) {
        this.parentProcessorNodeName = parentProcessorNodeName;
        this.processorNodeName = processorNodeName;
        this.repartitionRequired = repartitionRequired;
    }

    StreamsGraphNode parentNode() {
        return parentNode;
    }

    String parentProcessorNodeName() {
        return parentProcessorNodeName;
    }

    void setParentProcessorNodeName(final String parentProcessorNodeName) {
        this.parentProcessorNodeName = parentProcessorNodeName;
    }

    void setParentNode(final StreamsGraphNode parentNode) {
        this.parentNode = parentNode;
    }

    Collection<StreamsGraphNode> children() {
        return new LinkedHashSet<>(childNodes);
    }

    void addChildNode(final StreamsGraphNode node) {
        this.childNodes.add(node);
    }

    String processorNodeName() {
        return processorNodeName;
    }

    boolean repartitionRequired() {
        return repartitionRequired;
    }

    void setRepartitionRequired(boolean repartitionRequired) {
        this.repartitionRequired = repartitionRequired;
    }

    public boolean triggersRepartitioning() {
        return triggersRepartitioning;
    }

    public void setTriggersRepartitioning(final boolean triggersRepartitioning) {
        this.triggersRepartitioning = triggersRepartitioning;
    }

    void setId(final int id) {
        this.id = id;
    }

    Integer id() {
        return this.id;
    }

    public void setStreamsTopologyGraph(final StreamsTopologyGraph streamsTopologyGraph) {
        this.streamsTopologyGraph = streamsTopologyGraph;
    }

    StreamsTopologyGraph streamsTopologyGraph() {
        return streamsTopologyGraph;
    }

    abstract void writeToTopology(final InternalTopologyBuilder topologyBuilder);

}
