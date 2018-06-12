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

import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

public abstract class StreamsGraphNode {

    private StreamsGraphNode parentNode;
    private LinkedHashSet<StreamsGraphNode> childNodes = new LinkedHashSet<>();
    private final String nodeName;
    private boolean repartitionRequired;
    private boolean triggersRepartitioning;
    private Integer id;
    private InternalStreamsBuilder internalStreamsBuilder;

    public StreamsGraphNode(final String nodeName,
                            final boolean repartitionRequired) {
        this.nodeName = nodeName;
        this.repartitionRequired = repartitionRequired;
    }

    public StreamsGraphNode parentNode() {
        return parentNode;
    }

    public void setParentNode(final StreamsGraphNode parentNode) {
        this.parentNode = parentNode;
    }

    public Collection<StreamsGraphNode> children() {
        return new LinkedHashSet<>(childNodes);
    }

    public void clearChildren() {
        childNodes.clear();
    }

    public void setChildNodes(Set<StreamsGraphNode> streamsGraphNodes) {
        childNodes.clear();
        for (StreamsGraphNode streamsGraphNode : streamsGraphNodes) {
            addChildNode(streamsGraphNode);
        }
    }

    public void addChildNode(final StreamsGraphNode childNode) {
        this.childNodes.add(childNode);
        childNode.setParentNode(this);
    }

    public String nodeName() {
        return nodeName;
    }

    public boolean repartitionRequired() {
        return repartitionRequired;
    }

    public boolean triggersRepartitioning() {
        return triggersRepartitioning;
    }

    public void setTriggersRepartitioning(final boolean triggersRepartitioning) {
        this.triggersRepartitioning = triggersRepartitioning;
    }

    public void setId(final int id) {
        this.id = id;
    }

    public Integer id() {
        return this.id;
    }

    public void setInternalStreamsBuilder(final InternalStreamsBuilder internalStreamsBuilder) {
        this.internalStreamsBuilder = internalStreamsBuilder;
    }

    public InternalStreamsBuilder internalStreamsBuilder() {
        return internalStreamsBuilder;
    }

    public abstract void writeToTopology(final InternalTopologyBuilder topologyBuilder);

    @Override
    public String toString() {
        String parentName = parentNode != null ? parentNode.nodeName : "null";
        return "StreamsGraphNode{" +
               "nodeName='" + nodeName + '\'' +
               ", id=" + id +
               " parentNode=" + parentName + '}';
    }
}
