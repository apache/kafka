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
    private String nodeName;
    private boolean repartitionRequired;
    private boolean triggersRepartitioning;
    private Integer id;
    private InternalStreamsBuilder internalStreamsBuilder;

    StreamsGraphNode(final String nodeName,
                     final boolean repartitionRequired) {
        this.nodeName = nodeName;
        this.repartitionRequired = repartitionRequired;
    }

    StreamsGraphNode parentNode() {
        return parentNode;
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

    String nodeName() {
        return nodeName;
    }

    void setNodeName(String nodeName) {
        this.nodeName = nodeName;
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

    void setInternalStreamsBuilder(final InternalStreamsBuilder internalStreamsBuilder) {
        this.internalStreamsBuilder = internalStreamsBuilder;
    }

    InternalStreamsBuilder internalStreamsBuilder() {
        return internalStreamsBuilder;
    }

    abstract void writeToTopology(final InternalTopologyBuilder topologyBuilder);

}
