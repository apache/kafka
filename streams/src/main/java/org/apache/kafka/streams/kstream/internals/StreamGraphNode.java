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

import java.util.Collection;
import java.util.LinkedHashSet;

class StreamGraphNode<K, V> {

    private StreamGraphNode<K, V> parentNode;
    private final Collection<StreamGraphNode<K, V>> childNodes = new LinkedHashSet<>();
    private final String nodeName;
    private String parentNodeName;
    private boolean repartitionRequired;
    private boolean triggersRepartitioning;
    private Integer id;
    private StreamsTopologyGraph streamsTopologyGraph;

    StreamGraphNode(final String parentNodeName,
                    final String nodeName,
                    final boolean repartitionRequired) {
        this.parentNodeName = parentNodeName;
        this.nodeName = nodeName;
        this.repartitionRequired = repartitionRequired;
    }

    StreamGraphNode<K, V> parentNode() {
        return parentNode;
    }

    String parentNodeName() {
        return parentNodeName;
    }

    void setParentNodeName(String parentNodeName) {
        this.parentNodeName = parentNodeName;
    }

    void setParentNode(StreamGraphNode<K, V> parentNode) {
        this.parentNode = parentNode;
    }

    Collection<StreamGraphNode<K, V>> children() {
        return new LinkedHashSet<>(childNodes);
    }

    void addChildNode(StreamGraphNode<K, V> node) {
        this.childNodes.add(node);
    }

    String nodeName() {
        return nodeName;
    }

    boolean isRepartitionRequired() {
        return repartitionRequired;
    }

    void setRepartitionRequired(boolean repartitionRequired) {
        this.repartitionRequired = repartitionRequired;
    }

    public boolean isTriggersRepartitioning() {
        return triggersRepartitioning;
    }

    public void setTriggersRepartitioning(boolean triggersRepartitioning) {
        this.triggersRepartitioning = triggersRepartitioning;
    }

    void setId(final int id) {
        this.id = id;
    }

    Integer id() {
        return this.id;
    }

    public StreamsTopologyGraph getStreamsTopologyGraph() {
        return streamsTopologyGraph;
    }

    public void setStreamsTopologyGraph(StreamsTopologyGraph streamsTopologyGraph) {
        this.streamsTopologyGraph = streamsTopologyGraph;
    }

}
