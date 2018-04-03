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
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.TopologyNodeType.FLATMAP;
import static org.apache.kafka.streams.kstream.internals.TopologyNodeType.GROUP_BY;
import static org.apache.kafka.streams.kstream.internals.TopologyNodeType.MAP;
import static org.apache.kafka.streams.kstream.internals.TopologyNodeType.SELECT_KEY;
import static org.apache.kafka.streams.kstream.internals.TopologyNodeType.TRANSFORM;

/**
 * A Node in the Streams DAG used to contain all information needed to construct this
 * portion of the Topology.  The class holds information until the InternalTopologyBuilder builds
 * the actual topology.  This is the base case class and other more specific cases required
 * more detailed information will extend this class
 */
public class StreamsGraphNode {


    protected final TopologyNodeType topologyNodeType;
    protected StreamsGraphNode predecessor;
    protected final Collection<StreamsGraphNode> descendants = new LinkedHashSet<>();
    protected boolean repartitionRequired;
    protected ProcessDetails processDetails;
    protected String name;
    protected String predecessorName;
    protected Integer id;

    public StreamsGraphNode(final TopologyNodeType topologyNodeType) {
        this.topologyNodeType = topologyNodeType;
    }

    public StreamsGraphNode(final String name, final TopologyNodeType topologyNodeType) {
        this(name, topologyNodeType, false, null, null);
    }

    public StreamsGraphNode(final String name,
                            final TopologyNodeType topologyNodeType,
                            final boolean repartitionRequired,
                            final ProcessDetails processDetails,
                            final String predecessorName) {
        this.topologyNodeType = topologyNodeType;
        this.repartitionRequired = repartitionRequired;
        this.name = name;
        this.processDetails = processDetails;
        this.predecessorName = predecessorName;
    }

    public StreamsGraphNode getPredecessor() {
        return predecessor;
    }


    public void setPredecessor(StreamsGraphNode predecessor) {
        Objects.requireNonNull(predecessor, "Can't set a null predecessor");
        this.predecessor = predecessor;
        this.predecessorName = predecessor.name();
    }


    public Collection<StreamsGraphNode> getDescendants() {
        return descendants;
    }


    public void addDescendant(StreamsGraphNode descendant) {
        descendants.add(descendant);
    }


    public boolean triggersRepartitioning() {
        return topologyNodeType == MAP || topologyNodeType == GROUP_BY ||
               topologyNodeType == FLATMAP || topologyNodeType == TRANSFORM || topologyNodeType == SELECT_KEY;
    }

    public boolean needsRepartitioning() {
        return repartitionRequired;
    }


    public boolean hasLoggedStore() {
        return processDetails.getMaterializedInternal() != null && processDetails.getMaterializedInternal().loggingEnabled();
    }


    public void setRepartitionNeeded(boolean needsRepartitioning) {
        this.repartitionRequired = needsRepartitioning;
    }


    public String name() {
        return this.name;
    }


    public void setName(String name) {
        this.name = name;
    }

    public String getPredecessorName() {
        return predecessorName;
    }

    public void setPredecessorName(String predecessorName) {
        this.predecessorName = predecessorName;
    }

    public TopologyNodeType getType() {
        return this.topologyNodeType;
    }

    public void setProcessed(ProcessDetails processDetails) {
        this.processDetails = processDetails;
    }

    public void setId(int id) {
        if (this.id == null) {
            this.id = id;
        }
    }

    public ProcessDetails getProcessed() {
        return this.processDetails;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StreamsGraphNode graphNode = (StreamsGraphNode) o;
        return topologyNodeType == graphNode.topologyNodeType &&
               Objects.equals(name, graphNode.name) &&
               Objects.equals(id, graphNode.id);
    }

    @Override
    public int hashCode() {

        return Objects.hash(topologyNodeType, name, id);
    }

    @Override
    public String toString() {
        return "StreamsGraphNode{" +
               "topologyNodeType=" + topologyNodeType +
               ", name='" + name + '\'' +
               ", predecessor=" + predecessor +
               ", predecessorName='" + predecessorName + '\'' +
               '}';
    }
}
