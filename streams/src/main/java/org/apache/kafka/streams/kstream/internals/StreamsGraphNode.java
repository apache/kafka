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

import static org.apache.kafka.streams.kstream.internals.StreamsGraphNode.TopologyNodeType.FLATMAP;
import static org.apache.kafka.streams.kstream.internals.StreamsGraphNode.TopologyNodeType.GLOBAL_KTABLE;
import static org.apache.kafka.streams.kstream.internals.StreamsGraphNode.TopologyNodeType.GROUP_BY;
import static org.apache.kafka.streams.kstream.internals.StreamsGraphNode.TopologyNodeType.JOIN;
import static org.apache.kafka.streams.kstream.internals.StreamsGraphNode.TopologyNodeType.MAP;
import static org.apache.kafka.streams.kstream.internals.StreamsGraphNode.TopologyNodeType.SELECT_KEY;
import static org.apache.kafka.streams.kstream.internals.StreamsGraphNode.TopologyNodeType.SOURCE;
import static org.apache.kafka.streams.kstream.internals.StreamsGraphNode.TopologyNodeType.TABLE;
import static org.apache.kafka.streams.kstream.internals.StreamsGraphNode.TopologyNodeType.TRANSFORM;


/**
 * A Node in the Streams DAG used to contain all information needed to construct this
 * portion of the Topology.  The class holds information until the InternalTopologyBuilder builds
 * the actual topology.
 */
public class StreamsGraphNode {

    public enum TopologyNodeType {
        MAP,
        GROUP_BY,
        SOURCE,
        TABLE,
        SINK,
        JOIN,
        FLATMAP,
        PROCESSING,
        FILTER,
        TRANSFORM,
        TRANSFORM_VALUES,
        PROCESSOR,
        KTABLE,
        TOPOLOGY_PARENT,
        TO_STREAM,
        AGGREGATE_TYPE,
        GLOBAL_KTABLE,
        SELECT_KEY,
        MAP_VALUES,
        REPARTITION,
        STREAM_KTABLE_JOIN,
        STREAM_GLOBAL_TABLE_JOIN,
        AGGREGATE
    }


    protected final TopologyNodeType topologyNodeType;
    protected StreamsGraphNode predecessor;
    protected final Collection<StreamsGraphNode> descendants = new LinkedHashSet<>();
    protected boolean repartitionRequired;
    protected ProcessDetails processDetails;
    protected String name;
    protected String predecessorName;

    public StreamsGraphNode(TopologyNodeType nodeType) {
        this.topologyNodeType = nodeType;
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
        this.predecessor = predecessor;
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

    public boolean isSourceNode() {
        return topologyNodeType == SOURCE || topologyNodeType == TABLE || topologyNodeType == GLOBAL_KTABLE || topologyNodeType == JOIN;
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

    public TopologyNodeType getType() {
        return this.topologyNodeType;
    }

    public void setProcessed(ProcessDetails processDetails) {
        this.processDetails = processDetails;
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
        StreamsGraphNode that = (StreamsGraphNode) o;
        return topologyNodeType == that.topologyNodeType &&
               Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {

        return Objects.hash(topologyNodeType, name);
    }

    @Override
    public String toString() {
        return "StreamsGraphNode{" +
               "topologyNodeType=" + topologyNodeType +
               ", predecessor=" + predecessor +
               ", name='" + name + '\'' +
               ", predecessorName='" + predecessorName + '\'' +
               '}';
    }
}
