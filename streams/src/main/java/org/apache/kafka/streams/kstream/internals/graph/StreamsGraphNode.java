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


import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;

public abstract class StreamsGraphNode {

    private final Collection<StreamsGraphNode> childNodes = new LinkedHashSet<>();
    private final Collection<StreamsGraphNode> parentNodes = new LinkedHashSet<>();
    private final String nodeName;
    private boolean keyChangingOperation;
    private boolean valueChangingOperation;
    private boolean mergeNode;
    private Integer buildPriority;
    private boolean hasWrittenToTopology = false;

    public StreamsGraphNode(final String nodeName) {
        this.nodeName = nodeName;
    }

    public Collection<StreamsGraphNode> parentNodes() {
        return parentNodes;
    }

    String[] parentNodeNames() {
        final String[] parentNames = new String[parentNodes.size()];
        int index = 0;
        for (final StreamsGraphNode parentNode : parentNodes) {
            parentNames[index++] = parentNode.nodeName();
        }
        return parentNames;
    }

    public boolean allParentsWrittenToTopology() {
        for (final StreamsGraphNode parentNode : parentNodes) {
            if (!parentNode.hasWrittenToTopology()) {
                return false;
            }
        }
        return true;
    }

    public Collection<StreamsGraphNode> children() {
        return new LinkedHashSet<>(childNodes);
    }

    public void clearChildren() {
        for (final StreamsGraphNode childNode : childNodes) {
            childNode.parentNodes.remove(this);
        }
        childNodes.clear();
    }

    public boolean removeChild(final StreamsGraphNode child) {
        return childNodes.remove(child) && child.parentNodes.remove(this);
    }

    public void addChild(final StreamsGraphNode childNode) {
        this.childNodes.add(childNode);
        childNode.parentNodes.add(this);
    }

    public String nodeName() {
        return nodeName;
    }

    public boolean isKeyChangingOperation() {
        return keyChangingOperation;
    }

    public boolean isValueChangingOperation() {
        return valueChangingOperation;
    }

    public boolean isMergeNode() {
        return mergeNode;
    }

    public void setMergeNode(final boolean mergeNode) {
        this.mergeNode = mergeNode;
    }

    public void setValueChangingOperation(final boolean valueChangingOperation) {
        this.valueChangingOperation = valueChangingOperation;
    }

    public void keyChangingOperation(final boolean keyChangingOperation) {
        this.keyChangingOperation = keyChangingOperation;
    }

    public void setBuildPriority(final int buildPriority) {
        this.buildPriority = buildPriority;
    }

    public Integer buildPriority() {
        return this.buildPriority;
    }

    public abstract void writeToTopology(final InternalTopologyBuilder topologyBuilder);

    public boolean hasWrittenToTopology() {
        return hasWrittenToTopology;
    }

    public void setHasWrittenToTopology(final boolean hasWrittenToTopology) {
        this.hasWrittenToTopology = hasWrittenToTopology;
    }

    @Override
    public String toString() {
        final String[] parentNames = parentNodeNames();
        return "StreamsGraphNode{" +
               "nodeName='" + nodeName + '\'' +
               ", buildPriority=" + buildPriority +
               ", hasWrittenToTopology=" + hasWrittenToTopology +
               ", keyChangingOperation=" + keyChangingOperation +
               ", valueChangingOperation=" + valueChangingOperation +
               ", mergeNode=" + mergeNode +
               ", parentNodes=" + Arrays.toString(parentNames) + '}';
    }
}
