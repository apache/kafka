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

/**
 * Inform downstream operations to preserve the partition, i.e. does not trigger repartition.
 */
public class PartitionPreservingNode<K, V> extends ProcessorGraphNode<K, V> {

    private String nodeName;

    public PartitionPreservingNode(final ProcessorParameters<K, V, ?, ?> processorParameters, final String nodeName) {
        super(processorParameters);
        this.nodeName = nodeName;
    }

    @Override
    public boolean isKeyChangingOperation() {
        return false;
    }

    @Override
    public void keyChangingOperation(final boolean keyChangingOperation) {
        // can not change this to a key changing operation
        if (keyChangingOperation) {
            throw new IllegalArgumentException("Cannot set a PartitionPreservingNode to key changing");
        }
    }

    @Override
    public String toString() {
        return "PartitionPreservingNode{" +
                "nodeName='" + nodeName + '}';
    }
}
