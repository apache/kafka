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

import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.ConnectedStoreProvider;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.Arrays;
import java.util.Set;

/**
 * Used for stateful processors that need to be manually connected to the state store(s)
 * they need to access. This should only be used in cases where the stores) cannot
 * be connected automatically by implementing the {@link ConnectedStoreProvider#stores()} method
 * and returning the store directly. Generally this will only apply to DSL operators that utilize
 * value getters to access another processor's state store(s), and the process/processValues
 * operator where the user's custom processor supplier doesn't implement the #stores method
 * and has to be connected to the store when compiling the topology.
 */
public class ProcessorToStateConnectorNode<K, V> extends ProcessorGraphNode<K, V> {

    private final String[] storeNames;

    /**
     * Create a node representing a stateful processor that uses value getters to access stores, and needs to
     * be connected with those stores
     */
    public ProcessorToStateConnectorNode(final ProcessorParameters<K, V, ?, ?> processorParameters,
                                         final Set<KTableValueGetterSupplier<?, ?>> valueGetterSuppliers) {
        super(processorParameters.processorName(), processorParameters);
        storeNames = valueGetterSuppliers.stream().flatMap(s -> Arrays.stream(s.storeNames())).toArray(String[]::new);
    }

    /**
     * Create a node representing a stateful processor, which needs to be connected to the provided stores
     */
    public ProcessorToStateConnectorNode(final String nodeName,
                                         final ProcessorParameters<K, V, ?, ?> processorParameters,
                                         final String[] storeNames) {
        super(nodeName, processorParameters);
        this.storeNames = storeNames;
    }

    @Override
    public String toString() {
        return "ProcessorNode{" +
            "storeNames=" + Arrays.toString(storeNames) +
            "} " + super.toString();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        processorParameters().addProcessorTo(topologyBuilder, parentNodeNames());

        if (storeNames != null && storeNames.length > 0) {
            topologyBuilder.connectProcessorAndStateStores(processorParameters().processorName(), storeNames);
        }
    }
}
