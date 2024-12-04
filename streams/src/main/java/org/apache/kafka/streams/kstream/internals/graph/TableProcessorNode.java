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

import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.StoreFactory;

import java.util.Arrays;
import java.util.Objects;

public class TableProcessorNode<K, V> extends GraphNode {

    private final ProcessorParameters<K, V, ?, ?> processorParameters;
    private final StoreFactory storeFactory;
    private final String[] storeNames;

    public TableProcessorNode(final String nodeName,
                              final ProcessorParameters<K, V, ?, ?> processorParameters,
                              final StoreFactory storeFactory) {
        this(nodeName, processorParameters, storeFactory, null);
    }

    public TableProcessorNode(final String nodeName,
                              final ProcessorParameters<K, V, ?, ?> processorParameters,
                              final StoreFactory storeFactory,
                              final String[] storeNames) {
        super(nodeName);
        this.processorParameters = processorParameters;
        this.storeFactory = storeFactory;
        this.storeNames = storeNames != null ? storeNames : new String[] {};
    }

    public ProcessorParameters<K, V, ?, ?> processorParameters() {
        return processorParameters;
    }

    @Override
    public String toString() {
        return "TableProcessorNode{" +
            ", processorParameters=" + processorParameters +
            ", storeFactory=" + (storeFactory == null ? "null" : storeFactory.storeName()) +
            ", storeNames=" + Arrays.toString(storeNames) +
            "} " + super.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        final String processorName = processorParameters.processorName();
        topologyBuilder.addProcessor(processorName, processorParameters.processorSupplier(), parentNodeNames());

        if (storeNames.length > 0) {
            topologyBuilder.connectProcessorAndStateStores(processorName, storeNames);
        }

        final KTableSource<K, V> tableSource =  processorParameters.processorSupplier() instanceof KTableSource ?
                (KTableSource<K, V>) processorParameters.processorSupplier() : null;
        if (tableSource != null) {
            if (tableSource.materialized()) {
                topologyBuilder.addStateStore(Objects.requireNonNull(storeFactory, "storeFactory was null"),
                                              processorName);
            }
        } else if (storeFactory != null) {
            topologyBuilder.addStateStore(storeFactory, processorName);
        }
    }
}
