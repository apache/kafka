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

import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Used to represent any type of stateless operation:
 *
 * map, mapValues, flatMap, flatMapValues, filter, filterNot, branch
 *
 */
class StatelessProcessorNode<K, V> extends StreamsGraphNode {

    private final ProcessorSupplier<K, V> processorSupplier;

    // some processors need to register multiple parent names with
    // the InternalTopologyBuilder KStream#merge for example.
    // There is only one parent graph node but the name of each KStream merged needs
    // to get registered with InternalStreamsBuilder

    private List<String> multipleParentNames = new ArrayList<>();


    StatelessProcessorNode(final String parentProcessorNodeName,
                           final String processorNodeName,
                           final ProcessorSupplier<K, V> processorSupplier,
                           final boolean repartitionRequired) {

        super(parentProcessorNodeName,
              processorNodeName,
              repartitionRequired);

        this.processorSupplier = processorSupplier;
    }

    StatelessProcessorNode(final String parentProcessorNodeName,
                           final String processorNodeName,
                           final boolean repartitionRequired,
                           final ProcessorSupplier<K, V> processorSupplier,
                           final List<String> multipleParentNames) {

        this(parentProcessorNodeName, processorNodeName, processorSupplier, repartitionRequired);

        this.multipleParentNames = multipleParentNames;
    }

    ProcessorSupplier<K, V> processorSupplier() {
        return processorSupplier;
    }

    List<String> multipleParentNames() {
        return new ArrayList<>(multipleParentNames);
    }

    @Override
    void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        //TODO will implement in follow-up pr
    }
}
