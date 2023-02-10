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
package org.apache.kafka.streams;

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorNode;

import java.util.Properties;

/**
 * This class provides access to {@link TopologyTestDriver} protected methods.
 * It should only be used for internal testing, in the rare occasions where the
 * necessary functionality is not supported by {@link TopologyTestDriver}.
 */
public class TopologyTestDriverWrapper extends TopologyTestDriver {


    public TopologyTestDriverWrapper(final Topology topology,
                                     final Properties config) {
        super(topology, config);
    }

    /**
     * Get the processor context, setting the processor whose name is given as current node
     *
     * @param processorName processor name to set as current node
     * @return the processor context
     */
    @SuppressWarnings("unchecked")
    public <K, V> ProcessorContext<K, V> setCurrentNodeForProcessorContext(final String processorName) {
        final ProcessorContext<K, V> context = task.processorContext();
        ((ProcessorContextImpl) context).setCurrentNode(getProcessor(processorName));
        return context;
    }

    /**
     * Get a processor by name
     *
     * @param name the name to search for
     * @return the processor matching the search name
     */
    public ProcessorNode<?, ?, ?, ?> getProcessor(final String name) {
        for (final ProcessorNode<?, ?, ?, ?> node : processorTopology.processors()) {
            if (node.name().equals(name)) {
                return node;
            }
        }
        for (final ProcessorNode<?, ?, ?, ?> node : globalTopology.processors()) {
            if (node.name().equals(name)) {
                return node;
            }
        }
        throw new StreamsException("Could not find a processor named '" + name + "'");
    }
}
