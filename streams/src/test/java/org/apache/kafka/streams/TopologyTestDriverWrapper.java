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

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorNode;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

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

    public TopologyTestDriverWrapper(final Topology topology,
                                     final Properties config,
                                     final long initialWallClockTimeMs) {
        super(topology, config, initialWallClockTimeMs);
    }

    /**
     * Get the processor context
     *
     * @param topicName the topic name is used to identify the source node, which is set as current node
     * @return the processor context
     */
    public ProcessorContext getProcessorContext(final String topicName) {
        final ProcessorContext context = task.context();
        ((ProcessorContextImpl) context).setCurrentNode(sourceNodeByTopicName(topicName));
        return context;
    }

    /**
     * Identify the source node for a given topic
     *
     * @param topicName the topic name to search for
     * @return the source node
     */
    private ProcessorNode sourceNodeByTopicName(final String topicName) {
        ProcessorNode topicNode = processorTopology.source(topicName);
        if (topicNode == null) {
            for (final String sourceTopic : processorTopology.sourceTopics()) {
                if (Pattern.compile(sourceTopic).matcher(topicName).matches()) {
                    return processorTopology.source(sourceTopic);
                }
            }
            if (globalTopology != null) {
                topicNode = globalTopology.source(topicName);
            }
        }
        return topicNode;
    }

    /**
     * Get a processor by name
     *
     * @param name the name to search for
     * @return the processor matching the search name
     */
    public ProcessorNode getProcessor(final String name) {
        final List<ProcessorNode> nodes = processorTopology.processors();

        for (final ProcessorNode node : nodes) {
            if (node.name().equals(name)) {
                return node;
            }
        }

        return null;
    }
}
