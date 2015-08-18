/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streaming.processor.internals;

import org.apache.kafka.streaming.processor.ProcessorContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProcessorTopology {

    private Map<String, ProcessorNode> processors = new HashMap<>();
    private Map<String, SourceNode> sourceTopics = new HashMap<>();
    private Map<String, SinkNode> sinkTopics = new HashMap<>();

    public ProcessorTopology(Map<String, ProcessorNode> processors,
                             Map<String, SourceNode> sourceTopics,
                             Map<String, SinkNode> sinkTopics) {
        this.processors = processors;
        this.sourceTopics = sourceTopics;
        this.sinkTopics = sinkTopics;
    }

    public Set<String> sourceTopics() {
        return sourceTopics.keySet();
    }

    public Set<String> sinkTopics() {
        return sinkTopics.keySet();
    }

    public SourceNode source(String topic) {
        return sourceTopics.get(topic);
    }

    public SinkNode sink(String topic) {
        return sinkTopics.get(topic);
    }

    /**
     * Initialize the processors following the DAG reverse ordering
     * such that parents are always initialized before children
     */
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // initialize sources
        for (String topic : sourceTopics.keySet()) {
            SourceNode source = sourceTopics.get(topic);

            init(source, context);
        }
    }

    /**
     * Initialize the current processor node by first initializing
     * its parent nodes first, then the processor itself
     */
    @SuppressWarnings("unchecked")
    private void init(ProcessorNode node, ProcessorContext context) {
        for (ProcessorNode parentNode : (List<ProcessorNode>) node.parents()) {
            if (!parentNode.initialized) {
                init(parentNode, context);
            }
        }

        node.init(context);
        node.initialized = true;

        // try to initialize its children
        for (ProcessorNode childNode : (List<ProcessorNode>) node.children()) {
            if (!childNode.initialized) {
                init(childNode, context);
            }
        }
    }

    public final void close() {
        // close the processors
        // TODO: do we need to follow the DAG ordering
        for (ProcessorNode processorNode : processors.values()) {
            processorNode.close();
        }

        processors.clear();
        sourceTopics.clear();
    }
}
