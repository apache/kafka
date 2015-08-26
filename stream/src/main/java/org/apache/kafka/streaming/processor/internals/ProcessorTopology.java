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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProcessorTopology {

    private List<ProcessorNode> processors;
    private Map<String, SourceNode> sourceByTopics;
    private Map<String, SinkNode> sinkByTopics;

    public ProcessorTopology(List<ProcessorNode> processors,
                             Map<String, SourceNode> sourceByTopics,
                             Map<String, SinkNode> sinkByTopics) {
        this.processors = processors;
        this.sourceByTopics = sourceByTopics;
        this.sinkByTopics = sinkByTopics;
    }

    public Set<String> sourceTopics() {
        return sourceByTopics.keySet();
    }

    public Set<String> sinkTopics() {
        return sinkByTopics.keySet();
    }

    public SourceNode source(String topic) {
        return sourceByTopics.get(topic);
    }

    public SinkNode sink(String topic) {
        return sinkByTopics.get(topic);
    }

    public Collection<SourceNode> sources() {
        return sourceByTopics.values();
    }

    public Collection<SinkNode> sinks() {
        return sinkByTopics.values();
    }

    /**
     * Initialize the processors following the DAG reverse ordering
     * such that parents are always initialized before children
     */
    public void init(ProcessorContext context) {
        for (ProcessorNode node : processors) {
            node.init(context);
        }
    }

    public final void close() {
        // close the processors
        for (ProcessorNode node : processors) {
            node.close();
        }

        processors.clear();
        sourceByTopics.clear();
        sinkByTopics.clear();
    }
}
