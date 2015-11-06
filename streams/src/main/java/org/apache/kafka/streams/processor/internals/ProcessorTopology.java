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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.StateStoreSupplier;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProcessorTopology {

    private final List<ProcessorNode> processorNodes;
    private final Map<String, SourceNode> sourceByTopics;
    private final List<StateStoreSupplier> stateStoreSuppliers;

    public ProcessorTopology(List<ProcessorNode> processorNodes,
                             Map<String, SourceNode> sourceByTopics,
                             List<StateStoreSupplier> stateStoreSuppliers) {
        this.processorNodes = Collections.unmodifiableList(processorNodes);
        this.sourceByTopics = Collections.unmodifiableMap(sourceByTopics);
        this.stateStoreSuppliers = Collections.unmodifiableList(stateStoreSuppliers);
    }

    public Set<String> sourceTopics() {
        return sourceByTopics.keySet();
    }

    public SourceNode source(String topic) {
        return sourceByTopics.get(topic);
    }

    public Set<SourceNode> sources() {
        return new HashSet<>(sourceByTopics.values());
    }

    public List<ProcessorNode> processors() {
        return processorNodes;
    }

    public List<StateStoreSupplier> stateStoreSuppliers() {
        return stateStoreSuppliers;
    }

}
