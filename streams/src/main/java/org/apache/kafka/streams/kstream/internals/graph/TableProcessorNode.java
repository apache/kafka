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

import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

public class TableProcessorNode<K, V, S extends StateStore> extends StreamsGraphNode {

    private final MaterializedInternal<K, V, S> materializedInternal;
    private final ProcessorParameters<K, V> processorParameters;
    private final String[] storeNames;

    public TableProcessorNode(final String nodeName,
                              final ProcessorParameters<K, V> processorParameters,
                              final MaterializedInternal<K, V, S> materializedInternal,
                              final String[] storeNames) {

        super(nodeName,
              false);
        this.processorParameters = processorParameters;
        this.materializedInternal = materializedInternal;
        this.storeNames = storeNames != null ? storeNames : new String[]{};
    }


    @Override
    public void writeToTopology(InternalTopologyBuilder topologyBuilder) {

    }
}
