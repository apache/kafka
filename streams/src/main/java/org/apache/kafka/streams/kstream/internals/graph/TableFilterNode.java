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

import org.apache.kafka.streams.kstream.internals.KTableFilter;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.StoreFactory;

public class TableFilterNode<K, V> extends TableProcessorNode<K, V> implements VersionedSemanticsGraphNode {

    public TableFilterNode(final String nodeName,
                           final ProcessorParameters<K, V, ?, ?> processorParameters,
                           final StoreFactory storeFactory) {
        super(nodeName, processorParameters, storeFactory);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void enableVersionedSemantics(final boolean useVersionedSemantics, final String parentNodeName) {
        final ProcessorSupplier<K, V, ?, ?> processorSupplier = processorParameters().processorSupplier();
        if (!(processorSupplier instanceof KTableFilter)) {
            throw new IllegalStateException("Unexpected processor type for table filter: " + processorSupplier.getClass().getName());
        }

        final KTableFilter<K, V> tableFilter = (KTableFilter<K, V>) processorSupplier;
        tableFilter.setUseVersionedSemantics(useVersionedSemantics);
    }
}
