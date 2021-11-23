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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.TimestampedKeyValueStoreMaterializer;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

/**
 * Represents a KTable convert From KStream
 */
public class StreamToTableNode<K, V> extends GraphNode {

    private final ProcessorParameters<K, V, ?, ?> processorParameters;
    private final MaterializedInternal<K, V, ?> materializedInternal;

    public StreamToTableNode(final String nodeName,
                             final ProcessorParameters<K, V, ?, ?> processorParameters,
                             final MaterializedInternal<K, V, ?> materializedInternal) {
        super(nodeName);
        this.processorParameters = processorParameters;
        this.materializedInternal = materializedInternal;
    }

    @Override
    public String toString() {
        return "StreamToTableNode{" +
            ", processorParameters=" + processorParameters +
            ", materializedInternal=" + materializedInternal +
            "} " + super.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        final StoreBuilder<TimestampedKeyValueStore<K, V>> storeBuilder =
            new TimestampedKeyValueStoreMaterializer<>((MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>>) materializedInternal).materialize();

        final String processorName = processorParameters.processorName();
        final KTableSource<K, V> ktableSource = processorParameters.kTableSourceSupplier();
        topologyBuilder.addProcessor(processorName, processorParameters.processorSupplier(), parentNodeNames());

        if (storeBuilder != null && ktableSource.materialized()) {
            topologyBuilder.addStateStore(storeBuilder, processorName);
        }
    }
}
