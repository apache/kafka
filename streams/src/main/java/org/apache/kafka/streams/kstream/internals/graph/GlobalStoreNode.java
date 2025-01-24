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

import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.StoreDelegatingProcessorSupplier;
import org.apache.kafka.streams.processor.internals.StoreFactory;

import java.util.Set;

public class GlobalStoreNode<KIn, VIn, S extends StateStore> extends StateStoreNode<S> {

    private final String sourceName;
    private final String topic;
    private final ConsumedInternal<KIn, VIn> consumed;
    private final String processorName;
    private final ProcessorSupplier<KIn, VIn, Void, Void> stateUpdateSupplier;
    private final boolean reprocessOnRestore;


    public GlobalStoreNode(final StoreFactory storeBuilder,
                           final String sourceName,
                           final String topic,
                           final ConsumedInternal<KIn, VIn> consumed,
                           final String processorName,
                           final ProcessorSupplier<KIn, VIn, Void, Void> stateUpdateSupplier,
                           final boolean reprocessOnRestore) {

        super(storeBuilder);
        this.sourceName = sourceName;
        this.topic = topic;
        this.consumed = consumed;
        this.processorName = processorName;
        this.stateUpdateSupplier = stateUpdateSupplier;
        this.reprocessOnRestore = reprocessOnRestore;
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        storeBuilder.withLoggingDisabled();
        topologyBuilder.addGlobalStore(sourceName,
                                       consumed.timestampExtractor(),
                                       consumed.keyDeserializer(),
                                       consumed.valueDeserializer(),
                                       topic,
                                       processorName,
                                       new StoreDelegatingProcessorSupplier<>(
                                               stateUpdateSupplier,
                                               Set.of(new StoreFactory.FactoryWrappingStoreBuilder<>(storeBuilder))
                                       ), reprocessOnRestore);

    }

    @Override
    public String toString() {
        return "GlobalStoreNode{" +
               "sourceName='" + sourceName + '\'' +
               ", topic='" + topic + '\'' +
               ", processorName='" + processorName + '\'' +
               ", reprocessOnRestore='" + reprocessOnRestore + '\'' +
               "} ";
    }
}
