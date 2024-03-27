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

import java.util.Iterator;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.KeyValueStoreMaterializer;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collections;

/**
 * Used to represent either a KTable source or a GlobalKTable source. A boolean flag is used to indicate if this represents a GlobalKTable a {@link
 * org.apache.kafka.streams.kstream.GlobalKTable}
 */
public class TableSourceNode<K, V> extends SourceGraphNode<K, V> {

    private final MaterializedInternal<K, V, ?> materializedInternal;
    private final ProcessorParameters<K, V, ?, ?> processorParameters;
    private final String sourceName;
    private final boolean isGlobalKTable;
    private boolean shouldReuseSourceTopicForChangelog = false;

    private TableSourceNode(final String nodeName,
                            final String sourceName,
                            final String topic,
                            final ConsumedInternal<K, V> consumedInternal,
                            final MaterializedInternal<K, V, ?> materializedInternal,
                            final ProcessorParameters<K, V, ?, ?> processorParameters,
                            final boolean isGlobalKTable) {

        super(nodeName,
              Collections.singletonList(topic),
              consumedInternal);

        this.sourceName = sourceName;
        this.isGlobalKTable = isGlobalKTable;
        this.processorParameters = processorParameters;
        this.materializedInternal = materializedInternal;
    }


    public void reuseSourceTopicForChangeLog(final boolean shouldReuseSourceTopicForChangelog) {
        this.shouldReuseSourceTopicForChangelog = shouldReuseSourceTopicForChangelog;
    }

    @Override
    public String toString() {
        return "TableSourceNode{" +
               "materializedInternal=" + materializedInternal +
               ", processorParameters=" + processorParameters +
               ", sourceName='" + sourceName + '\'' +
               ", isGlobalKTable=" + isGlobalKTable +
               "} " + super.toString();
    }

    public static <K, V> TableSourceNodeBuilder<K, V> tableSourceNodeBuilder() {
        return new TableSourceNodeBuilder<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        final String topicName;
        if (topicNames().isPresent()) {
            final Iterator<String> topicNames = topicNames().get().iterator();
            topicName = topicNames.next();
            if (topicNames.hasNext()) {
                throw new IllegalStateException("A table source node must have a single topic as input");
            }
        } else {
            throw new IllegalStateException("A table source node must have a single topic as input");
        }

        final StoreFactory storeFactory =
            new KeyValueStoreMaterializer<>((MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>>) materializedInternal);

        if (isGlobalKTable) {
            topologyBuilder.addGlobalStore(
                storeFactory,
                sourceName,
                consumedInternal().timestampExtractor(),
                consumedInternal().keyDeserializer(),
                consumedInternal().valueDeserializer(),
                topicName,
                processorParameters.processorName(),
                (ProcessorSupplier<K, V, Void, Void>) processorParameters.processorSupplier(),
                false
            );
        } else {
            topologyBuilder.addSource(consumedInternal().offsetResetPolicy(),
                                      sourceName,
                                      consumedInternal().timestampExtractor(),
                                      consumedInternal().keyDeserializer(),
                                      consumedInternal().valueDeserializer(),
                                      topicName);

            topologyBuilder.addProcessor(processorParameters.processorName(), processorParameters.processorSupplier(), sourceName);

            // only add state store if the source KTable should be materialized
            final KTableSource<K, V> tableSource = (KTableSource<K, V>) processorParameters.processorSupplier();
            if (tableSource.materialized()) {
                topologyBuilder.addStateStore(storeFactory, nodeName());

                if (shouldReuseSourceTopicForChangelog) {
                    storeFactory.withLoggingDisabled();
                    topologyBuilder.connectSourceStoreAndTopic(storeFactory.name(), topicName);
                }
            }
        }

    }

    public static final class TableSourceNodeBuilder<K, V> {

        private String nodeName;
        private String sourceName;
        private String topic;
        private ConsumedInternal<K, V> consumedInternal;
        private MaterializedInternal<K, V, ?> materializedInternal;
        private ProcessorParameters<K, V, ?, ?> processorParameters;
        private boolean isGlobalKTable = false;

        private TableSourceNodeBuilder() {
        }

        public TableSourceNodeBuilder<K, V> withSourceName(final String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withTopic(final String topic) {
            this.topic = topic;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withMaterializedInternal(final MaterializedInternal<K, V, ?> materializedInternal) {
            this.materializedInternal = materializedInternal;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withConsumedInternal(final ConsumedInternal<K, V> consumedInternal) {
            this.consumedInternal = consumedInternal;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withProcessorParameters(final ProcessorParameters<K, V, ?, ?> processorParameters) {
            this.processorParameters = processorParameters;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public TableSourceNodeBuilder<K, V> isGlobalKTable(final boolean isGlobalKTable) {
            this.isGlobalKTable = isGlobalKTable;
            return this;
        }

        public TableSourceNode<K, V> build() {
            return new TableSourceNode<>(nodeName,
                                         sourceName,
                                         topic,
                                         consumedInternal,
                                         materializedInternal,
                                         processorParameters,
                                         isGlobalKTable);
        }
    }
}
