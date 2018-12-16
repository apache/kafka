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
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.kstream.internals.KeyValueStoreMaterializer;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.SessionStoreMaterializer;
import org.apache.kafka.streams.kstream.internals.TimeWindowStoreMaterializer;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.internals.StateStoreType;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Collections;

/**
 * Used to represent either a KTable source or a GlobalKTable source. A boolean flag is used to indicate if this represents a GlobalKTable a {@link
 * org.apache.kafka.streams.kstream.GlobalKTable}
 */
public class TableSourceNode<K, V> extends StreamSourceNode<K, V> {

    private final MaterializedInternal<K, V, ?> materializedInternal;
    private final ProcessorParameters<?, V> processorParameters;
    private final String sourceName;
    private final StateStoreType storeType;
    private final boolean isGlobalKTable;

    private TableSourceNode(final String nodeName,
                            final String sourceName,
                            final String topic,
                            final ConsumedInternal<K, V> consumedInternal,
                            final MaterializedInternal<K, V, ?> materializedInternal,
                            final ProcessorParameters<K, V> processorParameters,
                            final StateStoreType storeType,
                            final boolean isGlobalKTable) {
        super(nodeName,
              Collections.singletonList(topic),
              consumedInternal,
               storeType != StateStoreType.KEY_VALUE_STORE);

        this.sourceName = sourceName;
        this.storeType = storeType;
        this.isGlobalKTable = isGlobalKTable;
        this.processorParameters = processorParameters;
        this.materializedInternal = materializedInternal;
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
        final String topicName = getTopicNames().iterator().next();

        final StoreBuilder<KeyValueStore<K, V>> keyValueStoreBuilder =
            new KeyValueStoreMaterializer<>((MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>>) materializedInternal).materialize();

        if (isGlobalKTable) {
            // We only support key-value store for global KTable.
            topologyBuilder.addGlobalStore(keyValueStoreBuilder,
                                           sourceName,
                                           consumedInternal().timestampExtractor(),
                                           consumedInternal().keyDeserializer(),
                                           consumedInternal().valueDeserializer(),
                                           topicName,
                                           processorParameters.processorName(),
                                           processorParameters.processorSupplier());
        } else {
            topologyBuilder.addSource(consumedInternal().offsetResetPolicy(),
                                      sourceName,
                                      consumedInternal().timestampExtractor(),
                                      consumedInternal().keyDeserializer(),
                                      consumedInternal().valueDeserializer(),
                                      topicName);

            topologyBuilder.addProcessor(processorParameters.processorName(), processorParameters.processorSupplier(), sourceName);

            // only add state store if the source KTable should be materialized
            final KTableSource<K, V> ktableSource = (KTableSource<K, V>) processorParameters.processorSupplier();
            if (ktableSource.queryableName() != null) {
                // Add State store builder based on the given store type.
                if (storeType == StateStoreType.TIME_WINDOW_STORE) {
                    final StoreBuilder<WindowStore<K, V>> windowStoreBuilder =
                        new TimeWindowStoreMaterializer<>((MaterializedInternal<K, V, WindowStore<Bytes, byte[]>>) materializedInternal).materialize();

                    topologyBuilder.addStateStore(windowStoreBuilder, nodeName());
                    topologyBuilder.markSourceStoreAndTopic(windowStoreBuilder, topicName);
                } else if (storeType == StateStoreType.SESSION_STORE) {
                    final StoreBuilder<SessionStore<K, V>> sessionStoreBuilder =
                        new SessionStoreMaterializer<>((MaterializedInternal<K, V, SessionStore<Bytes, byte[]>>) materializedInternal).materialize();

                    topologyBuilder.addStateStore(sessionStoreBuilder, nodeName());
                    topologyBuilder.markSourceStoreAndTopic(sessionStoreBuilder, topicName);
                } else {
                    topologyBuilder.addStateStore(keyValueStoreBuilder, nodeName());
                    topologyBuilder.markSourceStoreAndTopic(keyValueStoreBuilder, topicName);
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
        private ProcessorParameters<?, V> processorParameters;
        private StateStoreType storeType;
        private boolean isGlobalKTable = false;
        private boolean isWindowedKTable = false;

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

        public TableSourceNodeBuilder<K, V> withProcessorParameters(final ProcessorParameters<?, V> processorParameters) {
            this.processorParameters = processorParameters;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withStoreType(StateStoreType storeType) {
          this.storeType = storeType;
          this.isWindowedKTable = storeType == StateStoreType.TIME_WINDOW_STORE || storeType == StateStoreType.SESSION_STORE;
          return this;
        }

        public TableSourceNodeBuilder<K, V> isGlobalKTable(final boolean isGlobaKTable) {
            this.isGlobalKTable = isGlobaKTable;
            return this;
        }

        public TableSourceNode<K, V> build() {
            return new TableSourceNode<>(nodeName,
                                         sourceName,
                                         topic,
                                         consumedInternal,
                                         materializedInternal,
                                         processorParameters,
                                         storeType,
                                         isGlobalKTable,
                                         isWindowedKTable);
        }
    }
}
