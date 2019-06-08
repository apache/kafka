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

import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.StateStoreType;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.SessionStoreMaterializer;
import org.apache.kafka.streams.kstream.internals.TimestampedKeyValueStoreMaterializer;
import org.apache.kafka.streams.kstream.internals.TimestampedWindowStoreMaterializer;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Collections;
import java.util.Optional;

/**
 * Used to represent either a KTable source or a GlobalKTable source. A boolean flag is used to indicate if this represents a GlobalKTable a {@link
 * org.apache.kafka.streams.kstream.GlobalKTable}
 */
public class TableSourceNode<K, V> extends StreamSourceNode<K, V> {

    private final String sourceName;
    private final MaterializedInternal<K, V, ?> materializedInternal;
    private final ProcessorParameters<V> processorParameters;
    private final boolean isGlobalKTable;
    private final StateStoreType stateStoreType;
    private final Optional<Windows<?>> timeWindows;
    private final Optional<SessionWindows> sessionWindow;
    private boolean shouldReuseSourceTopicForChangelog = false;

    private TableSourceNode(final String nodeName,
                            final String sourceName,
                            final String topic,
                            final ConsumedInternal<K, V> consumedInternal,
                            final MaterializedInternal<K, V, ?> materializedInternal,
                            final ProcessorParameters<V> processorParameters,
                            final boolean isGlobalKTable,
                            final StateStoreType stateStoreType,
                            final Windows<?> timeWindows,
                            final SessionWindows sessionWindow) {

        super(nodeName,
              Collections.singletonList(topic),
              consumedInternal);

        this.sourceName = sourceName;
        this.isGlobalKTable = isGlobalKTable;
        this.processorParameters = processorParameters;
        this.materializedInternal = materializedInternal;
        this.stateStoreType = stateStoreType;
        this.timeWindows = Optional.ofNullable(timeWindows);
        this.sessionWindow = Optional.ofNullable(sessionWindow);
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

    public static <K, V> TableSourceNodeBuilder<K, V> tableSourceNodeBuilder(StateStoreType stateStoreType) {
        return new TableSourceNodeBuilder<>(stateStoreType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        final String topicName = getTopicNames().iterator().next();

        StoreBuilder<?> storeBuilder;
        if (stateStoreType == StateStoreType.KEY_VALUE_STORE) {
            storeBuilder = new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize();
        } else if (stateStoreType == StateStoreType.TIME_WINDOW_STORE) {
            if (!timeWindows.isPresent()) {
                throw new IllegalArgumentException("Window store type KTable doesn't have time window defined");
            }
            storeBuilder = new TimestampedWindowStoreMaterializer(materializedInternal, timeWindows.get()).materialize();
        } else {
            if (!sessionWindow.isPresent()) {
                throw new IllegalArgumentException("Session store type KTable doesn't have session window defined");
            }
            storeBuilder = new SessionStoreMaterializer<>(materializedInternal, sessionWindow.get()).materialize();
        }

        if (isGlobalKTable) {
            topologyBuilder.addGlobalStore(storeBuilder,
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
                topologyBuilder.addStateStore(storeBuilder, nodeName());

                if (shouldReuseSourceTopicForChangelog) {
                    storeBuilder.withLoggingDisabled();
                    topologyBuilder.connectSourceStoreAndTopic(storeBuilder.name(), topicName);
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
        private ProcessorParameters<V> processorParameters;
        private boolean isGlobalKTable = false;
        private final StateStoreType stateStoreType;
        private Windows<?> timeWindows;
        private SessionWindows sessionWindow;

        private TableSourceNodeBuilder(StateStoreType stateStoreType) {
            this.stateStoreType = stateStoreType;
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

        public TableSourceNodeBuilder<K, V> withProcessorParameters(final ProcessorParameters<V> processorParameters) {
            this.processorParameters = processorParameters;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public TableSourceNodeBuilder<K, V> isGlobalKTable(final boolean isGlobaKTable) {
            this.isGlobalKTable = isGlobaKTable;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withTimeWindows(final Windows<?> timeWindows) {
            this.timeWindows = timeWindows;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withSessionWindows(final SessionWindows sessionWindow) {
            this.sessionWindow = sessionWindow;
            return this;
        }

        public TableSourceNode<K, V> build() {
            return new TableSourceNode<>(nodeName,
                                         sourceName,
                                         topic,
                                         consumedInternal,
                                         materializedInternal,
                                         processorParameters,
                                         isGlobalKTable,
                                         stateStoreType,
                                         timeWindows,
                                         sessionWindow);
        }
    }
}
