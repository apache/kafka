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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableKTableJoinMerger;
import org.apache.kafka.streams.kstream.internals.KTableProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

import java.util.Arrays;

/**
 * Too much specific information to generalize so the KTable-KTable join requires a specific node.
 */
public class KTableKTableJoinNode<K, V1, V2, VR> extends StreamsGraphNode<K, Change<V1>, K, Change<VR>> {

    private final Serde<K> keySerde;
    private final Serde<VR> valueSerde;
    private final String[] joinThisStoreNames;
    private final String[] joinOtherStoreNames;
    private final StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;
    private final ProcessorParameters<K, Change<V1>, K, Change<VR>> joinThisProcessorParameters;
    private final ProcessorParameters<K, Change<V2>, K, Change<VR>> joinOtherProcessorParameters;
    private final ProcessorParameters<K, Change<VR>, K, Change<VR>> joinMergeProcessorParameters;
    private final ValueJoiner<? super Change<V1>, ? super Change<V2>, ? extends Change<VR>> valueJoiner;
    private final String thisJoinSideNodeName;
    private final String otherJoinSideNodeName;

    private KTableKTableJoinNode(final String nodeName,
                                 final ProcessorParameters<K, Change<V1>, K, Change<VR>> joinThisProcessorParameters,
                                 final ProcessorParameters<K, Change<V2>, K, Change<VR>> joinOtherProcessorParameters,
                                 final ProcessorParameters<K, Change<VR>, K, Change<VR>> joinMergeProcessorParameters,
                                 final String thisJoinSide,
                                 final String otherJoinSide,
                                 final Serde<K> keySerde,
                                 final Serde<VR> valueSerde,
                                 final String[] joinThisStoreNames,
                                 final String[] joinOtherStoreNames,
                                 final StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder) {

        super(nodeName);

        this.valueJoiner = null;
        this.joinThisProcessorParameters = joinThisProcessorParameters;
        this.joinOtherProcessorParameters = joinOtherProcessorParameters;
        this.joinMergeProcessorParameters = joinMergeProcessorParameters;
        this.thisJoinSideNodeName = thisJoinSide;
        this.otherJoinSideNodeName = otherJoinSide;

        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.joinThisStoreNames = joinThisStoreNames;
        this.joinOtherStoreNames = joinOtherStoreNames;
        this.storeBuilder = storeBuilder;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<VR> valueSerde() {
        return valueSerde;
    }

    public String queryableStoreName() {
        return ((KTableKTableJoinMerger<K, VR>) joinMergeProcessorParameters.processorSupplier()).getQueryableName();
    }

    /**
     * The supplier which provides processor with KTable-KTable join merge functionality.
     */
    public KTableKTableJoinMerger<K, VR> joinMerger() {
        return (KTableKTableJoinMerger<K, VR>) joinMergeProcessorParameters.processorSupplier();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        final String thisProcessorName = joinThisProcessorParameters.processorName();
        final String otherProcessorName = joinOtherProcessorParameters.processorName();
        final String mergeProcessorName = joinMergeProcessorParameters.processorName();

        topologyBuilder.addProcessor(
            thisProcessorName,
            joinThisProcessorParameters.processorSupplier(),
            thisJoinSideNodeName);

        topologyBuilder.addProcessor(
            otherProcessorName,
            joinOtherProcessorParameters.processorSupplier(),
            otherJoinSideNodeName);

        topologyBuilder.addProcessor(
            mergeProcessorName,
            joinMergeProcessorParameters.processorSupplier(),
            thisProcessorName,
            otherProcessorName);

        topologyBuilder.connectProcessorAndStateStores(thisProcessorName, joinOtherStoreNames);
        topologyBuilder.connectProcessorAndStateStores(otherProcessorName, joinThisStoreNames);

        if (storeBuilder != null) {
            topologyBuilder.addStateStore(storeBuilder, mergeProcessorName);
        }
    }

    @Override
    public String toString() {
        return "KTableKTableJoinNode{" +
            "keySerde=" + keySerde +
            ", valueSerde=" + valueSerde +
            ", joinThisStoreNames=" + Arrays.toString(joinThisStoreNames) +
            ", joinOtherStoreNames=" + Arrays.toString(joinOtherStoreNames) +
            ", storeBuilder=" + storeBuilder +
            ", joinThisProcessorParameters=" + joinThisProcessorParameters +
            ", joinOtherProcessorParameters=" + joinOtherProcessorParameters +
            ", joinMergeProcessorParameters=" + joinMergeProcessorParameters +
            ", valueJoiner=" + valueJoiner +
            ", thisJoinSideNodeName='" + thisJoinSideNodeName + '\'' +
            ", otherJoinSideNodeName='" + otherJoinSideNodeName + '\'' +
            '}';
    }

    public static <K, V1, V2, VR> KTableKTableJoinNodeBuilder<K, V1, V2, VR> kTableKTableJoinNodeBuilder() {
        return new KTableKTableJoinNodeBuilder<>();
    }

    public static final class KTableKTableJoinNodeBuilder<K, V1, V2, VR> {
        private String nodeName;
        private ProcessorParameters<K, Change<V1>, K, Change<VR>> joinThisProcessorParameters;
        private ProcessorParameters<K, Change<V2>, K, Change<VR>> joinOtherProcessorParameters;
        private String thisJoinSide;
        private String otherJoinSide;
        private Serde<K> keySerde;
        private Serde<VR> valueSerde;
        private String[] joinThisStoreNames;
        private String[] joinOtherStoreNames;
        private String queryableStoreName;
        private StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;

        private KTableKTableJoinNodeBuilder() {
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinThisProcessorParameters(final ProcessorParameters<K, Change<V1>, K, Change<VR>> joinThisProcessorParameters) {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinOtherProcessorParameters(final ProcessorParameters<K, Change<V2>, K, Change<VR>> joinOtherProcessorParameters) {
            this.joinOtherProcessorParameters = joinOtherProcessorParameters;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withThisJoinSideNodeName(final String thisJoinSide) {
            this.thisJoinSide = thisJoinSide;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withOtherJoinSideNodeName(final String otherJoinSide) {
            this.otherJoinSide = otherJoinSide;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withKeySerde(final Serde<K> keySerde) {
            this.keySerde = keySerde;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withValueSerde(final Serde<VR> valueSerde) {
            this.valueSerde = valueSerde;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinThisStoreNames(final String[] joinThisStoreNames) {
            this.joinThisStoreNames = joinThisStoreNames;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinOtherStoreNames(final String[] joinOtherStoreNames) {
            this.joinOtherStoreNames = joinOtherStoreNames;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withQueryableStoreName(final String queryableStoreName) {
            this.queryableStoreName = queryableStoreName;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withStoreBuilder(final StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder) {
            this.storeBuilder = storeBuilder;
            return this;
        }

        public KTableKTableJoinNode<K, V1, V2, VR> build() {
            return new KTableKTableJoinNode<>(nodeName,
                joinThisProcessorParameters,
                joinOtherProcessorParameters,
                new ProcessorParameters<>(
                    new KTableKTableJoinMerger<>(
                        (KTableProcessorSupplier<K, Change<V1>, K, VR>) (joinThisProcessorParameters.processorSupplier()),
                        (KTableProcessorSupplier<K, Change<V2>, K, VR>) (joinOtherProcessorParameters.processorSupplier()),
                        queryableStoreName
                    ),
                    nodeName),
                thisJoinSide,
                otherJoinSide,
                keySerde,
                valueSerde,
                joinThisStoreNames,
                joinOtherStoreNames,
                storeBuilder);
        }
    }
}
