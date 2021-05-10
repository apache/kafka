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
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableKTableJoinMerger;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

import java.util.Arrays;
import java.util.Properties;

/**
 * Too much specific information to generalize so the KTable-KTable join requires a specific node.
 */
public class KTableKTableJoinNode<K, V, V1, VOut> extends
    BaseJoinProcessorNode<K, Change<V>, Change<V1>, Change<VOut>> {

    private final Serde<K> keySerde;
    private final Serde<VOut> valueSerde;
    private final String[] joinThisStoreNames;
    private final String[] joinOtherStoreNames;
    private final StoreBuilder<TimestampedKeyValueStore<K, VOut>> storeBuilder;

    KTableKTableJoinNode(final String nodeName,
                         final ProcessorParameters<K, Change<V>, ?, ?> joinThisProcessorParameters,
                         final ProcessorParameters<K, Change<V1>, ?, ?> joinOtherProcessorParameters,
                         final ProcessorParameters<K, Change<VOut>, ?, ?> joinMergeProcessorParameters,
                         final String thisJoinSide,
                         final String otherJoinSide,
                         final Serde<K> keySerde,
                         final Serde<VOut> valueSerde,
                         final String[] joinThisStoreNames,
                         final String[] joinOtherStoreNames,
                         final StoreBuilder<TimestampedKeyValueStore<K, VOut>> storeBuilder) {

        super(nodeName,
            null,
            joinThisProcessorParameters,
            joinOtherProcessorParameters,
            joinMergeProcessorParameters,
            thisJoinSide,
            otherJoinSide);

        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.joinThisStoreNames = joinThisStoreNames;
        this.joinOtherStoreNames = joinOtherStoreNames;
        this.storeBuilder = storeBuilder;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<VOut> valueSerde() {
        return valueSerde;
    }

    public String[] joinThisStoreNames() {
        return joinThisStoreNames;
    }

    public String[] joinOtherStoreNames() {
        return joinOtherStoreNames;
    }

    public String queryableStoreName() {
        return mergeProcessorParameters().kTableKTableJoinMergerProcessorSupplier().getQueryableName();
    }

    /**
     * The supplier which provides processor with KTable-KTable join merge functionality.
     */
    @SuppressWarnings("unchecked")
    public KTableKTableJoinMerger<K, VOut> joinMerger() {
        final KTableKTableJoinMerger<K, Change<VOut>> merger =
            mergeProcessorParameters().kTableKTableJoinMergerProcessorSupplier();
        // this incorrect cast should be corrected by the end of the KIP-478 implementation
        return (KTableKTableJoinMerger<K, VOut>) merger;
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder, final Properties props) {
        final String thisProcessorName = thisProcessorParameters().processorName();
        final String otherProcessorName = otherProcessorParameters().processorName();
        final String mergeProcessorName = mergeProcessorParameters().processorName();

        topologyBuilder.addProcessor(
            thisProcessorName,
            thisProcessorParameters().processorSupplier(),
            thisJoinSideNodeName());

        topologyBuilder.addProcessor(
            otherProcessorName,
            otherProcessorParameters().processorSupplier(),
            otherJoinSideNodeName());

        topologyBuilder.addProcessor(
            mergeProcessorName,
            mergeProcessorParameters().processorSupplier(),
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
            "joinThisStoreNames=" + Arrays.toString(joinThisStoreNames()) +
            ", joinOtherStoreNames=" + Arrays.toString(joinOtherStoreNames()) +
            "} " + super.toString();
    }

    public static <K, V, V1, VOut> KTableKTableJoinNodeBuilder<K, V, V1, VOut> kTableKTableJoinNodeBuilder() {
        return new KTableKTableJoinNodeBuilder<>();
    }

    public static final class KTableKTableJoinNodeBuilder<K, V, V1, VOut> {

        private String nodeName;
        private ProcessorParameters<K, Change<V>, ?, ?> joinThisProcessorParameters;
        private ProcessorParameters<K, Change<V1>, ?, ?> joinOtherProcessorParameters;
        private String thisJoinSide;
        private String otherJoinSide;
        private Serde<K> keySerde;
        private Serde<VOut> valueSerde;
        private String[] joinThisStoreNames;
        private String[] joinOtherStoreNames;
        private String queryableStoreName;
        private StoreBuilder<TimestampedKeyValueStore<K, VOut>> storeBuilder;

        private KTableKTableJoinNodeBuilder() {
        }

        public KTableKTableJoinNodeBuilder<K, V, V1, VOut> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V, V1, VOut> withJoinThisProcessorParameters(
            final ProcessorParameters<K, Change<V>, ?, ?> joinThisProcessorParameters) {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V, V1, VOut> withJoinOtherProcessorParameters(
            final ProcessorParameters<K, Change<V1>, ?, ?> joinOtherProcessorParameters) {
            this.joinOtherProcessorParameters = joinOtherProcessorParameters;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V, V1, VOut> withThisJoinSideNodeName(
            final String thisJoinSide) {
            this.thisJoinSide = thisJoinSide;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V, V1, VOut> withOtherJoinSideNodeName(
            final String otherJoinSide) {
            this.otherJoinSide = otherJoinSide;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V, V1, VOut> withKeySerde(final Serde<K> keySerde) {
            this.keySerde = keySerde;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V, V1, VOut> withValueSerde(
            final Serde<VOut> valueSerde) {
            this.valueSerde = valueSerde;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V, V1, VOut> withJoinThisStoreNames(
            final String[] joinThisStoreNames) {
            this.joinThisStoreNames = joinThisStoreNames;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V, V1, VOut> withJoinOtherStoreNames(
            final String[] joinOtherStoreNames) {
            this.joinOtherStoreNames = joinOtherStoreNames;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V, V1, VOut> withQueryableStoreName(
            final String queryableStoreName) {
            this.queryableStoreName = queryableStoreName;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V, V1, VOut> withStoreBuilder(
            final StoreBuilder<TimestampedKeyValueStore<K, VOut>> storeBuilder) {
            this.storeBuilder = storeBuilder;
            return this;
        }

        public KTableKTableJoinNode<K, V, V1, VOut> build() {
            return new KTableKTableJoinNode<>(
                nodeName,
                joinThisProcessorParameters,
                joinOtherProcessorParameters,
                new ProcessorParameters<>(
                    KTableKTableJoinMerger.of(
                        joinThisProcessorParameters.kTableChangeProcessorSupplier(),
                        joinOtherProcessorParameters.kTableChangeProcessorSupplier(),
                        queryableStoreName),
                    nodeName),
                thisJoinSide,
                otherJoinSide,
                keySerde,
                valueSerde,
                joinThisStoreNames,
                joinOtherStoreNames,
                storeBuilder
            );
        }
    }
}
