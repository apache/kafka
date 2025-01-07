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
import org.apache.kafka.streams.kstream.internals.KTableKTableAbstractJoin;
import org.apache.kafka.streams.kstream.internals.KTableKTableJoinMerger;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.Arrays;

/**
 * Too much specific information to generalize so the KTable-KTable join requires a specific node.
 */
public class KTableKTableJoinNode<K, V1, V2, VR> extends BaseJoinProcessorNode<K, Change<V1>, Change<V2>, Change<VR>> implements VersionedSemanticsGraphNode {

    private final Serde<K> keySerde;
    private final Serde<VR> valueSerde;
    private final String[] joinThisStoreNames;
    private final String[] joinOtherStoreNames;

    KTableKTableJoinNode(final String nodeName,
                         final ProcessorParameters<K, Change<V1>, ?, ?> joinThisProcessorParameters,
                         final ProcessorParameters<K, Change<V2>, ?, ?> joinOtherProcessorParameters,
                         final ProcessorParameters<K, Change<VR>, ?, ?> joinMergeProcessorParameters,
                         final String thisJoinSide,
                         final String otherJoinSide,
                         final Serde<K> keySerde,
                         final Serde<VR> valueSerde,
                         final String[] joinThisStoreNames,
                         final String[] joinOtherStoreNames) {

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
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<VR> valueSerde() {
        return valueSerde;
    }

    public String[] joinThisStoreNames() {
        return joinThisStoreNames;
    }

    public String[] joinOtherStoreNames() {
        return joinOtherStoreNames;
    }

    public String queryableStoreName() {
        return joinMerger().queryableName();
    }

    /**
     * The supplier which provides processor with KTable-KTable join merge functionality.
     */
    @SuppressWarnings("unchecked")
    public KTableKTableJoinMerger<K, VR> joinMerger() {
        final ProcessorSupplier<K, Change<VR>, K, Change<VR>> kChangeProcessorSupplier = (ProcessorSupplier<K, Change<VR>, K, Change<VR>>) mergeProcessorParameters().processorSupplier();
        return (KTableKTableJoinMerger<K, VR>) kChangeProcessorSupplier;
    }

    @Override
    public void enableVersionedSemantics(final boolean useVersionedSemantics, final String parentNodeName) {
        enableVersionedSemantics(thisProcessorParameters(), useVersionedSemantics, parentNodeName);
        enableVersionedSemantics(otherProcessorParameters(), useVersionedSemantics, parentNodeName);
    }

    @SuppressWarnings("unchecked")
    private void enableVersionedSemantics(final ProcessorParameters<K, ?, ?, ?> processorParameters,
                                          final boolean useVersionedSemantics,
                                          final String parentNodeName) {
        final ProcessorSupplier<K, ?, ?, ?> processorSupplier = processorParameters.processorSupplier();
        if (!(processorSupplier instanceof KTableKTableAbstractJoin)) {
            throw new IllegalStateException("Unexpected processor type for table-table join: " + processorSupplier.getClass().getName());
        }
        final KTableKTableAbstractJoin<K, ?, ?, ?> tableJoin = (KTableKTableAbstractJoin<K, ?, ?, ?>) processorSupplier;

        if (parentNodeName.equals(tableJoin.joinThisParentNodeName())) {
            tableJoin.setUseVersionedSemantics(useVersionedSemantics);
        }
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        final String thisProcessorName = thisProcessorParameters().processorName();
        final String otherProcessorName = otherProcessorParameters().processorName();

        thisProcessorParameters().addProcessorTo(topologyBuilder, thisJoinSideNodeName());
        otherProcessorParameters().addProcessorTo(topologyBuilder, otherJoinSideNodeName());
        mergeProcessorParameters().addProcessorTo(topologyBuilder, thisProcessorName, otherProcessorName);

        topologyBuilder.connectProcessorAndStateStores(thisProcessorName, joinOtherStoreNames);
        topologyBuilder.connectProcessorAndStateStores(otherProcessorName, joinThisStoreNames);
    }

    @Override
    public String toString() {
        return "KTableKTableJoinNode{" +
            "joinThisStoreNames=" + Arrays.toString(joinThisStoreNames()) +
            ", joinOtherStoreNames=" + Arrays.toString(joinOtherStoreNames()) +
            "} " + super.toString();
    }

    public static <K, V1, V2, VR> KTableKTableJoinNodeBuilder<K, V1, V2, VR> kTableKTableJoinNodeBuilder() {
        return new KTableKTableJoinNodeBuilder<>();
    }

    public static final class KTableKTableJoinNodeBuilder<K, V1, V2, VR> {
        private String nodeName;
        private ProcessorParameters<K, Change<V1>, ?, ?> joinThisProcessorParameters;
        private ProcessorParameters<K, Change<V2>, ?, ?> joinOtherProcessorParameters;
        private String thisJoinSide;
        private String otherJoinSide;
        private Serde<K> keySerde;
        private Serde<VR> valueSerde;
        private String[] joinThisStoreNames;
        private String[] joinOtherStoreNames;
        private ProcessorParameters<K, Change<VR>, ?, ?>
                joinMergeProcessorParameters;

        private KTableKTableJoinNodeBuilder() {
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinThisProcessorParameters(final ProcessorParameters<K, Change<V1>, ?, ?> joinThisProcessorParameters) {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinOtherProcessorParameters(final ProcessorParameters<K, Change<V2>, ?, ?> joinOtherProcessorParameters) {
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

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withMergeProcessorParameters(final ProcessorParameters<K, Change<VR>, ?, ?> joinMergeProcessorParameters) {
            this.joinMergeProcessorParameters = joinMergeProcessorParameters;
            return this;
        }

        public KTableKTableJoinNode<K, V1, V2, VR> build() {
            return new KTableKTableJoinNode<>(
                nodeName,
                joinThisProcessorParameters,
                joinOtherProcessorParameters,
                joinMergeProcessorParameters,
                thisJoinSide,
                otherJoinSide,
                keySerde,
                valueSerde,
                joinThisStoreNames,
                joinOtherStoreNames
            );
        }
    }
}
