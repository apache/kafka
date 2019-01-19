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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder;
import org.apache.kafka.streams.kstream.internals.KTableKTableJoinMerger;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Too much specific information to generalize so the KTable-KTable join requires a specific node.
 */
public abstract class KTableKTableJoinNode<K, V1, V2, VR> extends BaseJoinProcessorNode<K, Change<V1>, Change<V2>, Change<VR>> {

    private final Serde<K> keySerde;
    private final String[] joinThisStoreNames;
    private final String[] joinOtherStoreNames;

    KTableKTableJoinNode(final String nodeName,
                         final ValueJoiner<? super Change<V1>, ? super Change<V2>, ? extends Change<VR>> valueJoiner,
                         final ProcessorParameters<K, Change<V1>> joinThisProcessorParameters,
                         final ProcessorParameters<K, Change<V2>> joinOtherProcessorParameters,
                         final ProcessorParameters<K, Change<VR>> joinMergeProcessorParameters,
                         final String thisJoinSide,
                         final String otherJoinSide,
                         final Serde<K> keySerde,
                         final String[] joinThisStoreNames,
                         final String[] joinOtherStoreNames) {

        super(nodeName,
              valueJoiner,
              joinThisProcessorParameters,
              joinOtherProcessorParameters,
              joinMergeProcessorParameters,
              thisJoinSide,
              otherJoinSide);

        this.keySerde = keySerde;
        this.joinThisStoreNames = joinThisStoreNames;
        this.joinOtherStoreNames = joinOtherStoreNames;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<VR> valueSerde() {
        return null;
    }

    public String[] joinThisStoreNames() {
        return joinThisStoreNames;
    }

    public String[] joinOtherStoreNames() {
        return joinOtherStoreNames;
    }

    /**
     * The name of queryableStore, which stores the join results.
     */
    public abstract String queryableStoreName();

    /**
     * The supplier which provides processor with KTable-KTable join merge functionality.
     */
    public KTableKTableJoinMerger<K, VR> joinMerger() {
        return (KTableKTableJoinMerger<K, VR>) mergeProcessorParameters().processorSupplier();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        final String thisProcessorName = thisProcessorParameters().processorName();
        final String otherProcessorName = otherProcessorParameters().processorName();
        final String mergeProcessorName = mergeProcessorParameters().processorName();

        topologyBuilder.addProcessor(thisProcessorName,
                                     thisProcessorParameters().processorSupplier(),
                                     thisJoinSideNodeName());

        topologyBuilder.addProcessor(otherProcessorName,
                                     otherProcessorParameters().processorSupplier(),
                                     otherJoinSideNodeName());

        topologyBuilder.addProcessor(mergeProcessorName,
                                     mergeProcessorParameters().processorSupplier(),
                                     thisProcessorName,
                                     otherProcessorName);

        topologyBuilder.connectProcessorAndStateStores(thisProcessorName,
                                                       joinOtherStoreNames);
        topologyBuilder.connectProcessorAndStateStores(otherProcessorName,
                                                       joinThisStoreNames);
    }

    public static <K, V1, V2, VR> KTableKTableJoinNodeBuilder<K, V1, V2, VR> kTableKTableJoinNodeBuilder(final InternalStreamsBuilder builder) {
        return new KTableKTableJoinNodeBuilder<>(builder);
    }

    public static final class KTableKTableJoinNodeBuilder<K, V1, V2, VR> {

        private final InternalStreamsBuilder builder;
        private String nodeName;
        private ProcessorParameters<K, Change<V1>> joinThisProcessorParameters;
        private ProcessorParameters<K, Change<V2>> joinOtherProcessorParameters;
        private ValueJoiner<? super Change<V1>, ? super Change<V2>, ? extends Change<VR>> valueJoiner;
        private String thisJoinSide;
        private String otherJoinSide;
        private Serde<K> keySerde;
        private String[] joinThisStoreNames;
        private String[] joinOtherStoreNames;
        private MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal;

        private KTableKTableJoinNodeBuilder(final InternalStreamsBuilder builder) {
            this.builder = builder;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withValueJoiner(final ValueJoiner<? super Change<V1>, ? super Change<V2>, ? extends Change<VR>> valueJoiner) {
            this.valueJoiner = valueJoiner;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinThisProcessorParameters(final ProcessorParameters<K, Change<V1>> joinThisProcessorParameters) {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinOtherProcessorParameters(final ProcessorParameters<K, Change<V2>> joinOtherProcessorParameters) {
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

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinThisStoreNames(final String[] joinThisStoreNames) {
            this.joinThisStoreNames = joinThisStoreNames;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinOtherStoreNames(final String[] joinOtherStoreNames) {
            this.joinOtherStoreNames = joinOtherStoreNames;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withMaterializedInternal(
                final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal) {
            this.materializedInternal = materializedInternal;
            return this;
        }

        public KTableKTableJoinNode<K, V1, V2, VR> build() {
            // only materialize if specified in Materialized
            if (materializedInternal == null) {
                return new NonMaterializedKTableKTableJoinNode<>(nodeName,
                    valueJoiner,
                    joinThisProcessorParameters,
                    joinOtherProcessorParameters,
                    thisJoinSide,
                    otherJoinSide,
                    keySerde,
                    joinThisStoreNames,
                    joinOtherStoreNames);
            } else {
                return new MaterializedKTableKTableJoinNode<>(nodeName,
                    valueJoiner,
                    joinThisProcessorParameters,
                    joinOtherProcessorParameters,
                    thisJoinSide,
                    otherJoinSide,
                    keySerde,
                    joinThisStoreNames,
                    joinOtherStoreNames,
                    materializedInternal);
            }
        }
    }
}
