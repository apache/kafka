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
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KeyValueStoreMaterializer;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Arrays;

/**
 * Too much specific information to generalize so the KTable-KTable join requires a specific node.
 */
public class KTableKTableJoinNode<K, V1, V2, VR> extends BaseJoinProcessorNode<K, Change<V1>, Change<V2>, Change<VR>> {

    private final String[] joinThisStoreNames;
    private final String[] joinOtherStoreNames;
    private final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal;

    KTableKTableJoinNode(final String nodeName,
                         final ValueJoiner<? super Change<V1>, ? super Change<V2>, ? extends Change<VR>> valueJoiner,
                         final ProcessorParameters<K, Change<V1>> joinThisProcessorParameters,
                         final ProcessorParameters<K, Change<V2>> joinOtherProcessorParameters,
                         final ProcessorParameters<K, Change<VR>> joinMergeProcessorParameters,
                         final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal,
                         final String thisJoinSide,
                         final String otherJoinSide,
                         final String[] joinThisStoreNames,
                         final String[] joinOtherStoreNames) {

        super(nodeName,
              valueJoiner,
              joinThisProcessorParameters,
              joinOtherProcessorParameters,
              joinMergeProcessorParameters,
              thisJoinSide,
              otherJoinSide);

        this.joinThisStoreNames = joinThisStoreNames;
        this.joinOtherStoreNames = joinOtherStoreNames;
        this.materializedInternal = materializedInternal;
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

        if (materializedInternal != null) {
            final StoreBuilder<KeyValueStore<K, VR>> storeBuilder =
                new KeyValueStoreMaterializer<>(materializedInternal).materialize();
            topologyBuilder.addStateStore(storeBuilder, mergeProcessorName);
        }
    }

    @Override
    public String toString() {
        return "KTableKTableJoinNode{" +
               "joinThisStoreNames=" + Arrays.toString(joinThisStoreNames) +
               ", joinOtherStoreNames=" + Arrays.toString(joinOtherStoreNames) +
               ", materializedInternal=" + materializedInternal +
               "} " + super.toString();
    }

    public static <K, V1, V2, VR> KTableKTableJoinNodeBuilder<K, V1, V2, VR> kTableKTableJoinNodeBuilder() {
        return new KTableKTableJoinNodeBuilder<>();
    }

    public static final class KTableKTableJoinNodeBuilder<K, V1, V2, VR> {

        private String nodeName;
        private String[] joinThisStoreNames;
        private ProcessorParameters<K, Change<V1>> joinThisProcessorParameters;
        private String[] joinOtherStoreNames;
        private MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal;
        private ProcessorParameters<K, Change<V2>> joinOtherProcessorParameters;
        private ProcessorParameters<K, Change<VR>> joinMergeProcessorParameters;
        private ValueJoiner<? super Change<V1>, ? super Change<V2>, ? extends Change<VR>> valueJoiner;
        private String thisJoinSide;
        private String otherJoinSide;

        private KTableKTableJoinNodeBuilder() {
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinThisStoreNames(final String[] joinThisStoreNames) {
            this.joinThisStoreNames = joinThisStoreNames;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinThisProcessorParameters(final ProcessorParameters<K, Change<V1>> joinThisProcessorParameters) {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinOtherStoreNames(final String[] joinOtherStoreNames) {
            this.joinOtherStoreNames = joinOtherStoreNames;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinOtherProcessorParameters(final ProcessorParameters<K, Change<V2>> joinOtherProcessorParameters) {
            this.joinOtherProcessorParameters = joinOtherProcessorParameters;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinMergeProcessorParameters(final ProcessorParameters<K, Change<VR>> joinMergeProcessorParameters) {
            this.joinMergeProcessorParameters = joinMergeProcessorParameters;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withValueJoiner(final ValueJoiner<? super Change<V1>, ? super Change<V2>, ? extends Change<VR>> valueJoiner) {
            this.valueJoiner = valueJoiner;
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

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withMaterializedInternal(
                final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal) {
            this.materializedInternal = materializedInternal;
            return this;
        }

        public KTableKTableJoinNode<K, V1, V2, VR> build() {

            return new KTableKTableJoinNode<>(nodeName,
                                              valueJoiner,
                                              joinThisProcessorParameters,
                                              joinOtherProcessorParameters,
                                              joinMergeProcessorParameters,
                                              materializedInternal,
                                              thisJoinSide,
                                              otherJoinSide,
                                              joinThisStoreNames,
                                              joinOtherStoreNames);
        }
    }
}
