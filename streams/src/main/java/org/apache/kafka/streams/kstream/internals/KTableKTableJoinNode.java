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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Arrays;

/**
 * Too much specific information to generalize so the
 * KTable-KTable join requires a specific node.
 */
class KTableKTableJoinNode<K, V, V1, V2, VR> extends BaseJoinProcessorNode<K, V, V1, V2, VR> {

    private final String[] joinThisStoreNames;
    private final String[] joinOtherStoreNames;

    KTableKTableJoinNode(final String predecessorNodeName,
                         final String name,
                         final ValueJoiner<? super V, ? super V1, ? extends V2> valueJoiner,
                         final ProcessorParameters<K, V1> joinThisProcessorParameters,
                         final ProcessorParameters<K, V2> joinOtherProcessorParameters,
                         final ProcessorParameters<K, VR> joinMergeProcessorParameters,
                         final String thisJoinSide,
                         final String otherJoinSide,
                         final String[] joinThisStoreNames,
                         final String[] joinOtherStoreNames) {

        super(predecessorNodeName,
              name,
              valueJoiner,
              joinThisProcessorParameters,
              joinOtherProcessorParameters,
              joinMergeProcessorParameters,
              thisJoinSide,
              otherJoinSide);

        this.joinThisStoreNames = joinThisStoreNames;
        this.joinOtherStoreNames = joinOtherStoreNames;
    }

    String[] joinThisStoreNames() {
        return Arrays.copyOf(joinThisStoreNames, joinThisStoreNames.length);
    }

    String[] joinOtherStoreNames() {
        return Arrays.copyOf(joinOtherStoreNames, joinOtherStoreNames.length);
    }

    static <K, V, V1, V2, VR> KTableKTableJoinNodeBuilder<K, V, V1, V2, VR> kTableKTableJoinNodeBuilder() {
        return new KTableKTableJoinNodeBuilder<>();
    }

    static final class KTableKTableJoinNodeBuilder<K, V, V1, V2, VR> {

        private String name;
        private String predecessorNodeName;
        private String[] joinThisStoreNames;
        private ProcessorParameters<K, V1> joinThisProcessorParameters;
        private String[] joinOtherStoreNames;
        private ProcessorParameters<K, V2> joinOtherProcessorParameters;
        private ProcessorParameters<K, VR> joinMergeProcessorParameters;
        private ValueJoiner<? super V, ? super V1, ? extends V2> valueJoiner;
        private String thisJoinSide;
        private String otherJoinSide;

        private KTableKTableJoinNodeBuilder() {
        }

        KTableKTableJoinNodeBuilder withJoinThisStoreNames(final String[] joinThisStoreNames) {
            this.joinThisStoreNames = joinThisStoreNames;
            return this;
        }

        KTableKTableJoinNodeBuilder withJoinThisProcessorParameters(final ProcessorParameters<K, V1> joinThisProcessorParameters) {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        KTableKTableJoinNodeBuilder withName(String name) {
            this.name = name;
            return this;
        }

        KTableKTableJoinNodeBuilder withJoinOtherStoreNames(final String[] joinOtherStoreNames) {
            this.joinOtherStoreNames = joinOtherStoreNames;
            return this;
        }

        KTableKTableJoinNodeBuilder withPredecessorNodeName(final String predecessorNodeName) {
            this.predecessorNodeName = predecessorNodeName;
            return this;
        }

        KTableKTableJoinNodeBuilder withJoinOtherProcessorParameters(final ProcessorParameters<K, V2> joinOtherProcessorParameters) {
            this.joinOtherProcessorParameters = joinOtherProcessorParameters;
            return this;
        }

        KTableKTableJoinNodeBuilder withJoinMergeProcessorParameters(final ProcessorParameters<K, VR> joinMergeProcessorParameters) {
            this.joinMergeProcessorParameters = joinMergeProcessorParameters;
            return this;
        }

        KTableKTableJoinNodeBuilder withValueJoiner(final ValueJoiner<? super V, ? super V1, ? extends V2> valueJoiner) {
            this.valueJoiner = valueJoiner;
            return this;
        }

        KTableKTableJoinNodeBuilder withThisJoinSide(final String thisJoinSide) {
            this.thisJoinSide = thisJoinSide;
            return this;
        }

        KTableKTableJoinNodeBuilder withOtherJoinSide(final String otherJoinSide) {
            this.otherJoinSide = otherJoinSide;
            return this;
        }

        KTableKTableJoinNode<K, V, V1, V2, VR> build() {

            return new KTableKTableJoinNode<>(predecessorNodeName,
                                              name, valueJoiner,
                                              joinThisProcessorParameters,
                                              joinOtherProcessorParameters,
                                              joinMergeProcessorParameters,
                                              thisJoinSide,
                                              otherJoinSide,
                                              joinThisStoreNames,
                                              joinOtherStoreNames);
        }
    }
}
