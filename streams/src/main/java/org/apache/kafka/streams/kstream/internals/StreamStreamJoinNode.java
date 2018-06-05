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

import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Too much information to generalize, so Stream-Stream joins are
 * represented by a specific node.
 */
class StreamStreamJoinNode<K, V1, V2, VR> extends BaseJoinProcessorNode<K, V1, V2, VR> {

    private final ProcessorSupplier<K, V1> thisWindowedStreamProcessorSupplier;
    private final ProcessorSupplier<K, V2> otherWindowedStreamProcessorSupplier;
    private final StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder;
    private final StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder;
    private final Joined<K, V1, V2> joined;

    private final String thisWindowedStreamName;
    private final String otherWindowedStreamName;


    StreamStreamJoinNode(final String parentProcessorNodeName,
                         final String processorNodeName,
                         final ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner,
                         final ProcessorParameters<K, V1> joinThisProcessorParameters,
                         final ProcessorParameters<K, V2> joinOtherProcessParameters,
                         final ProcessorParameters<K, VR> joinMergeProcessorParameters,
                         final ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters,
                         final ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters,
                         final StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder,
                         final StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder,
                         final Joined<K, V1, V2> joined,
                         final String leftHandSideStreamName,
                         final String otherStreamName) {

        super(parentProcessorNodeName,
              processorNodeName,
              valueJoiner,
              joinThisProcessorParameters,
              joinOtherProcessParameters,
              joinMergeProcessorParameters,
              leftHandSideStreamName,
              otherStreamName);

        this.thisWindowedStreamProcessorSupplier = thisWindowedStreamProcessorParameters.processorSupplier();
        this.otherWindowedStreamProcessorSupplier = otherWindowedStreamProcessorParameters.processorSupplier();
        this.thisWindowedStreamName = thisWindowedStreamProcessorParameters.processorName();
        this.otherWindowedStreamName = otherWindowedStreamProcessorParameters.processorName();
        this.thisWindowStoreBuilder = thisWindowStoreBuilder;
        this.otherWindowStoreBuilder = otherWindowStoreBuilder;
        this.joined = joined;
    }

    ProcessorSupplier<K, V1> thisWindowedStreamProcessorSupplier() {
        return thisWindowedStreamProcessorSupplier;
    }

    ProcessorSupplier<K, V2> otherWindowedStreamProcessorSupplier() {
        return otherWindowedStreamProcessorSupplier;
    }

    String thisWindowedStreamName() {
        return thisWindowedStreamName;
    }

    String otherWindowedStreamName() {
        return otherWindowedStreamName;
    }

    StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder() {
        return thisWindowStoreBuilder;
    }

    StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder() {
        return otherWindowStoreBuilder;
    }

    @Override
    void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        //TODO will implement in follow-up pr
    }

    static <K, V, V1, V2, VR> StreamStreamJoinNodeBuilder<K, V1, V2, VR> streamStreamJoinNodeBuilder() {
        return new StreamStreamJoinNodeBuilder<>();
    }

    static final class StreamStreamJoinNodeBuilder<K, V1, V2, VR> {

        private String processorNodeName;
        private String parentProcessorNodeName;
        private ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner;
        private ProcessorParameters<K, V1> joinThisProcessorParameters;
        private ProcessorParameters<K, V2> joinOtherProcessorParameters;
        private ProcessorParameters<K, VR> joinMergeProcessorParameters;
        private ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters;
        private ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters;
        private StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder;
        private StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder;
        private Joined<K, V1, V2> joined;
        private String leftHandSideStreamName;
        private String otherStreamName;


        private StreamStreamJoinNodeBuilder() {
        }


        StreamStreamJoinNodeBuilder<K, V1, V2, VR> withValueJoiner(final ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner) {
            this.valueJoiner = valueJoiner;
            return this;
        }

        StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinThisProcessorParameters(final ProcessorParameters<K, V1> joinThisProcessorParameters) {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        StreamStreamJoinNodeBuilder<K, V1, V2, VR> withThisWindowedStreamProcessorParameters(final ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters) {
            this.thisWindowedStreamProcessorParameters = thisWindowedStreamProcessorParameters;
            return this;
        }

        StreamStreamJoinNodeBuilder<K, V1, V2, VR> withProcessorNodeName(final String name) {
            this.processorNodeName = name;
            return this;
        }

        StreamStreamJoinNodeBuilder<K, V1, V2, VR> withParentProcessorNodeName(final String predecessorNodeName) {
            this.parentProcessorNodeName = predecessorNodeName;
            return this;
        }

        StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinOtherProcessorParameters(final ProcessorParameters<K, V2> joinOtherProcessParameters) {
            this.joinOtherProcessorParameters = joinOtherProcessParameters;
            return this;
        }

        StreamStreamJoinNodeBuilder<K, V1, V2, VR> withOtherWindowedStreamProcessorParameters(final ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters) {
            this.otherWindowedStreamProcessorParameters = otherWindowedStreamProcessorParameters;
            return this;
        }

        StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinMergeProcessorParameters(final ProcessorParameters<K, VR> joinMergeProcessorParameters) {
            this.joinMergeProcessorParameters = joinMergeProcessorParameters;
            return this;
        }

        StreamStreamJoinNodeBuilder<K, V1, V2, VR> withLeftHandSideStreamName(final String leftHandSideStreamName) {
            this.leftHandSideStreamName = leftHandSideStreamName;
            return this;
        }

        StreamStreamJoinNodeBuilder<K, V1, V2, VR> withOtherStreamName(final String otherStreamName) {
            this.otherStreamName = otherStreamName;
            return this;
        }

        StreamStreamJoinNodeBuilder<K, V1, V2, VR> withThisWindowStoreBuilder(final StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder) {
            this.thisWindowStoreBuilder = thisWindowStoreBuilder;
            return this;
        }

        StreamStreamJoinNodeBuilder<K, V1, V2, VR> withOtherWindowStoreBuilder(final StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder) {
            this.otherWindowStoreBuilder = otherWindowStoreBuilder;
            return this;
        }

        StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoined(final Joined<K, V1, V2> joined) {
            this.joined = joined;
            return this;
        }

        StreamStreamJoinNode<K, V1, V2, VR> build() {

            return new StreamStreamJoinNode<>(parentProcessorNodeName,
                                              processorNodeName,
                                              valueJoiner,
                                              joinThisProcessorParameters,
                                              joinOtherProcessorParameters,
                                              joinMergeProcessorParameters,
                                              thisWindowedStreamProcessorParameters,
                                              otherWindowedStreamProcessorParameters,
                                              thisWindowStoreBuilder,
                                              otherWindowStoreBuilder,
                                              joined,
                                              leftHandSideStreamName,
                                              otherStreamName);


        }
    }
}
