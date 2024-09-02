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

import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.StoreFactory;

import java.util.Optional;

/**
 * Too much information to generalize, so Stream-Stream joins are represented by a specific node.
 */
public class StreamStreamJoinNode<K, V1, V2, VR> extends BaseJoinProcessorNode<K, V1, V2, VR> {
    private final ProcessorParameters<K, V1, ?, ?> thisWindowedStreamProcessorParameters;
    private final ProcessorParameters<K, V2, ?, ?> otherWindowedStreamProcessorParameters;
    private final StoreFactory thisWindowStoreBuilder;
    private final StoreFactory otherWindowStoreBuilder;
    private final Optional<StoreFactory> outerJoinWindowStoreBuilder;
    private final Joined<K, V1, V2> joined;
    private final boolean enableSpuriousResultFix;
    private final ProcessorParameters<K, V1, ?, ?> selfJoinProcessorParameters;
    private boolean isSelfJoin;

    private StreamStreamJoinNode(final String nodeName,
                                 final ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends VR> valueJoiner,
                                 final ProcessorParameters<K, V1, ?, ?> joinThisProcessorParameters,
                                 final ProcessorParameters<K, V2, ?, ?> joinOtherProcessParameters,
                                 final ProcessorParameters<K, VR, ?, ?> joinMergeProcessorParameters,
                                 final ProcessorParameters<K, V1, ?, ?> thisWindowedStreamProcessorParameters,
                                 final ProcessorParameters<K, V2, ?, ?> otherWindowedStreamProcessorParameters,
                                 final StoreFactory thisStoreFactory,
                                 final StoreFactory otherStoreFactory,
                                 final Optional<StoreFactory> outerJoinStoreFactory,
                                 final Joined<K, V1, V2> joined,
                                 final boolean enableSpuriousResultFix,
                                 final ProcessorParameters<K, V1, ?, ?> selfJoinProcessorParameters) {

        super(nodeName,
              valueJoiner,
              joinThisProcessorParameters,
              joinOtherProcessParameters,
              joinMergeProcessorParameters,
              null,
              null);

        this.thisWindowStoreBuilder = thisStoreFactory;
        this.otherWindowStoreBuilder = otherStoreFactory;
        this.joined = joined;
        this.thisWindowedStreamProcessorParameters = thisWindowedStreamProcessorParameters;
        this.otherWindowedStreamProcessorParameters =  otherWindowedStreamProcessorParameters;
        this.outerJoinWindowStoreBuilder = outerJoinStoreFactory;
        this.enableSpuriousResultFix = enableSpuriousResultFix;
        this.selfJoinProcessorParameters = selfJoinProcessorParameters;
    }


    @Override
    public String toString() {
        return "StreamStreamJoinNode{" +
               "thisWindowedStreamProcessorParameters=" + thisWindowedStreamProcessorParameters +
               ", otherWindowedStreamProcessorParameters=" + otherWindowedStreamProcessorParameters +
               ", thisWindowStoreBuilder=" + thisWindowStoreBuilder +
               ", otherWindowStoreBuilder=" + otherWindowStoreBuilder +
               ", outerJoinWindowStoreBuilder=" + outerJoinWindowStoreBuilder +
               ", joined=" + joined +
               "} " + super.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {

        final String thisProcessorName = thisProcessorParameters().processorName();
        final String otherProcessorName = otherProcessorParameters().processorName();
        final String thisWindowedStreamProcessorName = thisWindowedStreamProcessorParameters.processorName();
        final String otherWindowedStreamProcessorName = otherWindowedStreamProcessorParameters.processorName();

        if (isSelfJoin) {
            topologyBuilder.addProcessor(selfJoinProcessorParameters.processorName(), selfJoinProcessorParameters.processorSupplier(), thisWindowedStreamProcessorName);
            topologyBuilder.addStateStore(thisWindowStoreBuilder, thisWindowedStreamProcessorName, selfJoinProcessorParameters.processorName());
        } else {
            topologyBuilder.addProcessor(thisProcessorName, thisProcessorParameters().processorSupplier(), thisWindowedStreamProcessorName);
            topologyBuilder.addProcessor(otherProcessorName, otherProcessorParameters().processorSupplier(), otherWindowedStreamProcessorName);
            topologyBuilder.addProcessor(mergeProcessorParameters().processorName(), mergeProcessorParameters().processorSupplier(), thisProcessorName, otherProcessorName);
            topologyBuilder.addStateStore(thisWindowStoreBuilder, thisWindowedStreamProcessorName, otherProcessorName);
            topologyBuilder.addStateStore(otherWindowStoreBuilder, otherWindowedStreamProcessorName, thisProcessorName);

            if (enableSpuriousResultFix) {
                outerJoinWindowStoreBuilder.ifPresent(builder -> topologyBuilder.addStateStore(builder, thisProcessorName, otherProcessorName));
            }
        }
    }

    public void setSelfJoin() {
        this.isSelfJoin = true;
    }

    public boolean getSelfJoin() {
        return isSelfJoin;
    }

    public ProcessorParameters<K, V1, ?, ?> thisWindowedStreamProcessorParameters() {
        return thisWindowedStreamProcessorParameters;
    }

    public ProcessorParameters<K, V2, ?, ?> otherWindowedStreamProcessorParameters() {
        return otherWindowedStreamProcessorParameters;
    }

    public static <K, V1, V2, VR> StreamStreamJoinNodeBuilder<K, V1, V2, VR> streamStreamJoinNodeBuilder() {
        return new StreamStreamJoinNodeBuilder<>();
    }

    public static final class StreamStreamJoinNodeBuilder<K, V1, V2, VR> {

        private String nodeName;
        private ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends VR> valueJoiner;
        private ProcessorParameters<K, V1, ?, ?> joinThisProcessorParameters;
        private ProcessorParameters<K, V2, ?, ?> joinOtherProcessorParameters;
        private ProcessorParameters<K, VR, ?, ?> joinMergeProcessorParameters;
        private ProcessorParameters<K, V1, ?, ?> thisWindowedStreamProcessorParameters;
        private ProcessorParameters<K, V2, ?, ?> otherWindowedStreamProcessorParameters;
        private StoreFactory thisStoreFactory;
        private StoreFactory otherStoreFactory;
        private Optional<StoreFactory> outerJoinStoreFactory;
        private Joined<K, V1, V2> joined;
        private boolean enableSpuriousResultFix = false;
        private ProcessorParameters<K, V1, ?, ?> selfJoinProcessorParameters;

        private StreamStreamJoinNodeBuilder() {
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withValueJoiner(final ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends VR> valueJoiner) {
            this.valueJoiner = valueJoiner;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinThisProcessorParameters(final ProcessorParameters<K, V1, ?, ?> joinThisProcessorParameters) {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinOtherProcessorParameters(final ProcessorParameters<K, V2, ?, ?> joinOtherProcessParameters) {
            this.joinOtherProcessorParameters = joinOtherProcessParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinMergeProcessorParameters(final ProcessorParameters<K, VR, ?, ?> joinMergeProcessorParameters) {
            this.joinMergeProcessorParameters = joinMergeProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withThisWindowedStreamProcessorParameters(final ProcessorParameters<K, V1, ?, ?> thisWindowedStreamProcessorParameters) {
            this.thisWindowedStreamProcessorParameters = thisWindowedStreamProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withOtherWindowedStreamProcessorParameters(
            final ProcessorParameters<K, V2, ?, ?> otherWindowedStreamProcessorParameters) {
            this.otherWindowedStreamProcessorParameters = otherWindowedStreamProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withThisWindowStoreBuilder(final StoreFactory thisStoreFactory) {
            this.thisStoreFactory = thisStoreFactory;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withOtherWindowStoreBuilder(final StoreFactory otherStoreFactory) {
            this.otherStoreFactory = otherStoreFactory;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withOuterJoinWindowStoreBuilder(final Optional<StoreFactory> outerJoinWindowStoreBuilder) {
            this.outerJoinStoreFactory = outerJoinWindowStoreBuilder;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoined(final Joined<K, V1, V2> joined) {
            this.joined = joined;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withSpuriousResultFixEnabled() {
            this.enableSpuriousResultFix = true;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withSelfJoinProcessorParameters(
            final ProcessorParameters<K, V1, ?, ?> selfJoinProcessorParameters) {
            this.selfJoinProcessorParameters = selfJoinProcessorParameters;
            return this;
        }

        public StreamStreamJoinNode<K, V1, V2, VR> build() {

            return new StreamStreamJoinNode<>(nodeName,
                                              valueJoiner,
                                              joinThisProcessorParameters,
                                              joinOtherProcessorParameters,
                                              joinMergeProcessorParameters,
                                              thisWindowedStreamProcessorParameters,
                                              otherWindowedStreamProcessorParameters,
                                              thisStoreFactory,
                                              otherStoreFactory,
                                              outerJoinStoreFactory,
                                              joined,
                                              enableSpuriousResultFix,
                                              selfJoinProcessorParameters);


        }
    }
}
