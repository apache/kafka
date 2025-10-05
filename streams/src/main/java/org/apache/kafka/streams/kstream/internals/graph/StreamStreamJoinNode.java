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

import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;


/**
 * Too much information to generalize, so Stream-Stream joins are represented by a specific node.
 */
public class StreamStreamJoinNode<K, V1, V2, VR> extends BaseJoinProcessorNode<K, V1, V2, VR> {
    private final String thisWindowedStreamProcessorName;
    private final String otherWindowedStreamProcessorName;
    private final ProcessorParameters<K, V1, ?, ?> selfJoinProcessorParameters;
    private boolean isSelfJoin;

    private StreamStreamJoinNode(final String nodeName,
                                 final ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends VR> valueJoiner,
                                 final ProcessorParameters<K, V1, ?, ?> joinThisProcessorParameters,
                                 final ProcessorParameters<K, V2, ?, ?> joinOtherProcessParameters,
                                 final ProcessorParameters<K, VR, ?, ?> joinMergeProcessorParameters,
                                 final ProcessorParameters<K, V1, ?, ?> selfJoinProcessorParameters,
                                 final String thisWindowedStreamProcessorName,
                                 final String otherWindowedStreamProcessorName) {

        super(nodeName,
              valueJoiner,
              joinThisProcessorParameters,
              joinOtherProcessParameters,
              joinMergeProcessorParameters,
              null,
              null);

        this.thisWindowedStreamProcessorName = thisWindowedStreamProcessorName;
        this.otherWindowedStreamProcessorName =  otherWindowedStreamProcessorName;
        this.selfJoinProcessorParameters = selfJoinProcessorParameters;
    }

    @Override
    public String toString() {
        return "StreamStreamJoinNode{" +
            "thisWindowedStreamProcessorName=" + thisWindowedStreamProcessorName +
            ", otherWindowedStreamProcessorName=" + otherWindowedStreamProcessorName +
               "} " + super.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {

        final String thisProcessorName = thisProcessorParameters().processorName();
        final String otherProcessorName = otherProcessorParameters().processorName();

        if (isSelfJoin) {
            selfJoinProcessorParameters.addProcessorTo(topologyBuilder, new String[]{thisWindowedStreamProcessorName});
        } else {
            thisProcessorParameters().addProcessorTo(topologyBuilder, new String[]{thisWindowedStreamProcessorName});
            otherProcessorParameters().addProcessorTo(topologyBuilder, new String[]{otherWindowedStreamProcessorName});

            mergeProcessorParameters().addProcessorTo(topologyBuilder, new String[]{thisProcessorName, otherProcessorName});
        }
    }

    public void setSelfJoin() {
        this.isSelfJoin = true;
    }

    public boolean getSelfJoin() {
        return isSelfJoin;
    }

    public String thisWindowedStreamProcessorName() {
        return thisWindowedStreamProcessorName;
    }

    public String otherWindowedStreamProcessorName() {
        return otherWindowedStreamProcessorName;
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
        private ProcessorParameters<K, V1, ?, ?> selfJoinProcessorParameters;
        private String thisWindowedStreamProcessorName;
        private String otherWindowedStreamProcessorName;

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

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withSelfJoinProcessorParameters(
            final ProcessorParameters<K, V1, ?, ?> selfJoinProcessorParameters) {
            this.selfJoinProcessorParameters = selfJoinProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinMergeProcessorParameters(final ProcessorParameters<K, VR, ?, ?> joinMergeProcessorParameters) {
            this.joinMergeProcessorParameters = joinMergeProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withThisWindowedStreamProcessorName(final String thisWindowedStreamProcessorName) {
            this.thisWindowedStreamProcessorName = thisWindowedStreamProcessorName;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withOtherWindowedStreamProcessorName(final String otherWindowedStreamProcessorName) {
            this.otherWindowedStreamProcessorName = otherWindowedStreamProcessorName;
            return this;
        }

        public StreamStreamJoinNode<K, V1, V2, VR> build() {

            return new StreamStreamJoinNode<>(nodeName,
                                              valueJoiner,
                                              joinThisProcessorParameters,
                                              joinOtherProcessorParameters,
                                              joinMergeProcessorParameters,
                                              selfJoinProcessorParameters,
                                              thisWindowedStreamProcessorName,
                                              otherWindowedStreamProcessorName);


        }
    }
}
