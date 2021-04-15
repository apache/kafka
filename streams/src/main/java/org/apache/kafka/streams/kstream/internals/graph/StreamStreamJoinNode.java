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
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Too much information to generalize, so Stream-Stream joins are represented by a specific node.
 */
public class StreamStreamJoinNode<K, V, V1, VOut> extends BaseJoinProcessorNode<K, V, V1, VOut> {

    private final ProcessorParameters<K, V, ?, ?> thisWindowedStreamProcessorParameters;
    private final ProcessorParameters<K, V1, ?, ?> otherWindowedStreamProcessorParameters;
    private final StoreBuilder<WindowStore<K, V>> thisWindowStoreBuilder;
    private final StoreBuilder<WindowStore<K, V1>> otherWindowStoreBuilder;
    private final Joined<K, V, V1> joined;


    private StreamStreamJoinNode(final String nodeName,
                                 final ValueJoinerWithKey<? super K, ? super V, ? super V1, ? extends VOut> valueJoiner,
                                 final ProcessorParameters<K, V, ?, ?> joinThisProcessorParameters,
                                 final ProcessorParameters<K, V1, ?, ?> joinOtherProcessParameters,
                                 final ProcessorParameters<K, VOut, ?, ?> joinMergeProcessorParameters,
                                 final ProcessorParameters<K, V, ?, ?> thisWindowedStreamProcessorParameters,
                                 final ProcessorParameters<K, V1, ?, ?> otherWindowedStreamProcessorParameters,
                                 final StoreBuilder<WindowStore<K, V>> thisWindowStoreBuilder,
                                 final StoreBuilder<WindowStore<K, V1>> otherWindowStoreBuilder,
                                 final Joined<K, V, V1> joined) {

        super(nodeName,
              valueJoiner,
              joinThisProcessorParameters,
              joinOtherProcessParameters,
              joinMergeProcessorParameters,
              null,
              null);

        this.thisWindowStoreBuilder = thisWindowStoreBuilder;
        this.otherWindowStoreBuilder = otherWindowStoreBuilder;
        this.joined = joined;
        this.thisWindowedStreamProcessorParameters = thisWindowedStreamProcessorParameters;
        this.otherWindowedStreamProcessorParameters =  otherWindowedStreamProcessorParameters;

    }


    @Override
    public String toString() {
        return "StreamStreamJoinNode{" +
               "thisWindowedStreamProcessorParameters=" + thisWindowedStreamProcessorParameters +
               ", otherWindowedStreamProcessorParameters=" + otherWindowedStreamProcessorParameters +
               ", thisWindowStoreBuilder=" + thisWindowStoreBuilder +
               ", otherWindowStoreBuilder=" + otherWindowStoreBuilder +
               ", joined=" + joined +
               "} " + super.toString();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {

        final String thisProcessorName = thisProcessorParameters().processorName();
        final String otherProcessorName = otherProcessorParameters().processorName();
        final String thisWindowedStreamProcessorName = thisWindowedStreamProcessorParameters.processorName();
        final String otherWindowedStreamProcessorName = otherWindowedStreamProcessorParameters.processorName();

        topologyBuilder.addProcessor(thisProcessorName, thisProcessorParameters().processorSupplier(), thisWindowedStreamProcessorName);
        topologyBuilder.addProcessor(otherProcessorName, otherProcessorParameters().processorSupplier(), otherWindowedStreamProcessorName);
        topologyBuilder.addProcessor(mergeProcessorParameters().processorName(), mergeProcessorParameters().processorSupplier(), thisProcessorName, otherProcessorName);
        topologyBuilder.addStateStore(thisWindowStoreBuilder, thisWindowedStreamProcessorName, otherProcessorName);
        topologyBuilder.addStateStore(otherWindowStoreBuilder, otherWindowedStreamProcessorName, thisProcessorName);
    }

    public static <K, V, V1, VOut> StreamStreamJoinNodeBuilder<K, V, V1, VOut> streamStreamJoinNodeBuilder() {
        return new StreamStreamJoinNodeBuilder<>();
    }

    public static final class StreamStreamJoinNodeBuilder<K, V, V1, VOut> {

        private String nodeName;
        private ValueJoinerWithKey<? super K, ? super V, ? super V1, ? extends VOut> valueJoiner;
        private ProcessorParameters<K, V, ?, ?> joinThisProcessorParameters;
        private ProcessorParameters<K, V1, ?, ?> joinOtherProcessorParameters;
        private ProcessorParameters<K, VOut, ?, ?> joinMergeProcessorParameters;
        private ProcessorParameters<K, V, ?, ?> thisWindowedStreamProcessorParameters;
        private ProcessorParameters<K, V1, ?, ?> otherWindowedStreamProcessorParameters;
        private StoreBuilder<WindowStore<K, V>> thisWindowStoreBuilder;
        private StoreBuilder<WindowStore<K, V1>> otherWindowStoreBuilder;
        private Joined<K, V, V1> joined;


        private StreamStreamJoinNodeBuilder() {
        }


        public StreamStreamJoinNodeBuilder<K, V, V1, VOut> withValueJoiner(final ValueJoinerWithKey<? super K, ? super V, ? super V1, ? extends VOut> valueJoiner) {
            this.valueJoiner = valueJoiner;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V, V1, VOut> withJoinThisProcessorParameters(final ProcessorParameters<K, V, ?, ?> joinThisProcessorParameters) {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V, V1, VOut> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V, V1, VOut> withJoinOtherProcessorParameters(final ProcessorParameters<K, V1, ?, ?> joinOtherProcessParameters) {
            this.joinOtherProcessorParameters = joinOtherProcessParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V, V1, VOut> withJoinMergeProcessorParameters(final ProcessorParameters<K, VOut, ?, ?> joinMergeProcessorParameters) {
            this.joinMergeProcessorParameters = joinMergeProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V, V1, VOut> withThisWindowedStreamProcessorParameters(final ProcessorParameters<K, V, ?, ?> thisWindowedStreamProcessorParameters) {
            this.thisWindowedStreamProcessorParameters = thisWindowedStreamProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V, V1, VOut> withOtherWindowedStreamProcessorParameters(
            final ProcessorParameters<K, V1, ?, ?> otherWindowedStreamProcessorParameters) {
            this.otherWindowedStreamProcessorParameters = otherWindowedStreamProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V, V1, VOut> withThisWindowStoreBuilder(final StoreBuilder<WindowStore<K, V>> thisWindowStoreBuilder) {
            this.thisWindowStoreBuilder = thisWindowStoreBuilder;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V, V1, VOut> withOtherWindowStoreBuilder(final StoreBuilder<WindowStore<K, V1>> otherWindowStoreBuilder) {
            this.otherWindowStoreBuilder = otherWindowStoreBuilder;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V, V1, VOut> withJoined(final Joined<K, V, V1> joined) {
            this.joined = joined;
            return this;
        }

        public StreamStreamJoinNode<K, V, V1, VOut> build() {

            return new StreamStreamJoinNode<>(nodeName,
                                              valueJoiner,
                                              joinThisProcessorParameters,
                                              joinOtherProcessorParameters,
                                              joinMergeProcessorParameters,
                                              thisWindowedStreamProcessorParameters,
                                              otherWindowedStreamProcessorParameters,
                                              thisWindowStoreBuilder,
                                              otherWindowStoreBuilder,
                                              joined);


        }
    }
}
