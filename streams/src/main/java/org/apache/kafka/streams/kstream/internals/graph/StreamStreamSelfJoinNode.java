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
public class StreamStreamSelfJoinNode<K, V1, VR> extends BaseJoinProcessorNode<K, V1, V1, VR> {
    private final ProcessorParameters<K, V1, ?, ?> thisWindowedStreamProcessorParameters;
    private final StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder;
    private final Joined<K, V1, V1> joined;

    private StreamStreamSelfJoinNode(
        final String nodeName,
        final ValueJoinerWithKey<? super K, ? super V1, ? super V1, ? extends VR> valueJoiner,
        final ProcessorParameters<K, V1, ?, ?> joinThisProcessorParameters,
        final ProcessorParameters<K, V1, ?, ?> thisWindowedStreamProcessorParameters,
        final StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder,
        final Joined<K, V1, V1> joined) {

        super(nodeName,
              valueJoiner,
              joinThisProcessorParameters,
              joinThisProcessorParameters,
              null,
              null,
              null);

        this.thisWindowStoreBuilder = thisWindowStoreBuilder;
        this.joined = joined;
        this.thisWindowedStreamProcessorParameters = thisWindowedStreamProcessorParameters;
    }


    @Override
    public String toString() {
        return "StreamStreamJoinNode{" +
               "thisWindowedStreamProcessorParameters=" + thisWindowedStreamProcessorParameters +
               ", thisWindowStoreBuilder=" + thisWindowStoreBuilder +
               ", joined=" + joined +
               "} " + super.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {

        final String thisProcessorName = thisProcessorParameters().processorName();
        final String thisWindowedStreamProcessorName = thisWindowedStreamProcessorParameters.processorName();

        topologyBuilder.addProcessor(
            thisProcessorName,
            thisProcessorParameters().processorSupplier(),
            thisWindowedStreamProcessorName);
        topologyBuilder.addStateStore(
            thisWindowStoreBuilder, thisWindowedStreamProcessorName, thisProcessorName);
    }

    public static <K, V1, V2, VR> StreamStreamSelfJoinNodeBuilder<K, V1, VR> streamStreamJoinNodeBuilder() {
        return new StreamStreamSelfJoinNodeBuilder<>();
    }

    public static final class StreamStreamSelfJoinNodeBuilder<K, V1, VR> {

        private String nodeName;
        private ValueJoinerWithKey<? super K, ? super V1, ? super V1, ? extends VR> valueJoiner;
        private ProcessorParameters<K, V1, ?, ?> joinThisProcessorParameters;
        private ProcessorParameters<K, V1, ?, ?> thisWindowedStreamProcessorParameters;
        private StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder;
        private Joined<K, V1, V1> joined;

        private StreamStreamSelfJoinNodeBuilder() {
        }

        public StreamStreamSelfJoinNodeBuilder<K, V1, VR> withValueJoiner(final ValueJoinerWithKey<? super K, ? super V1, ? super V1, ? extends VR> valueJoiner) {
            this.valueJoiner = valueJoiner;
            return this;
        }

        public StreamStreamSelfJoinNodeBuilder<K, V1, VR> withJoinThisProcessorParameters(final ProcessorParameters<K, V1, ?, ?> joinThisProcessorParameters) {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        public StreamStreamSelfJoinNodeBuilder<K, V1, VR> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }


        public StreamStreamSelfJoinNodeBuilder<K, V1, VR> withThisWindowedStreamProcessorParameters(final ProcessorParameters<K, V1, ?, ?> thisWindowedStreamProcessorParameters) {
            this.thisWindowedStreamProcessorParameters = thisWindowedStreamProcessorParameters;
            return this;
        }

        public StreamStreamSelfJoinNodeBuilder<K, V1, VR> withThisWindowStoreBuilder(final StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder) {
            this.thisWindowStoreBuilder = thisWindowStoreBuilder;
            return this;
        }

        public StreamStreamSelfJoinNodeBuilder<K, V1, VR> withJoined(final Joined<K, V1, V1> joined) {
            this.joined = joined;
            return this;
        }

        public StreamStreamSelfJoinNode<K, V1, VR> build() {

            return new StreamStreamSelfJoinNode<>(nodeName,
                                                  valueJoiner,
                                                  joinThisProcessorParameters,
                                                  thisWindowedStreamProcessorParameters,
                                                  thisWindowStoreBuilder,
                                                  joined);


        }
    }
}
