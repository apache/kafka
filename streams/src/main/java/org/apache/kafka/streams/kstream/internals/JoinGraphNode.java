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

import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Objects;

public class JoinGraphNode extends StreamsGraphNode {

    final String thisWindowStreamName;
    final String otherWindowStreamName;
    final String leftHandSideCallingStream;
    final String otherStreamName;
    final String joinThisName;
    final String joinOtherName;
    final String joinMergeName;
    final ProcessorSupplier joinThisProcessor;
    final ProcessorSupplier joinOtherProcessor;
    final ProcessorSupplier thisWindowedStreamProcessor;
    final ProcessorSupplier otherWindowedStreamProcessor;
    final ProcessorSupplier joinMergeProcessor;
    final StoreBuilder thisWindowBuilder;
    final StoreBuilder otherWindowBuilder;


    public JoinGraphNode(final TopologyNodeType nodeType,
                         final String thisWindowStreamName,
                         final String otherWindowStreamName,
                         final String leftHandSideCallingStream,
                         final String otherStreamName,
                         final String joinThisName,
                         final String joinOtherName,
                         final String joinMergeName,
                         final ProcessorSupplier joinThisProcessor,
                         final ProcessorSupplier joinOtherProcessor,
                         final ProcessorSupplier thisWindowedStreamProcessor,
                         final ProcessorSupplier otherWindowedStreamProcessor,
                         final ProcessorSupplier joinMergeProcessor,
                         final StoreBuilder thisWindowBuilder,
                         final StoreBuilder otherWindowBuilder) {
        super(nodeType);
        this.thisWindowStreamName = thisWindowStreamName;
        this.otherWindowStreamName = otherWindowStreamName;
        this.leftHandSideCallingStream = leftHandSideCallingStream;
        this.otherStreamName = otherStreamName;
        this.joinThisName = joinThisName;
        this.joinOtherName = joinOtherName;
        this.joinMergeName = joinMergeName;
        this.joinThisProcessor = joinThisProcessor;
        this.joinOtherProcessor = joinOtherProcessor;
        this.thisWindowedStreamProcessor = thisWindowedStreamProcessor;
        this.otherWindowedStreamProcessor = otherWindowedStreamProcessor;
        this.joinMergeProcessor = joinMergeProcessor;
        this.thisWindowBuilder = thisWindowBuilder;
        this.otherWindowBuilder = otherWindowBuilder;
    }


    public static final class Builder {

        private TopologyNodeType topologyNodeType;
        private String thisWindowStreamName;
        private String otherWindowStreamName;
        private String leftHandSideCallingStream;
        private String otherStreamName;
        private String joinThisName;
        private String joinOtherName;
        private String joinMergeName;
        private ProcessorSupplier joinThisProcessor;
        private ProcessorSupplier joinOtherProcessor;
        private ProcessorSupplier thisWindowedStream;
        private ProcessorSupplier otherWindowedStream;
        private ProcessorSupplier joinMergeProcessor;
        private StoreBuilder thisWindowBuilder;
        private StoreBuilder otherWindowBuilder;
        private String name;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder withThisWindowStreamName(String thisWindowStreamName) {
            this.thisWindowStreamName = thisWindowStreamName;
            return this;
        }

        public Builder withOtherWindowStreamName(String otherWindowStreamName) {
            this.otherWindowStreamName = otherWindowStreamName;
            return this;
        }

        public Builder withLeftHandSideCallingStream(String leftHandSideCallingStream) {
            this.leftHandSideCallingStream = leftHandSideCallingStream;
            return this;
        }

        public Builder withOtherStreamName(String otherStreamName) {
            this.otherStreamName = otherStreamName;
            return this;
        }

        public Builder withJoinThisName(String joinThisName) {
            this.joinThisName = joinThisName;
            return this;
        }

        public Builder withJoinOtherName(String joinOtherName) {
            this.joinOtherName = joinOtherName;
            return this;
        }

        public Builder withJoinMergeName(String joinMergeName) {
            this.joinMergeName = joinMergeName;
            return this;
        }

        public Builder withJoinThisProcessor(ProcessorSupplier joinThisProcessor) {
            this.joinThisProcessor = joinThisProcessor;
            return this;
        }

        public Builder withJoinOtherProcessor(ProcessorSupplier joinOtherProcessor) {
            this.joinOtherProcessor = joinOtherProcessor;
            return this;
        }

        public Builder withThisWindowedStreamProcessor(ProcessorSupplier thisWindowedStream) {
            this.thisWindowedStream = thisWindowedStream;
            return this;
        }

        public Builder withOtherWindowedStreamProcessor(ProcessorSupplier otherWindowedStream) {
            this.otherWindowedStream = otherWindowedStream;
            return this;
        }

        public Builder withJoinMergeProcessor(ProcessorSupplier joinMergeProcessor) {
            this.joinMergeProcessor = joinMergeProcessor;
            return this;
        }

        public Builder withThisWindowBuilder(StoreBuilder thisWindowBuilder) {
            this.thisWindowBuilder = thisWindowBuilder;
            return this;
        }

        public Builder withOtherWindowBuilder(StoreBuilder otherWindowBuilder) {
            this.otherWindowBuilder = otherWindowBuilder;
            return this;
        }

        public Builder withTopologyNodeType(TopologyNodeType topologyNodeType) {
            this.topologyNodeType = topologyNodeType;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public JoinGraphNode build() {
            JoinGraphNode joinGraphNode = new  JoinGraphNode(topologyNodeType, thisWindowStreamName, otherWindowStreamName, leftHandSideCallingStream, otherStreamName, joinThisName,
                                     joinOtherName,
                                     joinMergeName, joinThisProcessor, joinOtherProcessor, thisWindowedStream, otherWindowedStream, joinMergeProcessor,
                                     thisWindowBuilder, otherWindowBuilder);

            joinGraphNode.setName(name);
            return joinGraphNode;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        JoinGraphNode that = (JoinGraphNode) o;
        return Objects.equals(thisWindowStreamName, that.thisWindowStreamName) &&
               Objects.equals(otherWindowStreamName, that.otherWindowStreamName) &&
               Objects.equals(leftHandSideCallingStream, that.leftHandSideCallingStream) &&
               Objects.equals(otherStreamName, that.otherStreamName) &&
               Objects.equals(joinThisName, that.joinThisName) &&
               Objects.equals(joinOtherName, that.joinOtherName) &&
               Objects.equals(joinMergeName, that.joinMergeName) &&
               Objects.equals(joinThisProcessor, that.joinThisProcessor) &&
               Objects.equals(joinOtherProcessor, that.joinOtherProcessor) &&
               Objects.equals(thisWindowedStreamProcessor, that.thisWindowedStreamProcessor) &&
               Objects.equals(otherWindowedStreamProcessor, that.otherWindowedStreamProcessor) &&
               Objects.equals(joinMergeProcessor, that.joinMergeProcessor) &&
               Objects.equals(thisWindowBuilder, that.thisWindowBuilder) &&
               Objects.equals(otherWindowBuilder, that.otherWindowBuilder);
    }

    @Override
    public int hashCode() {

        return Objects
            .hash(super.hashCode(), thisWindowStreamName, otherWindowStreamName, leftHandSideCallingStream, otherStreamName, joinThisName, joinOtherName,
                  joinMergeName, joinThisProcessor, joinOtherProcessor, thisWindowedStreamProcessor, otherWindowedStreamProcessor, joinMergeProcessor,
                  thisWindowBuilder, otherWindowBuilder);
    }
}
