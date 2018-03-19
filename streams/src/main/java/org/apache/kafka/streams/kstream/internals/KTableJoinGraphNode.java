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

import java.util.Arrays;
import java.util.Objects;

public class KTableJoinGraphNode extends StreamsGraphNode {

    final ProcessorSupplier joinThisProcessor;
    final ProcessorSupplier joinOtherProcessor;
    final ProcessorSupplier joinMergeProcessor;
    final String joinThisName;
    final String joinOtherName;
    final String joinMerggeName;
    final String[] joinThisStoreNames;
    final String[] joinOtherStoreNames;
    final String otherKTableName;


    public KTableJoinGraphNode(TopologyNodeType topologyNodeType, ProcessorSupplier joinThisProcessor, ProcessorSupplier joinOtherProcessor,
                               ProcessorSupplier joinMergeProcessor, String joinThisName, String joinOtherName, String joinMerggeName,
                               String[] joinThisStoreNames, String[] joinOtherStoreNames, String otherKTableName) {
        super(topologyNodeType);
        this.joinThisProcessor = joinThisProcessor;
        this.joinOtherProcessor = joinOtherProcessor;
        this.joinMergeProcessor = joinMergeProcessor;
        this.joinThisName = joinThisName;
        this.joinOtherName = joinOtherName;
        this.joinMerggeName = joinMerggeName;
        this.joinThisStoreNames = joinThisStoreNames;
        this.joinOtherStoreNames = joinOtherStoreNames;
        this.otherKTableName = otherKTableName;

    }

    public static Builder builder() {
        return  new Builder();
    }

    public static final class Builder {

        protected TopologyNodeType topologyTopologyNodeType;
        protected String name;
        protected ProcessDetails processDetails;
        ProcessorSupplier joinThisProcessor;
        ProcessorSupplier joinOtherProcessor;
        ProcessorSupplier joinMergeProcessor;
        String joinThisName;
        String joinOtherName;
        String joinMerggeName;
        String[] joinThisStoreNames;
        String[] joinOtherStoreNames;
        String otherKTableName;

        private Builder() {
        }

        public static Builder aKTableJoinGraphNode() {
            return new Builder();
        }

        public Builder withJoinThisProcessor(ProcessorSupplier joinThisProcessor) {
            this.joinThisProcessor = joinThisProcessor;
            return this;
        }

        public Builder withJoinOtherProcessor(ProcessorSupplier joinOtherProcessor) {
            this.joinOtherProcessor = joinOtherProcessor;
            return this;
        }

        public Builder withJoinMergeProcessor(ProcessorSupplier joinMergeProcessor) {
            this.joinMergeProcessor = joinMergeProcessor;
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

        public Builder withJoinMerggeName(String joinMerggeName) {
            this.joinMerggeName = joinMerggeName;
            return this;
        }

        public Builder withJoinThisStoreNames(String[] joinThisStoreNames) {
            this.joinThisStoreNames = joinThisStoreNames;
            return this;
        }

        public Builder withJoinOtherStoreNames(String[] joinOtherStoreNames) {
            this.joinOtherStoreNames = joinOtherStoreNames;
            return this;
        }

        public Builder withTopologyNodeType(TopologyNodeType topologyTopologyNodeType) {
            this.topologyTopologyNodeType = topologyTopologyNodeType;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withProcessDetails(ProcessDetails processDetails) {
            this.processDetails = processDetails;
            return this;
        }

        public Builder withOtherKtableName(String otherKTableName) {
            this.otherKTableName = otherKTableName;
            return this;
        }



        public KTableJoinGraphNode build() {
            KTableJoinGraphNode
                kTableJoinGraphNode =
                new KTableJoinGraphNode(topologyTopologyNodeType, joinThisProcessor, joinOtherProcessor, joinMergeProcessor, joinThisName, joinOtherName, joinMerggeName,
                                        joinThisStoreNames, joinOtherStoreNames, otherKTableName);
            kTableJoinGraphNode.setName(name);
            kTableJoinGraphNode.setProcessed(processDetails);
            return kTableJoinGraphNode;
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
        KTableJoinGraphNode that = (KTableJoinGraphNode) o;
        return Objects.equals(joinThisProcessor, that.joinThisProcessor) &&
               Objects.equals(joinOtherProcessor, that.joinOtherProcessor) &&
               Objects.equals(joinMergeProcessor, that.joinMergeProcessor) &&
               Objects.equals(joinThisName, that.joinThisName) &&
               Objects.equals(joinOtherName, that.joinOtherName) &&
               Objects.equals(joinMerggeName, that.joinMerggeName) &&
               Arrays.equals(joinThisStoreNames, that.joinThisStoreNames) &&
               Arrays.equals(joinOtherStoreNames, that.joinOtherStoreNames) &&
               Objects.equals(otherKTableName, that.otherKTableName);
    }

    @Override
    public int hashCode() {

        int
            result =
            Objects.hash(super.hashCode(), joinThisProcessor, joinOtherProcessor, joinMergeProcessor, joinThisName, joinOtherName, joinMerggeName,
                         otherKTableName);
        result = 31 * result + Arrays.hashCode(joinThisStoreNames);
        result = 31 * result + Arrays.hashCode(joinOtherStoreNames);
        return result;
    }
}
