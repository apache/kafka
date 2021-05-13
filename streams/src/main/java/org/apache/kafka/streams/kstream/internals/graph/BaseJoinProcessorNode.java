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

/**
 * Utility base class containing the common fields between
 * a Stream-Stream join and a Table-Table join
 */
abstract class BaseJoinProcessorNode<K, V, V1, VOut> extends GraphNode {

    private final ProcessorParameters<K, V, ?, ?> joinThisProcessorParameters;
    private final ProcessorParameters<K, V1, ?, ?> joinOtherProcessorParameters;
    private final ProcessorParameters<K, VOut, ?, ?> joinMergeProcessorParameters;
    private final ValueJoinerWithKey<? super K, ? super V, ? super V1, ? extends VOut> valueJoiner;
    private final String thisJoinSideNodeName;
    private final String otherJoinSideNodeName;


    BaseJoinProcessorNode(final String nodeName,
                          final ValueJoinerWithKey<? super K, ? super V, ? super V1, ? extends VOut> valueJoiner,
                          final ProcessorParameters<K, V, ?, ?> joinThisProcessorParameters,
                          final ProcessorParameters<K, V1, ?, ?> joinOtherProcessorParameters,
                          final ProcessorParameters<K, VOut, ?, ?> joinMergeProcessorParameters,
                          final String thisJoinSideNodeName,
                          final String otherJoinSideNodeName) {

        super(nodeName);

        this.valueJoiner = valueJoiner;
        this.joinThisProcessorParameters = joinThisProcessorParameters;
        this.joinOtherProcessorParameters = joinOtherProcessorParameters;
        this.joinMergeProcessorParameters = joinMergeProcessorParameters;
        this.thisJoinSideNodeName = thisJoinSideNodeName;
        this.otherJoinSideNodeName = otherJoinSideNodeName;
    }

    ProcessorParameters<K, V, ?, ?> thisProcessorParameters() {
        return joinThisProcessorParameters;
    }

    ProcessorParameters<K, V1, ?, ?> otherProcessorParameters() {
        return joinOtherProcessorParameters;
    }

    ProcessorParameters<K, VOut, ?, ?> mergeProcessorParameters() {
        return joinMergeProcessorParameters;
    }

    ValueJoinerWithKey<? super K, ? super V, ? super V1, ? extends VOut> valueJoiner() {
        return valueJoiner;
    }

    String thisJoinSideNodeName() {
        return thisJoinSideNodeName;
    }

    String otherJoinSideNodeName() {
        return otherJoinSideNodeName;
    }

    @Override
    public String toString() {
        return "BaseJoinProcessorNode{" +
               "joinThisProcessorParameters=" + joinThisProcessorParameters +
               ", joinOtherProcessorParameters=" + joinOtherProcessorParameters +
               ", joinMergeProcessorParameters=" + joinMergeProcessorParameters +
               ", valueJoiner=" + valueJoiner +
               ", thisJoinSideNodeName='" + thisJoinSideNodeName + '\'' +
               ", otherJoinSideNodeName='" + otherJoinSideNodeName + '\'' +
               "} " + super.toString();
    }
}
