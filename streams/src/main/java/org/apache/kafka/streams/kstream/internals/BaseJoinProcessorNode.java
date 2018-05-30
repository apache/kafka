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
import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * Utility base class containing the common fields between
 * a Stream-Stream join and a Table-Table join
 */
abstract class BaseJoinProcessorNode<K, V1, V2, VR> extends StreamsGraphNode {

    private final ProcessorSupplier<K, V1> joinThisProcessSupplier;
    private final ProcessorSupplier<K, V2> joinOtherProcessSupplier;
    private final ProcessorSupplier<K, VR> joinMergeProcessor;
    private final ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner;
    private final String joinThisProcessorName;
    private final String joinOtherProcessorName;
    private final String joinMergeProcessorName;
    private final String thisJoinSide;
    private final String otherJoinSide;


    BaseJoinProcessorNode(final String parentProcessorNodeName,
                          final String processorNodeName,
                          final ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner,
                          final ProcessorParameters<K, V1> joinThisProcessorDetails,
                          final ProcessorParameters<K, V2> joinOtherProcessDetails,
                          final ProcessorParameters<K, VR> joinMergeProcessorDetails,
                          final String thisJoinSide,
                          final String otherJoinSide) {

        super(parentProcessorNodeName,
              processorNodeName,
              false);

        this.valueJoiner = valueJoiner;
        this.joinThisProcessSupplier = joinThisProcessorDetails.processorSupplier();
        this.joinOtherProcessSupplier = joinOtherProcessDetails.processorSupplier();
        this.joinMergeProcessor = joinMergeProcessorDetails.processorSupplier();
        this.joinThisProcessorName = joinThisProcessorDetails.processorName();
        this.joinOtherProcessorName = joinOtherProcessDetails.processorName();
        this.joinMergeProcessorName = joinMergeProcessorDetails.processorName();
        this.thisJoinSide = thisJoinSide;
        this.otherJoinSide = otherJoinSide;
    }

    ProcessorSupplier<K, V1> joinThisProcessorSupplier() {
        return joinThisProcessSupplier;
    }

    ProcessorSupplier<K, V2> joinOtherProcessorSupplier() {
        return joinOtherProcessSupplier;
    }

    ProcessorSupplier<K, VR> joinMergeProcessor() {
        return joinMergeProcessor;
    }

    ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner() {
        return valueJoiner;
    }

    String joinThisProcessorName() {
        return joinThisProcessorName;
    }

    String joinOtherProcessorName() {
        return joinOtherProcessorName;
    }

    String joinMergeProcessorName() {
        return joinMergeProcessorName;
    }

    String thisJoinSide() {
        return thisJoinSide;
    }

    String otherJoinSide() {
        return otherJoinSide;
    }
}
