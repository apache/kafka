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
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.Arrays;

/**
 * Represents a join between a KStream and a KTable or GlobalKTable
 *
 */

class StreamTableJoinNode<K1, K2,  V1, V2, VR> extends StreamsGraphNode {

    private final String[] storeNames;
    private final ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner;
    private final Joined<K1, V1, V2> joined;
    private final KeyValueMapper<? super K1, ? super V1, ? extends K2> keyValueMapper;
    private final ProcessorSupplier<K1, V1> processorSupplier;

    StreamTableJoinNode(final String parentProcessorNodeName,
                        final String processorNodeName,
                        final ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner,
                        final KeyValueMapper<? super K1, ? super V1, ? extends K2> keyValueMapper,
                        final Joined<K1, V1, V2> joined,
                        final ProcessorSupplier<K1, V1> processorSupplier,
                        final String[] storeNames) {
        super(parentProcessorNodeName,
              processorNodeName,
              false);

        this.storeNames = storeNames;
        this.valueJoiner = valueJoiner;
        this.keyValueMapper = keyValueMapper;
        this.joined = joined;
        this.processorSupplier = processorSupplier;
    }

    String[] storeNames() {
        return Arrays.copyOf(storeNames, storeNames.length);
    }

    ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner() {
        return valueJoiner;
    }

    Joined<K1, V1, V2> joined() {
        return joined;
    }

    ProcessorSupplier<K1, V1> processorSupplier() {
        return processorSupplier;
    }

    KeyValueMapper<? super K1, ? super V1, ? extends K2> keyValueMapper() {
        return keyValueMapper;
    }

    @Override
    void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        //TODO will implement in follow-up pr
    }
}
