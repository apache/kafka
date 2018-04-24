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

import java.util.Arrays;

/**
 * Represents a join between a KStream and a KTable or GlobalKTable
 *
 * @param <K> key type
 * @param <V> value type of stream
 * @param <V1> value type of table
 * @param <V2> value type resulting from join
 */

class StreamTableJoinNode<K, V, V1, V2> extends StreamGraphNode<K, V> {

    private final String[] storeNames;
    private final ValueJoiner<? super V, ? super V1, ? extends V2> valueJoiner;
    private final ProcessorSupplier<K, V> processorSupplier;

    StreamTableJoinNode(final String predecessorNodeName,
                        final String name,
                        final ValueJoiner<? super V, ? super V1, ? extends V2> valueJoiner,
                        final ProcessorSupplier<K, V> processorSupplier,
                        final String[] storeNames) {
        super(predecessorNodeName,
              name,
              false);

        this.storeNames = storeNames;
        this.valueJoiner = valueJoiner;
        this.processorSupplier = processorSupplier;
    }

    String[] storeNames() {
        return Arrays.copyOf(storeNames, storeNames.length);
    }

    ValueJoiner<? super V, ? super V1, ? extends V2> valueJoiner() {
        return valueJoiner;
    }

    ProcessorSupplier<K, V> processorSupplier() {
        return processorSupplier;
    }

}
