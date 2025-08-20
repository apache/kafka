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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.DslStoreSuppliers;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.util.Map;

public class StreamJoinedInternal<K, V1, V2> extends StreamJoined<K, V1, V2> {

    // this tracks the original dsl store suppliers that were passed
    // in -- this helps ensure that we can resolve the outer join
    // store in the desired order (see comments in OuterStreamJoinFactory)
    private final DslStoreSuppliers passedInDslStoreSuppliers;

    //Needs to be public for testing
    public StreamJoinedInternal(
        final StreamJoined<K, V1, V2> streamJoined,
        final InternalStreamsBuilder builder
    ) {
        super(streamJoined);
        passedInDslStoreSuppliers = dslStoreSuppliers;
        if (dslStoreSuppliers == null) {
            final TopologyConfig topologyConfig = builder.internalTopologyBuilder().topologyConfigs();
            if (topologyConfig != null) {
                dslStoreSuppliers = topologyConfig.resolveDslStoreSuppliers().orElse(null);
            }
        }
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V1> valueSerde() {
        return valueSerde;
    }

    public Serde<V2> otherValueSerde() {
        return otherValueSerde;
    }

    public String name() {
        return name;
    }

    public String storeName() {
        return storeName;
    }

    public DslStoreSuppliers passedInDslStoreSuppliers() {
        return passedInDslStoreSuppliers;
    }

    public DslStoreSuppliers dslStoreSuppliers() {
        return dslStoreSuppliers;
    }

    public WindowBytesStoreSupplier thisStoreSupplier() {
        return thisStoreSupplier;
    }

    public WindowBytesStoreSupplier otherStoreSupplier() {
        return otherStoreSupplier;
    }

    public boolean loggingEnabled() {
        return loggingEnabled;
    }

    Map<String, String> logConfig() {
        return topicConfig;
    }

}
