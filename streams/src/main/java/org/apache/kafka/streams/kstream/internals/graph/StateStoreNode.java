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

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.StoreFactory;

public class StateStoreNode<S extends StateStore> extends GraphNode {

    protected final StoreFactory storeBuilder;

    public StateStoreNode(final StoreFactory storeBuilder) {
        super(storeBuilder.name());

        this.storeBuilder = storeBuilder;
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        topologyBuilder.addStateStore(storeBuilder);
    }

    @Override
    public String toString() {
        return "StateStoreNode{" +
               " name='" + storeBuilder.name() +  '\'' +
               ", logConfig=" + storeBuilder.logConfig() +
               ", loggingEnabled='" + storeBuilder.loggingEnabled() + '\'' +
               "} ";
    }
}
