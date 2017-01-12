/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.util.List;

public class GlobalProcessorContextImpl extends AbstractProcessorContext {


    public GlobalProcessorContextImpl(final StreamsConfig config,
                                      final StateManager stateMgr,
                                      final StreamsMetrics metrics,
                                      final ThreadCache cache) {
        super(new TaskId(-1, -1), config.getString(StreamsConfig.APPLICATION_ID_CONFIG), config, metrics, stateMgr, cache);
    }

    /**
     * @throws TopologyBuilderException if an attempt is made to access this state store from an unknown node
     */
    @Override
    public StateStore getStateStore(final String name) {
        return stateManager.getGlobalStore(name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(K key, V value) {
        final ProcessorNode previousNode = currentNode();
        try {
            for (ProcessorNode child : (List<ProcessorNode>) currentNode().children()) {
                setCurrentNode(child);
                child.process(key, value);
            }
        } finally {
            setCurrentNode(previousNode);
        }
    }

    @Override
    public <K, V> void forward(K key, V value, int childIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> void forward(K key, V value, String childName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit() {
        //no-op
    }

    @Override
    public void schedule(long interval) {
        throw new UnsupportedOperationException();
    }

}
