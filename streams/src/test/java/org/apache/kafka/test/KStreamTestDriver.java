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

package org.apache.kafka.test;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;

import java.util.List;

public class KStreamTestDriver {

    private final ProcessorTopology topology;
    private final MockProcessorContext context;
    private ProcessorNode currNode;

    public KStreamTestDriver(KStreamBuilder builder) {
        this(builder, null, null);
    }

    public KStreamTestDriver(KStreamBuilder builder, Serializer<?> serializer, Deserializer<?> deserializer) {
        this.topology = builder.build(null);
        this.context = new MockProcessorContext(this, serializer, deserializer);

        for (ProcessorNode node : topology.processors()) {
            currNode = node;
            try {
                node.init(context);
            } finally {
                currNode = null;
            }
        }
    }

    public void process(String topicName, Object key, Object value) {
        currNode = topology.source(topicName);
        try {
            forward(key, value);
        } finally {
            currNode = null;
        }
    }

    public void setTime(long timestamp) {
        context.setTime(timestamp);
    }

    public StateStore getStateStore(String name) {
        return context.getStateStore(name);
    }

    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value) {
        ProcessorNode thisNode = currNode;
        for (ProcessorNode childNode : (List<ProcessorNode<K, V>>) thisNode.children()) {
            currNode = childNode;
            try {
                childNode.process(key, value);
            } finally {
                currNode = thisNode;
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value, int childIndex) {
        ProcessorNode thisNode = currNode;
        ProcessorNode childNode = (ProcessorNode<K, V>) thisNode.children().get(childIndex);
        currNode = childNode;
        try {
            childNode.process(key, value);
        } finally {
            currNode = thisNode;
        }
    }

}
