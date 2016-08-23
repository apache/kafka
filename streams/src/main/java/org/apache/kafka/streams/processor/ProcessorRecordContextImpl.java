/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor;

import org.apache.kafka.streams.processor.internals.ProcessorNode;

import java.util.List;

public class ProcessorRecordContextImpl implements ProcessorRecordContext {

    private final long timestamp;
    private final long offset;
    private final String topic;
    private final int partition;
    private ProcessorNode node;

    public ProcessorRecordContextImpl(final long timestamp,
                                      final long offset,
                                      final int partition,
                                      final String topic,
                                      final ProcessorNode node) {

        this.timestamp = timestamp;
        this.offset = offset;
        this.node = node;
        this.topic = topic;
        this.partition = partition;
    }

    public long offset() {
        return offset;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int partition() {
        return partition;
    }

    /**
     * Forwards a key/value pair to the downstream processors
     *
     * @param key   key
     * @param value value
     */
    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(K key, V value) {
        ProcessorNode currNode = node;
        try {
            for (ProcessorNode child : (List<ProcessorNode>) node.children()) {
                node = child;
                child.process(key, value);
            }
        } finally {
            node = currNode;
        }

    }

    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value, int childIndex) {
        ProcessorNode old = node;
        final ProcessorNode child = (ProcessorNode<K, V>) node.children().get(childIndex);
        node = child;
        try {
            child.process(key, value);
        } finally {
            node = old;
        }
    }

    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value, String childName) {
        for (ProcessorNode child : (List<ProcessorNode<K, V>>) node.children()) {
            if (child.name().equals(childName)) {
                ProcessorNode old = node;
                node = child;
                try {
                    child.process(key, value);
                    return;
                } finally {
                    node = old;
                }
            }
        }
    }

}
