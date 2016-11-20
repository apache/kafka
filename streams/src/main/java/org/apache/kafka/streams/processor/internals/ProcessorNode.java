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

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ProcessorNode<K, V> {

    private final List<ProcessorNode<?, ?>> children;

    private final String name;
    private final Processor<K, V> processor;

    public final Set<String> stateStores;

    public ProcessorNode(String name) {
        this(name, null, null);
    }

    public ProcessorNode(String name, Processor<K, V> processor, Set<String> stateStores) {
        this.name = name;
        this.processor = processor;
        this.children = new ArrayList<>();
        this.stateStores = stateStores;
    }


    public final String name() {
        return name;
    }

    public final Processor<K, V> processor() {
        return processor;
    }

    public final List<ProcessorNode<?, ?>> children() {
        return children;
    }

    public void addChild(ProcessorNode<?, ?> child) {
        children.add(child);
    }

    public void init(ProcessorContext context) {
        try {
            processor.init(context);
        } catch (Exception e) {
            throw new StreamsException(String.format("failed to initialize processor %s", name), e);
        }
    }

    public void close() {
        try {
            processor.close();
        } catch (Exception e) {
            throw new StreamsException(String.format("failed to close processor %s", name), e);
        }
    }

    public void process(final K key, final V value) {
        processor.process(key, value);
    }

    /**
     * @return a string representation of this node, useful for debugging.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append(name + ": ");
        if (stateStores != null && !stateStores.isEmpty()) {
            sb.append("stateStores [");
            for (String store : (Set<String>) stateStores) {
                sb.append(store + ",");
            }
            sb.setLength(sb.length() - 1);
            sb.append("] ");
        }
        return sb.toString();
    }
}
