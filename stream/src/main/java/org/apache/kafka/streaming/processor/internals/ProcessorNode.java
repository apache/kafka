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

package org.apache.kafka.streaming.processor.internals;

import org.apache.kafka.streaming.processor.KafkaProcessor;
import org.apache.kafka.streaming.processor.ProcessorContext;

import java.util.ArrayList;
import java.util.List;

public class ProcessorNode<K1, V1, K2, V2> {

    private final List<ProcessorNode<K2, V2, ?, ?>> children;
    private final List<ProcessorNode<?, ?, K1, V1>> parents;

    private final String name;
    private final KafkaProcessor<K1, V1> processor;

    public boolean initialized;

    public ProcessorNode(String name) {
        this(name, null);
    }

    public ProcessorNode(String name, KafkaProcessor<K1, V1> processor) {
        this.name = name;
        this.processor = processor;
        this.parents = new ArrayList<>();
        this.children = new ArrayList<>();

        this.initialized = false;
    }

    public String name() {
        return name;
    }

    public KafkaProcessor<K1, V1> processor() {
        return processor;
    }

    public List<ProcessorNode<?, ?, K1, V1>> parents() {
        return parents;
    }

    public List<ProcessorNode<K2, V2, ?, ?>> children() {
        return children;
    }

    public final void chain(ProcessorNode<K2, V2, ?, ?> child) {
        child.parents.add(this);
        children.add(child);
    }

    public void init(ProcessorContext context) {
        processor.init(context);
    }

    public void process(K1 key, V1 value) {
        processor.process(key, value);
    }

    public void close() {
        processor.close();
    }
}
