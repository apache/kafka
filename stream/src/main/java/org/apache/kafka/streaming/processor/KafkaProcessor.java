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

package org.apache.kafka.streaming.processor;

import java.util.ArrayList;
import java.util.List;

public abstract class KafkaProcessor<K1, V1, K2, V2> implements Processor<K1, V1, K2, V2>, Punctuator {

    private final List<KafkaProcessor<K2, V2, ?, ?>> children;
    private final List<KafkaProcessor<?, ?, K1, V1>> parents;

    private final String name;
    private final ProcessorMetadata metadata;

    public boolean initialized;

    public KafkaProcessor(String name) {
        this(name, null);
    }

    public KafkaProcessor(String name, ProcessorMetadata metadata) {
        this.name = name;
        this.metadata = metadata;

        this.children  = new ArrayList<>();
        this.parents = new ArrayList<>();

        this.initialized = false;
    }

    public String name() {
        return name;
    }

    public ProcessorMetadata metadata() {
        return metadata;
    }

    public List<KafkaProcessor<K2, V2, ?, ?>> children() {
        return children;
    }

    public List<KafkaProcessor<?, ?, K1, V1>> parents() {
        return parents;
    }

    public final void chain(KafkaProcessor<K2, V2, ?, ?> child) {
        child.parents.add(this);
        children.add(child);
    }

    @Override
    public final void forward(K2 key, V2 value) {
        for (KafkaProcessor<K2, V2, ?, ?> child : children) {
            child.process(key, value);
        }
    }

    /* Following functions can be overridden by users */

    @Override
    public void punctuate(long streamTime) {
        // do nothing
    }

    @Override
    public void init(ProcessorContext context) {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }
}
