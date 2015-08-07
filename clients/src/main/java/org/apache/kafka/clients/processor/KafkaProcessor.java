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

package org.apache.kafka.clients.processor;

import java.util.ArrayList;
import java.util.List;

public abstract class KafkaProcessor<K1, V1, K2, V2> implements Processor<K1, V1>, Receiver<K1, V1>, Punctuator {

    protected final List<KafkaProcessor<K2, V2, ?, ?>> children;

    private final String name;

    public KafkaProcessor(String name) {
        this.name = name;
        this.children  = new ArrayList<>();
    }

    public String name() { return name; }

    public final void chain(KafkaProcessor<K2, V2, ?, ?> child) {
        children.add(child);
    }

    public final void forward(K2 key, V2 value) {
        for (KafkaProcessor<K2, V2, ?, ?> child : children) {
            child.receive(key, value);
        }
    }

    @Override
    public final void receive(K1 key, V1 value) {
        this.process(key, value);
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
        for (KafkaProcessor child : children) {
            child.close();
        }
    }
}
