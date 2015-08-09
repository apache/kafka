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

import org.apache.kafka.clients.processor.Processor;
import org.apache.kafka.clients.processor.ProcessorContext;
import org.apache.kafka.clients.processor.Punctuator;
import org.apache.kafka.clients.processor.Receiver;

import java.util.ArrayList;

public class MockProcessor<K, V> implements Processor<K, V>, Receiver<K, V>, Punctuator {
    public final ArrayList<String> processed = new ArrayList<>();
    public final ArrayList<Long> punctuated = new ArrayList<>();

    @Override
    public void process(K key, V value) {
        processed.add(key + ":" + value);
    }

    @Override
    public void receive(K key, V value) {
        process(key, value);
    }

    @Override
    public void init(ProcessorContext context) {
    }

    @Override
    public void punctuate(long streamTime) {
        punctuated.add(streamTime);
    }

    @Override
    public void close() {

    }
}
