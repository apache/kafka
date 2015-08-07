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

package org.apache.kafka.stream.topology.internals;

import org.apache.kafka.clients.processor.KafkaProcessor;
import org.apache.kafka.clients.processor.ProcessorContext;
import org.apache.kafka.stream.topology.Predicate;

class KStreamFilter<K, V> extends KafkaProcessor<K, V, K, V> {

    private static final String FILTER_NAME = "KAFKA-FILTER";

    private final Predicate<K, V> predicate;
    private final boolean filterOut;

    public KStreamFilter(Predicate<K, V> predicate) {
        this(predicate, false);
    }

    public KStreamFilter(Predicate<K, V> predicate, boolean filterOut) {
        super(FILTER_NAME);

        this.predicate = predicate;
        this.filterOut = filterOut;
    }

    @Override
    public void init(ProcessorContext context) {
        // do nothing
    }

    @Override
    public void process(K key, V value) {
        if ((!filterOut && predicate.apply(key, value))
            || (filterOut && !predicate.apply(key, value))) {
            forward(key, value);
        }
    }

    @Override
    public void close() {
        // do nothing
    }
}
