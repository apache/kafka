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

package org.apache.kafka.streaming.kstream.internals;

import org.apache.kafka.streaming.processor.KafkaProcessor;
import org.apache.kafka.streaming.kstream.Predicate;
import org.apache.kafka.streaming.processor.ProcessorMetadata;

class KStreamFilter<K, V> extends KafkaProcessor<K, V, K, V> {

    private final PredicateOut<K, V> predicateOut;

    public static final class PredicateOut<K1, V1> {

        public final Predicate<K1, V1> predicate;
        public final boolean filterOut;

        public PredicateOut(Predicate<K1, V1> predicate) {
            this(predicate, false);
        }

        public PredicateOut(Predicate<K1, V1> predicate, boolean filterOut) {
            this.predicate = predicate;
            this.filterOut = filterOut;
        }
    }

    @SuppressWarnings("unchecked")
    public KStreamFilter(String name, ProcessorMetadata config) {
        super(name, config);

        if (this.config() == null)
            throw new IllegalStateException("ProcessorMetadata should be specified.");

        this.predicateOut = (PredicateOut<K, V>) config.value();
    }

    @Override
    public void process(K key, V value) {
        if ((!predicateOut.filterOut && predicateOut.predicate.apply(key, value))
            || (predicateOut.filterOut && !predicateOut.predicate.apply(key, value))) {
            forward(key, value);
        }
    }
}
