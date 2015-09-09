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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.ProcessorDef;

class KStreamFilter<K, V> implements ProcessorDef {

    private final Predicate<K, V> predicate;
    private final boolean filterOut;

    public KStreamFilter(Predicate<K, V> predicate, boolean filterOut) {
        this.predicate = predicate;
        this.filterOut = filterOut;
    }

    @Override
    public Processor instance() {
        return new KStreamFilterProcessor();
    }

    private class KStreamFilterProcessor extends KStreamProcessor<K, V> {
        @Override
        public void process(K key, V value) {
            if (filterOut ^ predicate.apply(key, value)) {
                context.forward(key, value);
            }
        }
    }
}
