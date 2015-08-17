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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.streaming.processor.KafkaProcessor;
import org.apache.kafka.streaming.processor.ProcessorMetadata;
import org.apache.kafka.streaming.kstream.Predicate;

class KStreamBranch<K, V> extends KafkaProcessor<K, V, K, V> {

    private final Predicate<K, V>[] predicates;

    @SuppressWarnings("unchecked")
    public KStreamBranch(String name, ProcessorMetadata config) {
        super(name, config);

        if (this.config() == null)
            throw new IllegalStateException("ProcessorMetadata should be specified.");

        this.predicates = (Predicate<K, V>[]) config.value();
    }

    @Override
    public void process(K key, V value) {
        if (this.children().size() != this.predicates.length)
            throw new KafkaException("Number of branched streams does not match the length of predicates: this should not happen.");

        for (int i = 0; i < predicates.length; i++) {
            if (predicates[i].apply(key, value)) {
                // do not use forward here bu directly call process() and then break the loop
                // so that no record is going to be piped to multiple streams
                this.children().get(i).process(key, value);
                break;
            }
        }
    }
}
