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
import org.apache.kafka.streaming.kstream.KeyValue;
import org.apache.kafka.streaming.kstream.KeyValueMapper;
import org.apache.kafka.streaming.processor.ProcessorMetadata;

class KStreamFlatMap<K1, V1, K2, V2> extends KafkaProcessor<K1, V1, K2, V2> {

    private final KeyValueMapper<K1, V1, K2, ? extends Iterable<V2>> mapper;

    @SuppressWarnings("unchecked")
    KStreamFlatMap(String name, ProcessorMetadata config) {
        super(name, config);

        if (this.config() == null)
            throw new IllegalStateException("ProcessorMetadata should be specified.");

        this.mapper = (KeyValueMapper<K1, V1, K2, ? extends Iterable<V2>>) config.value();
    }

    @Override
    public void process(K1 key, V1 value) {
        KeyValue<K2, ? extends Iterable<V2>> newPair = mapper.apply(key, value);
        for (V2 v : newPair.value) {
            forward(newPair.key, v);
        }
    }
}
