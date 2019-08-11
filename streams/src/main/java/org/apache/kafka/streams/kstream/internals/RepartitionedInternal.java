/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.InternalTopicProperties;

class RepartitionedInternal<K, V> extends Repartitioned<K, V> {

    RepartitionedInternal(final Repartitioned<K, V> repartitioned) {
        super(repartitioned);
    }

    InternalTopicProperties toInternalTopicProperties() {
        return new InternalTopicProperties(numberOfPartitions());
    }

    String name() {
        return name;
    }

    Serde<K> keySerde() {
        return keySerde;
    }

    Serde<V> valueSerde() {
        return valueSerde;
    }

    StreamPartitioner<K, V> streamPartitioner() {
        return partitioner;
    }

    Integer numberOfPartitions() {
        return numberOfPartitions;
    }
}
