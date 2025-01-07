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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class DefaultStreamPartitioner<K, V> implements StreamPartitioner<K, V> {

    private final Serializer<K> keySerializer;

    public DefaultStreamPartitioner(final Serializer<K> keySerializer) {
        this.keySerializer = keySerializer;
    }

    @Override
    public Optional<Set<Integer>> partitions(final String topic, final K key, final V value, final int numPartitions) {
        final byte[] keyBytes = keySerializer.serialize(topic, key);

        // if the key bytes are not available, we just return empty optional to let the producer decide
        // which partition to send internally; otherwise stick with the same built-in partitioner
        // util functions that producer used to make sure its behavior is consistent with the producer
        if (keyBytes == null) {
            return Optional.empty();
        } else {
            return Optional.of(Collections.singleton(BuiltInPartitioner.partitionForKey(keyBytes, numPartitions)));
        }
    }
}
