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

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class DefaultStreamPartitioner<K, V> implements StreamPartitioner<K, V> {

    private final Cluster cluster;
    private final Serializer<K> keySerializer;
    private final DefaultPartitioner defaultPartitioner;

    public DefaultStreamPartitioner(final Serializer<K> keySerializer, final Cluster cluster) {
        this.cluster = cluster;
        this.keySerializer = keySerializer;
        this.defaultPartitioner = new DefaultPartitioner();
    }

    @Override
    public Integer partition(final String topic, final K key, final V value, final int numPartitions) {
        final byte[] keyBytes = keySerializer.serialize(topic, key);
        return defaultPartitioner.partition(topic, key, keyBytes, value, null, cluster);
    }
}
