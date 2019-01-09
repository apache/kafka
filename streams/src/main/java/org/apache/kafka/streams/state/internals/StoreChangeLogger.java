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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.StateSerdes;

/**
 * Note that the use of array-typed keys is discouraged because they result in incorrect caching behavior.
 * If you intend to work on byte arrays as key, for example, you may want to wrap them with the {@code Bytes} class,
 * i.e. use {@code RocksDBStore<Bytes, ...>} rather than {@code RocksDBStore<byte[], ...>}.
 *
 * @param <K>
 * @param <V>
 */
class StoreChangeLogger<K, V> {

    protected final StateSerdes<K, V> serialization;

    private final String topic;
    private final int partition;
    private final ProcessorContext context;
    private final RecordCollector collector;

    StoreChangeLogger(final String storeName,
                      final ProcessorContext context,
                      final StateSerdes<K, V> serialization) {
        this(storeName, context, context.taskId().partition, serialization);
    }

    private StoreChangeLogger(final String storeName,
                              final ProcessorContext context,
                              final int partition,
                              final StateSerdes<K, V> serialization) {
        this.topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);
        this.context = context;
        this.partition = partition;
        this.serialization = serialization;
        this.collector = ((RecordCollector.Supplier) context).recordCollector();
    }

    void logChange(final K key,
                   final V value) {
        if (collector != null) {
            final Serializer<K> keySerializer = serialization.keySerializer();
            final Serializer<V> valueSerializer = serialization.valueSerializer();
            // Sending null headers to changelog topics (KIP-244)
            collector.send(this.topic, key, value, null, this.partition, context.timestamp(), keySerializer, valueSerializer);
        }
    }
}
