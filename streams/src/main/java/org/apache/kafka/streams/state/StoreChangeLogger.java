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

package org.apache.kafka.streams.state;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;

import java.util.HashSet;
import java.util.Set;

public class StoreChangeLogger<K, V> {

    public interface ValueGetter<K, V> {
        V get(K key);
    }

    protected final Serdes<K, V> serialization;

    private final Set<K> dirty;
    private final Set<K> removed;
    private final int maxDirty;
    private final int maxRemoved;

    private final String topic;
    private int partition;
    private ProcessorContext context;

    // always wrap the logged store with the metered store
    public StoreChangeLogger(String topic, ProcessorContext context, Serdes<K, V> serialization) {
        this.topic = topic;
        this.serialization = serialization;
        this.context = context;
        this.partition = context.id().partition;

        this.dirty = new HashSet<>();
        this.removed = new HashSet<>();
        this.maxDirty = 100; // TODO: this needs to be configurable
        this.maxRemoved = 100; // TODO: this needs to be configurable
    }

    public void add(K key) {
        this.dirty.add(key);
        this.removed.remove(key);
    }

    public void delete(K key) {
        this.dirty.remove(key);
        this.removed.add(key);
    }

    public void maybeLogChange(ValueGetter<K, V> getter) {
        if (this.dirty.size() > this.maxDirty || this.removed.size() > this.maxRemoved)
            logChange(getter);
    }

    public void logChange(ValueGetter<K, V> getter) {
        RecordCollector collector = ((RecordCollector.Supplier) context).recordCollector();
        if (collector != null) {
            Serializer<K> keySerializer = serialization.keySerializer();
            Serializer<V> valueSerializer = serialization.valueSerializer();

            for (K k : this.removed) {
                collector.send(new ProducerRecord<>(this.topic, this.partition, k, (V) null), keySerializer, valueSerializer);
            }
            for (K k : this.dirty) {
                V v = getter.get(k);
                collector.send(new ProducerRecord<>(this.topic, this.partition, k, v), keySerializer, valueSerializer);
            }
            this.removed.clear();
            this.dirty.clear();
        }
    }

}
