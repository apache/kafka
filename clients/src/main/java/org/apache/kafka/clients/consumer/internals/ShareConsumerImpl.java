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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeCommitCallback;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This {@link ShareConsumer} implementation uses an {@link ApplicationEventHandler event handler} to process
 * {@link ApplicationEvent application events} so that the network I/O can be processed in a dedicated
 * {@link ConsumerNetworkThread network thread}.
 */
public class ShareConsumerImpl<K, V> implements ShareConsumer<K, V> {

    private final Deserializers<K, V> deserializers;

    ShareConsumerImpl(final ConsumerConfig config,
                      final Deserializer<K> keyDeserializer,
                      final Deserializer<V> valueDeserializer) {
        this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer);
    }

    @Override
    public Set<String> subscription() {
        return Collections.emptySet();
    }

    @Override
    public void subscribe(Collection<String> topics) {
    }

    @Override
    public void unsubscribe() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized ConsumerRecords<K, V> poll(Duration timeout) {
        return new ConsumerRecords<K, V>(Collections.EMPTY_MAP);
    }

    @Override
    public void acknowledge(ConsumerRecord<K, V> record) {
    }

    @Override
    public void acknowledge(ConsumerRecord<K, V> record, AcknowledgeType type) {
    }

    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync() {
        return new HashMap<>();
    }

    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync(Duration timeout) {
        return new HashMap<>();
    }

    @Override
    public void commitAsync() {
    }

    @Override
    public void setAcknowledgeCommitCallback(AcknowledgeCommitCallback callback) {
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public void close(Duration timeout) {
    }

    @Override
    public void wakeup() {
    }
}