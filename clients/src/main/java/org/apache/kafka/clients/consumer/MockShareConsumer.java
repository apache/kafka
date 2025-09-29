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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.LogContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;

/**
 * A mock of the {@link ShareConsumer} interface you can use for testing code that uses Kafka. This class is <i> not
 * thread-safe </i>.
 */
public class MockShareConsumer<K, V> implements ShareConsumer<K, V> {

    private final SubscriptionState subscriptions;
    private final AtomicBoolean wakeup;

    private final Map<TopicPartition, List<ConsumerRecord<K, V>>> records;

    private boolean closed;
    private Uuid clientInstanceId;

    public MockShareConsumer() {
        this.subscriptions = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        this.records = new HashMap<>();
        this.closed = false;
        this.wakeup = new AtomicBoolean(false);
    }

    @Override
    public synchronized Set<String> subscription() {
        ensureNotClosed();
        return subscriptions.subscription();
    }

    @Override
    public synchronized void subscribe(Collection<String> topics) {
        ensureNotClosed();
        subscriptions.subscribe(new HashSet<>(topics), Optional.empty());
    }

    @Override
    public synchronized void unsubscribe() {
        ensureNotClosed();
        subscriptions.unsubscribe();
    }

    @Override
    public synchronized ConsumerRecords<K, V> poll(Duration timeout) {
        ensureNotClosed();

        final Map<TopicPartition, List<ConsumerRecord<K, V>>> results = new HashMap<>();
        for (Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> entry : records.entrySet()) {
            final List<ConsumerRecord<K, V>> recs = entry.getValue();
            for (final ConsumerRecord<K, V> rec : recs) {
                results.computeIfAbsent(entry.getKey(), partition -> new ArrayList<>()).add(rec);
            }
        }

        records.clear();
        return new ConsumerRecords<>(results, Map.of());
    }

    @Override
    public synchronized void acknowledge(ConsumerRecord<K, V> record) {
    }

    @Override
    public synchronized void acknowledge(ConsumerRecord<K, V> record, AcknowledgeType type) {
    }

    @Override
    public synchronized Map<TopicIdPartition, Optional<KafkaException>> commitSync() {
        return new HashMap<>();
    }

    @Override
    public synchronized Map<TopicIdPartition, Optional<KafkaException>> commitSync(Duration timeout) {
        return new HashMap<>();
    }

    @Override
    public synchronized void commitAsync() {
    }

    @Override
    public void setAcknowledgementCommitCallback(AcknowledgementCommitCallback callback) {
    }

    public synchronized void setClientInstanceId(final Uuid clientInstanceId) {
        this.clientInstanceId = clientInstanceId;
    }

    @Override
    public synchronized Uuid clientInstanceId(Duration timeout) {
        if (clientInstanceId == null) {
            throw new UnsupportedOperationException("clientInstanceId not set");
        }

        return clientInstanceId;
    }

    @Override
    public synchronized Map<MetricName, ? extends Metric> metrics() {
        ensureNotClosed();
        return Collections.emptyMap();
    }

    @Override
    public synchronized void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    @Override
    public synchronized void close(Duration timeout) {
        closed = true;
    }

    @Override
    public synchronized void wakeup() {
        wakeup.set(true);
    }

    public synchronized void addRecord(ConsumerRecord<K, V> record) {
        ensureNotClosed();
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        if (!subscriptions.subscription().contains(record.topic()))
            throw new IllegalStateException("Cannot add records for a topics that is not subscribed by the consumer");
        List<ConsumerRecord<K, V>> recs = records.computeIfAbsent(tp, k -> new ArrayList<>());
        recs.add(record);
    }

    private void ensureNotClosed() {
        if (closed)
            throw new IllegalStateException("This consumer has already been closed.");
    }
}