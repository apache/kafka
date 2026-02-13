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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A client that consumes records from a Kafka cluster using a share group.
 *
 * @see KafkaShareConsumer
 * @see MockShareConsumer
 */
public interface ShareConsumer<K, V> extends Closeable {

    /**
     * @see KafkaShareConsumer#subscription()
     */
    Set<String> subscription();

    /**
     * @see KafkaShareConsumer#subscribe(Collection)
     */
    void subscribe(Collection<String> topics);

    /**
     * @see KafkaShareConsumer#unsubscribe()
     */
    void unsubscribe();

    /**
     * @see KafkaShareConsumer#poll(Duration)
     */
    ConsumerRecords<K, V> poll(Duration timeout);

    /**
     * @see KafkaShareConsumer#acknowledge(ConsumerRecord)
     */
    void acknowledge(ConsumerRecord<K, V> record);

    /**
     * @see KafkaShareConsumer#acknowledge(ConsumerRecord, AcknowledgeType)
     */
    void acknowledge(ConsumerRecord<K, V> record, AcknowledgeType type);

    /**
     * @see KafkaShareConsumer#acknowledge(String, int, long, AcknowledgeType)
     */
    void acknowledge(String topic, int partition, long offset, AcknowledgeType type);

    /**
     * @see KafkaShareConsumer#commitSync()
     */
    Map<TopicIdPartition, Optional<KafkaException>> commitSync();

    /**
     * @see KafkaShareConsumer#commitSync(Duration)
     */
    Map<TopicIdPartition, Optional<KafkaException>> commitSync(Duration timeout);

    /**
     * @see KafkaShareConsumer#commitAsync()
     */
    void commitAsync();

    /**
     * @see KafkaShareConsumer#setAcknowledgementCommitCallback(AcknowledgementCommitCallback)
     */
    void setAcknowledgementCommitCallback(AcknowledgementCommitCallback callback);

    /**
     * @see KafkaShareConsumer#clientInstanceId(Duration)
     */
    Uuid clientInstanceId(Duration timeout);

    /**
     * @see KafkaShareConsumer#acquisitionLockTimeoutMs()
     */
    Optional<Integer> acquisitionLockTimeoutMs();

    /**
     * @see KafkaShareConsumer#metrics()
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * @see KafkaShareConsumer#registerMetricForSubscription(KafkaMetric)
     */
    void registerMetricForSubscription(KafkaMetric metric);

    /**
     * @see KafkaShareConsumer#unregisterMetricFromSubscription(KafkaMetric)
     */
    void unregisterMetricFromSubscription(KafkaMetric metric);

    /**
     * @see KafkaShareConsumer#close()
     */
    void close();

    /**
     * @see KafkaShareConsumer#close(Duration)
     */
    void close(Duration timeout);

    /**
     * @see KafkaShareConsumer#wakeup()
     */
    void wakeup();
}
