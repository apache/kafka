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
import org.apache.kafka.common.annotation.InterfaceStability;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @see KafkaShareConsumer
 * @see MockShareConsumer
 */
@InterfaceStability.Evolving
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
     * @see KafkaShareConsumer#setAcknowledgeCommitCallback(AcknowledgeCommitCallback)
     */
    void setAcknowledgeCommitCallback(AcknowledgeCommitCallback callback);

    /**
     * See {@link KafkaShareConsumer#clientInstanceId(Duration)}}
     */
    Uuid clientInstanceId(Duration timeout);

    /**
     * @see KafkaShareConsumer#metrics()
     */
    Map<MetricName, ? extends Metric> metrics();

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
