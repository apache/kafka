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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ClientUtils {

    // currently admin client is shared among all threads
    public static String getSharedAdminClientId(final String clientId) {
        return clientId + "-admin";
    }

    public static String getConsumerClientId(final String threadClientId) {
        return threadClientId + "-consumer";
    }

    public static String getRestoreConsumerClientId(final String threadClientId) {
        return threadClientId + "-restore-consumer";
    }

    public static String getThreadProducerClientId(final String threadClientId) {
        return threadClientId + "-producer";
    }

    public static String getTaskProducerClientId(final String threadClientId, final TaskId taskId) {
        return threadClientId + "-" + taskId + "-producer";
    }

    public static Map<MetricName, Metric> consumerMetrics(final Consumer<byte[], byte[]> mainConsumer,
                                                          final Consumer<byte[], byte[]> restoreConsumer) {
        final Map<MetricName, ? extends Metric> consumerMetrics = mainConsumer.metrics();
        final Map<MetricName, ? extends Metric> restoreConsumerMetrics = restoreConsumer.metrics();
        final LinkedHashMap<MetricName, Metric> result = new LinkedHashMap<>();
        result.putAll(consumerMetrics);
        result.putAll(restoreConsumerMetrics);
        return result;
    }

    public static Map<MetricName, Metric> adminClientMetrics(final Admin adminClient) {
        final Map<MetricName, ? extends Metric> adminClientMetrics = adminClient.metrics();
        return new LinkedHashMap<>(adminClientMetrics);
    }

    public static Map<MetricName, Metric> producerMetrics(final Collection<StreamsProducer> producers) {
        final Map<MetricName, Metric> result = new LinkedHashMap<>();
        for (final StreamsProducer producer : producers) {
            final Map<MetricName, ? extends Metric> producerMetrics = producer.metrics();
            if (producerMetrics != null) {
                result.putAll(producerMetrics);
            }
        }
        return result;
    }

    public static Map<TopicPartition, ListOffsetsResultInfo> fetchEndOffsetsWithoutTimeout(final Collection<TopicPartition> partitions,
                                                                                           final Admin adminClient) {
        return fetchEndOffsets(partitions, adminClient, null);
    }

    public static Map<TopicPartition, ListOffsetsResultInfo> fetchEndOffsets(final Collection<TopicPartition> partitions,
                                                                             final Admin adminClient,
                                                                             final Duration timeout) {
        final Map<TopicPartition, ListOffsetsResultInfo> endOffsets;
        try {
            final KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> future =  adminClient.listOffsets(
                partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest())))
                                                                                        .all();
            if (timeout == null) {
                endOffsets = future.get();
            } else {
                endOffsets = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            }
        } catch (final TimeoutException | RuntimeException | InterruptedException | ExecutionException e) {
            throw new StreamsException("Unable to obtain end offsets from kafka", e);
        }
        return endOffsets;
    }
}
