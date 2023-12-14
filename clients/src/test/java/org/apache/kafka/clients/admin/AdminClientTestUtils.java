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
package org.apache.kafka.clients.admin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.HostResolver;
import org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.KafkaFutureImpl;

public class AdminClientTestUtils {

    /**
     * Helper to create a ListPartitionReassignmentsResult instance for a given Throwable.
     * ListPartitionReassignmentsResult's constructor is only accessible from within the
     * admin package.
     */
    public static ListPartitionReassignmentsResult listPartitionReassignmentsResult(Throwable t) {
        KafkaFutureImpl<Map<TopicPartition, PartitionReassignment>> future = new KafkaFutureImpl<>();
        future.completeExceptionally(t);
        return new ListPartitionReassignmentsResult(future);
    }

    /**
     * Helper to create a CreateTopicsResult instance for a given Throwable.
     * CreateTopicsResult's constructor is only accessible from within the
     * admin package.
     */
    public static CreateTopicsResult createTopicsResult(String topic, Throwable t) {
        KafkaFutureImpl<TopicMetadataAndConfig> future = new KafkaFutureImpl<>();
        future.completeExceptionally(t);
        return new CreateTopicsResult(Collections.singletonMap(topic, future));
    }

    /**
     * Helper to create a DeleteTopicsResult instance for a given Throwable.
     * DeleteTopicsResult's constructor is only accessible from within the
     * admin package.
     */
    public static DeleteTopicsResult deleteTopicsResult(String topic, Throwable t) {
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        future.completeExceptionally(t);
        return DeleteTopicsResult.ofTopicNames(Collections.singletonMap(topic, future));
    }

    /**
     * Helper to create a ListTopicsResult instance for a given topic.
     * ListTopicsResult's constructor is only accessible from within the
     * admin package.
     */
    public static ListTopicsResult listTopicsResult(String topic) {
        KafkaFutureImpl<Map<String, TopicListing>> future = new KafkaFutureImpl<>();
        future.complete(Collections.singletonMap(topic, new TopicListing(topic, Uuid.ZERO_UUID, false)));
        return new ListTopicsResult(future);
    }

    /**
     * Helper to create a AlterConfigsResult instance for a given Throwable.
     * AlterConfigsResult's constructor is only accessible from within the
     * admin package.
     */
    public static AlterConfigsResult alterConfigsResult(ConfigResource cr, Throwable t) {
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        Map<ConfigResource, KafkaFuture<Void>> futures = Collections.singletonMap(cr, future);
        future.completeExceptionally(t);
        return new AlterConfigsResult(futures);
    }

    /**
     * Helper to create a AlterConfigsResult instance for a given ConfigResource.
     * AlterConfigsResult's constructor is only accessible from within the
     * admin package.
     */
    public static AlterConfigsResult alterConfigsResult(ConfigResource cr) {
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        Map<ConfigResource, KafkaFuture<Void>> futures = Collections.singletonMap(cr, future);
        future.complete(null);
        return new AlterConfigsResult(futures);
    }

    /** Helper to create a DescribeConfigsResult instance for a given ConfigResource.
     * DescribeConfigsResult's constructor is only accessible from within the
     * admin package.
     */
    public static DescribeConfigsResult describeConfigsResult(ConfigResource cr, Config config) {
        KafkaFutureImpl<Config> future = new KafkaFutureImpl<>();
        future.complete(config);
        return new DescribeConfigsResult(Collections.singletonMap(cr, future));
    }

    /**
     * Helper to create a CreatePartitionsResult instance for a given Throwable.
     * CreatePartitionsResult's constructor is only accessible from within the
     * admin package.
     */
    public static CreatePartitionsResult createPartitionsResult(String topic, Throwable t) {
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        future.completeExceptionally(t);
        return new CreatePartitionsResult(Collections.singletonMap(topic, future));
    }

    /**
     * Helper to create a DescribeTopicsResult instance for a given topic.
     * DescribeTopicsResult's constructor is only accessible from within the
     * admin package.
     */
    public static DescribeTopicsResult describeTopicsResult(String topic, TopicDescription description) {
        KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
        future.complete(description);
        return DescribeTopicsResult.ofTopicNames(Collections.singletonMap(topic, future));
    }

    public static DescribeTopicsResult describeTopicsResult(Map<String, TopicDescription> topicDescriptions) {
        return DescribeTopicsResult.ofTopicNames(topicDescriptions.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> KafkaFuture.completedFuture(e.getValue()))));
    }

    public static ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult(Map<String, Map<TopicPartition, OffsetAndMetadata>> offsets) {
        Map<CoordinatorKey, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> resultMap = offsets.entrySet().stream()
            .collect(Collectors.toMap(e -> CoordinatorKey.byGroupId(e.getKey()),
                                      e -> KafkaFutureImpl.completedFuture(e.getValue())));
        return new ListConsumerGroupOffsetsResult(resultMap);
    }

    public static ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult(String group, KafkaException exception) {
        final KafkaFutureImpl<Map<TopicPartition, OffsetAndMetadata>> future = new KafkaFutureImpl<>();
        future.completeExceptionally(exception);
        return new ListConsumerGroupOffsetsResult(Collections.singletonMap(CoordinatorKey.byGroupId(group), future));
    }

    public static ListClientMetricsResourcesResult listClientMetricsResourcesResult(String... names) {
        return new ListClientMetricsResourcesResult(
                KafkaFuture.completedFuture(Arrays.stream(names)
                        .map(name -> new ClientMetricsResourceListing(name))
                        .collect(Collectors.toList())));
    }

    public static ListClientMetricsResourcesResult listClientMetricsResourcesResult(KafkaException exception) {
        final KafkaFutureImpl<Collection<ClientMetricsResourceListing>> future = new KafkaFutureImpl<>();
        future.completeExceptionally(exception);
        return new ListClientMetricsResourcesResult(future);
    }

    /**
     * Helper to create a KafkaAdminClient with a custom HostResolver accessible to tests outside this package.
     */
    public static Admin create(Map<String, Object> conf, HostResolver hostResolver) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(conf, true), null, hostResolver);
    }
}
