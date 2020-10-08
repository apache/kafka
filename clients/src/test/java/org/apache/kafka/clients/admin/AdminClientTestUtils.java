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

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig;
import org.apache.kafka.common.TopicPartition;
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
        return new DeleteTopicsResult(Collections.singletonMap(topic, future));
    }

    /**
     * Helper to create a ListTopicsResult instance for a given topic.
     * ListTopicsResult's constructor is only accessible from within the
     * admin package.
     */
    public static ListTopicsResult listTopicsResult(String topic) {
        KafkaFutureImpl<Map<String, TopicListing>> future = new KafkaFutureImpl<>();
        future.complete(Collections.singletonMap(topic, new TopicListing(topic, false)));
        return new ListTopicsResult(future);
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
        return new DescribeTopicsResult(Collections.singletonMap(topic, future));
    }
}
