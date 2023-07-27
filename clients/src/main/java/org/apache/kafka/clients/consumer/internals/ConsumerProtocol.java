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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ConsumerProtocolAssignment;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.SchemaException;

import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * ConsumerProtocol contains the schemas for consumer subscriptions and assignments for use with
 * Kafka's generalized group management protocol.
 *
 * The current implementation assumes that future versions will not break compatibility. When
 * it encounters a newer version, it parses it using the current format. This basically means
 * that new versions cannot remove or reorder any of the existing fields.
 */
public class ConsumerProtocol {
    public static final String PROTOCOL_TYPE = "consumer";

    static {
        // Safety check to ensure that both parts of the consumer protocol remain in sync.
        if (ConsumerProtocolSubscription.LOWEST_SUPPORTED_VERSION
                != ConsumerProtocolAssignment.LOWEST_SUPPORTED_VERSION)
            throw new IllegalStateException("Subscription and Assignment schemas must have the " +
                "same lowest version");

        if (ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION
                != ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION)
            throw new IllegalStateException("Subscription and Assignment schemas must have the " +
                "same highest version");
    }

    public static short deserializeVersion(final ByteBuffer buffer) {
        try {
            return buffer.getShort();
        } catch (BufferUnderflowException e) {
            throw new SchemaException("Buffer underflow while parsing consumer protocol's header", e);
        }
    }

    public static ByteBuffer serializeSubscription(final Subscription subscription) {
        return serializeSubscription(subscription, ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION);
    }

    public static ByteBuffer serializeSubscription(final Subscription subscription, short version) {
        version = checkSubscriptionVersion(version);

        ConsumerProtocolSubscription data = new ConsumerProtocolSubscription();

        List<String> topics = new ArrayList<>(subscription.topics());
        Collections.sort(topics);
        data.setTopics(topics);

        data.setUserData(subscription.userData() != null ? subscription.userData().duplicate() : null);

        List<TopicPartition> ownedPartitions = new ArrayList<>(subscription.ownedPartitions());
        ownedPartitions.sort(Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
        ConsumerProtocolSubscription.TopicPartition partition = null;
        for (TopicPartition tp : ownedPartitions) {
            if (partition == null || !partition.topic().equals(tp.topic())) {
                partition = new ConsumerProtocolSubscription.TopicPartition().setTopic(tp.topic());
                data.ownedPartitions().add(partition);
            }
            partition.partitions().add(tp.partition());
        }
        subscription.rackId().ifPresent(data::setRackId);

        data.setGenerationId(subscription.generationId().orElse(-1));
        return MessageUtil.toVersionPrefixedByteBuffer(version, data);
    }

    public static Subscription deserializeSubscription(final ByteBuffer buffer, short version) {
        version = checkSubscriptionVersion(version);

        try {
            ConsumerProtocolSubscription data =
                new ConsumerProtocolSubscription(new ByteBufferAccessor(buffer), version);

            List<TopicPartition> ownedPartitions = new ArrayList<>();
            for (ConsumerProtocolSubscription.TopicPartition tp : data.ownedPartitions()) {
                for (Integer partition : tp.partitions()) {
                    ownedPartitions.add(new TopicPartition(tp.topic(), partition));
                }
            }

            return new Subscription(
                data.topics(),
                data.userData() != null ? data.userData().duplicate() : null,
                ownedPartitions,
                data.generationId(),
                data.rackId() == null || data.rackId().isEmpty() ? Optional.empty() : Optional.of(data.rackId()));
        } catch (BufferUnderflowException e) {
            throw new SchemaException("Buffer underflow while parsing consumer protocol's subscription", e);
        }
    }

    public static Subscription deserializeSubscription(final ByteBuffer buffer) {
        return deserializeSubscription(buffer, deserializeVersion(buffer));
    }

    public static ByteBuffer serializeAssignment(final Assignment assignment) {
        return serializeAssignment(assignment, ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION);
    }

    public static ByteBuffer serializeAssignment(final Assignment assignment, short version) {
        version = checkAssignmentVersion(version);

        ConsumerProtocolAssignment data = new ConsumerProtocolAssignment();
        data.setUserData(assignment.userData() != null ? assignment.userData().duplicate() : null);
        assignment.partitions().forEach(tp -> {
            ConsumerProtocolAssignment.TopicPartition partition = data.assignedPartitions().find(tp.topic());
            if (partition == null) {
                partition = new ConsumerProtocolAssignment.TopicPartition().setTopic(tp.topic());
                data.assignedPartitions().add(partition);
            }
            partition.partitions().add(tp.partition());
        });
        return MessageUtil.toVersionPrefixedByteBuffer(version, data);
    }

    public static Assignment deserializeAssignment(final ByteBuffer buffer, short version) {
        version = checkAssignmentVersion(version);

        try {
            ConsumerProtocolAssignment data =
                new ConsumerProtocolAssignment(new ByteBufferAccessor(buffer), version);

            List<TopicPartition> assignedPartitions = new ArrayList<>();
            for (ConsumerProtocolAssignment.TopicPartition tp : data.assignedPartitions()) {
                for (Integer partition : tp.partitions()) {
                    assignedPartitions.add(new TopicPartition(tp.topic(), partition));
                }
            }

            return new Assignment(
                assignedPartitions,
                data.userData() != null ? data.userData().duplicate() : null);
        } catch (BufferUnderflowException e) {
            throw new SchemaException("Buffer underflow while parsing consumer protocol's assignment", e);
        }
    }

    public static Assignment deserializeAssignment(final ByteBuffer buffer) {
        return deserializeAssignment(buffer, deserializeVersion(buffer));
    }

    private static short checkSubscriptionVersion(final short version) {
        if (version < ConsumerProtocolSubscription.LOWEST_SUPPORTED_VERSION)
            throw new SchemaException("Unsupported subscription version: " + version);
        else if (version > ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION)
            return ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION;
        else
            return version;
    }

    private static short checkAssignmentVersion(final short version) {
        if (version < ConsumerProtocolAssignment.LOWEST_SUPPORTED_VERSION)
            throw new SchemaException("Unsupported assignment version: " + version);
        else if (version > ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION)
            return ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION;
        else
            return version;
    }
}
