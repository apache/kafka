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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRegularExpression;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerProtocolAssignment;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import com.dynatrace.hash4j.hashing.HashStream64;
import com.dynatrace.hash4j.hashing.Hashing;
import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

public class Utils {
    private Utils() {}

    /**
     * @return An OptionalInt containing the value iff the value is different from
     * the sentinel (or default) value -1.
     */
    public static OptionalInt ofSentinel(int value) {
        return value != -1 ? OptionalInt.of(value) : OptionalInt.empty();
    }

    /**
     * @return An OptionalLong containing the value iff the value is different from
     * the sentinel (or default) value -1.
     */
    public static OptionalLong ofSentinel(long value) {
        return value != -1 ? OptionalLong.of(value) : OptionalLong.empty();
    }

    /**
     * @return The provided assignment as a String.
     *
     * Example:
     * [topicid1-0, topicid1-1, topicid2-0, topicid2-1]
     */
    public static String assignmentToString(
        Map<Uuid, Set<Integer>> assignment
    ) {
        StringBuilder builder = new StringBuilder("[");
        Iterator<Map.Entry<Uuid, Set<Integer>>> topicsIterator = assignment.entrySet().iterator();
        while (topicsIterator.hasNext()) {
            Map.Entry<Uuid, Set<Integer>> entry = topicsIterator.next();
            Iterator<Integer> partitionsIterator = entry.getValue().iterator();
            while (partitionsIterator.hasNext()) {
                builder.append(entry.getKey());
                builder.append("-");
                builder.append(partitionsIterator.next());
                if (partitionsIterator.hasNext() || topicsIterator.hasNext()) {
                    builder.append(", ");
                }
            }
        }
        builder.append("]");
        return builder.toString();
    }

    /**
     * Decrements value by 1; returns null when reaching zero. This helper is
     * meant to be used with Map#compute.
     */
    public static Integer decValue(String key, Integer value) {
        if (value == null) return null;
        return value == 1 ? null : value - 1;
    }

    /**
     * Increments value by 1; This helper is meant to be used with Map#compute.
     */
    public static Integer incValue(String key, Integer value) {
        return value == null ? 1 : value + 1;
    }

    /**
     * Decrements value by 1; returns null when reaching zero. This helper is
     * meant to be used with Map#compute.
     */
    public static Long decValue(Object key, Long value) {
        if (value == null) return null;
        return value == 1 ? null : value - 1;
    }

    /**
     * Increments value by 1; This helper is meant to be used with Map#compute.
     */
    public static Long incValue(Object key, Long value) {
        return value == null ? 1 : value + 1;
    }

    /**
     * @return An Optional containing the provided string if it is not null and not empty,
     *         otherwise an empty Optional.
     */
    public static Optional<String> toOptional(String str) {
        return str == null || str.isEmpty() ? Optional.empty() : Optional.of(str);
    }

    /**
     * Converts a map of topic id and partition set to a ConsumerProtocolAssignment.
     *
     * @param assignment    The map to convert.
     * @param image   The CoordinatorMetadataImage.
     * @return The converted ConsumerProtocolAssignment.
     */
    public static ConsumerProtocolAssignment toConsumerProtocolAssignment(
        Map<Uuid, Set<Integer>> assignment,
        CoordinatorMetadataImage image
    ) {
        ConsumerProtocolAssignment.TopicPartitionCollection collection =
            new ConsumerProtocolAssignment.TopicPartitionCollection();
        assignment.forEach((topicId, partitions) -> {
            image.topicMetadata(topicId).ifPresent(topicMetadata ->
                collection.add(new ConsumerProtocolAssignment.TopicPartition()
                    .setTopic(topicMetadata.name())
                    .setPartitions(new ArrayList<>(partitions))));
        });
        return new ConsumerProtocolAssignment()
            .setAssignedPartitions(collection);
    }

    /**
     * Converts a map of topic id and partition set to a ConsumerProtocolAssignment.
     *
     * @param consumerProtocolAssignment The ConsumerProtocolAssignment.
     * @param metadataImage              The Metadata image.
     * @return The converted map.
     */
    public static Map<Uuid, Set<Integer>> toTopicPartitionMap(
        ConsumerProtocolAssignment consumerProtocolAssignment,
        CoordinatorMetadataImage metadataImage
    ) {
        Map<Uuid, Set<Integer>> topicPartitionMap = new HashMap<>();
        consumerProtocolAssignment.assignedPartitions().forEach(topicPartition -> {
            metadataImage.topicMetadata(topicPartition.topic()).ifPresent(topicMetadata -> {
                topicPartitionMap.put(topicMetadata.id(), new HashSet<>(topicPartition.partitions()));
            });
        });
        return topicPartitionMap;
    }

    /**
     * Converts a ConsumerProtocolSubscription.TopicPartitionCollection to a list of ConsumerGroupHeartbeatRequestData.TopicPartitions.
     *
     * @param topicPartitionCollection The TopicPartitionCollection to convert.
     * @param metadataImage            The Metadata image.
     * @return a list of ConsumerGroupHeartbeatRequestData.TopicPartitions.
     */
    public static List<ConsumerGroupHeartbeatRequestData.TopicPartitions> toTopicPartitions(
        ConsumerProtocolSubscription.TopicPartitionCollection topicPartitionCollection,
        CoordinatorMetadataImage metadataImage
    ) {
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> res = new ArrayList<>();
        for (ConsumerProtocolSubscription.TopicPartition tp : topicPartitionCollection) {
            metadataImage.topicMetadata(tp.topic()).ifPresent(topicMetadata -> {
                res.add(
                    new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(topicMetadata.id())
                        .setPartitions(tp.partitions())
                );
            });
        }
        return res;
    }

    /**
     * Creates a map of topic id and partition set from a list of consumer group TopicPartitions.
     *
     * @param topicPartitionsList   The list of TopicPartitions.
     * @return a map of topic id and partition set.
     */
    public static Map<Uuid, Set<Integer>> assignmentFromTopicPartitions(
        List<ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions> topicPartitionsList
    ) {
        return topicPartitionsList.stream().collect(Collectors.toMap(
            ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions::topicId,
            topicPartitions -> Collections.unmodifiableSet(new HashSet<>(topicPartitions.partitions()))));
    }

    /**
     * Creates a map of topic id and partition set from a list of share group TopicPartitions.
     *
     * @param topicPartitionsList   The list of TopicPartitions.
     * @return a map of topic id and partition set.
     */
    public static Map<Uuid, Set<Integer>> assignmentFromShareGroupTopicPartitions(
            List<ShareGroupCurrentMemberAssignmentValue.TopicPartitions> topicPartitionsList
    ) {
        return topicPartitionsList.stream().collect(Collectors.toMap(
                ShareGroupCurrentMemberAssignmentValue.TopicPartitions::topicId,
                topicPartitions -> Collections.unmodifiableSet(new HashSet<>(topicPartitions.partitions()))));
    }

    /**
     * @return The ApiMessage or null.
     */
    public static ApiMessage messageOrNull(ApiMessageAndVersion apiMessageAndVersion) {
        if (apiMessageAndVersion == null) {
            return null;
        } else {
            return apiMessageAndVersion.message();
        }
    }

    /**
     * Throws an InvalidRequestException if the value is non-null and empty.
     * A string containing only whitespaces is also considered empty.
     *
     * @param value The value.
     * @param error The error message.
     * @throws InvalidRequestException
     */
    static void throwIfEmptyString(
        String value,
        String error
    ) throws InvalidRequestException {
        if (value != null && value.trim().isEmpty()) {
            throw new InvalidRequestException(error);
        }
    }

    /**
     * Throws an InvalidRequestException if the value is null or non-empty.
     *
     * @param value The value.
     * @param error The error message.
     * @throws InvalidRequestException
     */
    static void throwIfNotEmptyCollection(
        Collection<?> value,
        String error
    ) throws InvalidRequestException {
        if (value == null || !value.isEmpty()) {
            throw new InvalidRequestException(error);
        }
    }

    /**
     * Throws an InvalidRequestException if the value is not null and non-empty.
     *
     * @param value The value.
     * @param error The error message.
     * @throws InvalidRequestException
     */
    static void throwIfNotNullOrEmpty(
        Collection<?> value,
        String error
    ) throws InvalidRequestException {
        if (value != null && !value.isEmpty()) {
            throw new InvalidRequestException(error);
        }
    }

    /**
     * Throws an InvalidRequestException if the value is non-null.
     *
     * @param value The value.
     * @param error The error message.
     * @throws InvalidRequestException
     */
    static void throwIfNotNull(
        Object value,
        String error
    ) throws InvalidRequestException {
        if (value != null) {
            throw new InvalidRequestException(error);
        }
    }

    /**
     * Throws an InvalidRequestException if the value is null.
     *
     * @param value The value.
     * @param error The error message.
     * @throws InvalidRequestException
     */
    static void throwIfNull(
        Object value,
        String error
    ) throws InvalidRequestException {
        if (value == null) {
            throw new InvalidRequestException(error);
        }
    }

    /**
     * Validates if the provided regular expression is valid.
     *
     * @param regex The regular expression to validate.
     * @throws InvalidRegularExpression if the regular expression is invalid.
     */
    static void throwIfRegularExpressionIsInvalid(
        String regex
    ) throws InvalidRegularExpression {
        try {
            Pattern.compile(regex);
        } catch (PatternSyntaxException ex) {
            throw new InvalidRegularExpression(
                String.format("SubscribedTopicRegex `%s` is not a valid regular expression: %s.",
                    regex, ex.getDescription()));
        }
    }

    /**
     * The magic byte used to identify the version of topic hash function.
     */
    static final byte TOPIC_HASH_MAGIC_BYTE = 0x00;

    /**
     * Computes the hash of the topics in a group.
     * <p>
     * The computed hash value is stored as the metadata hash in the *GroupMetadataValue.
     * <p>
     * If there is no topic, the hash value is set to 0.
     * The hashing process involves the following steps:
     * 1. Sort the topic hashes by topic name.
     * 2. Write each topic hash in order.
     *
     * @param topicHashes The map of topic hashes. Key is topic name and value is the topic hash.
     * @return The hash of the group.
     */
    public static long computeGroupHash(Map<String, Long> topicHashes) {
        if (topicHashes.isEmpty()) {
            return 0;
        }

        // Sort entries by topic name
        List<Map.Entry<String, Long>> sortedEntries = new ArrayList<>(topicHashes.entrySet());
        sortedEntries.sort(Map.Entry.comparingByKey());

        HashStream64 hasher = Hashing.xxh3_64().hashStream();
        for (Map.Entry<String, Long> entry : sortedEntries) {
            hasher.putLong(entry.getValue());
        }

        return hasher.getAsLong();
    }

    /**
     * Computes the hash of the topic id, name, number of partitions, and partition racks by streaming XXH3.
     * <p>
     * The computed hash value for the topic is utilized in conjunction with the {@link #computeGroupHash(Map)}
     * method and is stored as part of the metadata hash in the *GroupMetadataValue.
     * It is important to note that if the hash algorithm is changed, the magic byte must be updated to reflect the
     * new hash version.
     * <p>
     * For non-existent topics, the hash value is set to 0.
     * For existent topics, the hashing process involves the following steps:
     * 1. Write a magic byte to denote the version of the hash function.
     * 2. Write the hash code of the topic ID with mostSignificantBits and leastSignificantBits.
     * 3. Write the topic name.
     * 4. Write the number of partitions associated with the topic.
     * 5. For each partition, write the partition ID and a sorted list of rack identifiers.
     * - Rack identifiers are formatted as "<length1><value1><length2><value2>" to prevent issues with simple separators.
     *
     * @param topicName The topic name.
     * @param metadataImage The topic metadata.
     * @return The hash of the topic.
     */
    public static long computeTopicHash(String topicName, CoordinatorMetadataImage metadataImage) {
        Optional<CoordinatorMetadataImage.TopicMetadata> topicImage = metadataImage.topicMetadata(topicName);
        if (topicImage.isEmpty()) {
            return 0;
        }

        CoordinatorMetadataImage.TopicMetadata topicMetadata = topicImage.get();

        HashStream64 hasher = Hashing.xxh3_64().hashStream();
        hasher = hasher
            .putByte(TOPIC_HASH_MAGIC_BYTE)
            .putLong(topicMetadata.id().getMostSignificantBits())
            .putLong(topicMetadata.id().getLeastSignificantBits())
            .putString(topicMetadata.name())
            .putInt(topicMetadata.partitionCount());

        for (int i = 0; i < topicMetadata.partitionCount(); i++) {
            hasher = hasher.putInt(i);
            List<String> partitionRacks = topicMetadata.partitionRacks(i);
            Collections.sort(partitionRacks);

            for (String rack : partitionRacks) {
                // Format: "<length><value>"
                // The rack string combination cannot use simple separator like ",", because there is no limitation for rack character.
                // If using simple separator like "," it may hit edge case like ",," and ",,," / ",,," and ",,".
                // Add length before the rack string to avoid the edge case.
                hasher = hasher.putInt(rack.length()).putString(rack);
            }
        }

        return hasher.getAsLong();
    }
}
