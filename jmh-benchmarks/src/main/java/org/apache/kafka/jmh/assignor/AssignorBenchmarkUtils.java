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
package org.apache.kafka.jmh.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.TopicIds;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.TopicsImage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;


public class AssignorBenchmarkUtils {
    /**
     * Generate a reverse look up map of partition to member target assignments from the given member spec.
     *
     * @param groupAssignment       The group assignment.
     * @return Map of topic partition to member assignments.
     */
    public static Map<Uuid, Map<Integer, String>> computeInvertedTargetAssignment(
        GroupAssignment groupAssignment
    ) {
        Map<Uuid, Map<Integer, String>> invertedTargetAssignment = new HashMap<>();
        for (Map.Entry<String, MemberAssignment> memberEntry : groupAssignment.members().entrySet()) {
            String memberId = memberEntry.getKey();
            Map<Uuid, Set<Integer>> topicsAndPartitions = memberEntry.getValue().partitions();

            for (Map.Entry<Uuid, Set<Integer>> topicEntry : topicsAndPartitions.entrySet()) {
                Uuid topicId = topicEntry.getKey();
                Set<Integer> partitions = topicEntry.getValue();

                Map<Integer, String> partitionMap = invertedTargetAssignment.computeIfAbsent(topicId, k -> new HashMap<>());

                for (Integer partitionId : partitions) {
                    partitionMap.put(partitionId, memberId);
                }
            }
        }
        return invertedTargetAssignment;
    }

    /**
     * Generates a list of topic names for use in benchmarks.
     *
     * @param topicCount            The number of topic names to generate.
     * @return The list of topic names.
     */
    public static List<String> createTopicNames(int topicCount) {
        List<String> topicNames = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) {
            topicNames.add("topic-" + i);
        }
        return topicNames;
    }

    /**
     * Creates a subscription metadata map for the given topics.
     *
     * @param topicNames                The names of the topics.
     * @param partitionsPerTopic        The number of partitions per topic.
     * @return The subscription metadata map.
     */
    public static Map<String, TopicMetadata> createSubscriptionMetadata(
        List<String> topicNames,
        int partitionsPerTopic
    ) {
        Map<String, TopicMetadata> subscriptionMetadata = new HashMap<>();

        for (String topicName : topicNames) {
            Uuid topicId = Uuid.randomUuid();

            TopicMetadata metadata = new TopicMetadata(
                topicId,
                topicName,
                partitionsPerTopic
            );
            subscriptionMetadata.put(topicName, metadata);
        }

        return subscriptionMetadata;
    }

    /**
     * Creates a topic metadata map from the given subscription metadata.
     *
     * @param subscriptionMetadata  The subscription metadata.
     * @return The topic metadata map.
     */
    public static Map<Uuid, TopicMetadata> createTopicMetadata(
        Map<String, TopicMetadata> subscriptionMetadata
    ) {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>((int) (subscriptionMetadata.size() / 0.75f + 1));
        for (Map.Entry<String, TopicMetadata> entry : subscriptionMetadata.entrySet()) {
            topicMetadata.put(entry.getValue().id(), entry.getValue());
        }
        return topicMetadata;
    }

    /**
     * Creates a TopicsImage from the given subscription metadata.
     *
     * @param subscriptionMetadata  The subscription metadata.
     * @return A TopicsImage containing the topic ids, names and partition counts from the
     *         subscription metadata.
     */
    public static TopicsImage createTopicsImage(Map<String, TopicMetadata> subscriptionMetadata) {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);

        for (Map.Entry<String, TopicMetadata> entry : subscriptionMetadata.entrySet()) {
            TopicMetadata topicMetadata = entry.getValue();
            AssignorBenchmarkUtils.addTopic(
                delta,
                topicMetadata.id(),
                topicMetadata.name(),
                topicMetadata.numPartitions()
            );
        }

        return delta.apply(MetadataProvenance.EMPTY).topics();
    }

    /**
     * Creates a GroupSpec from the given ConsumerGroupMembers.
     *
     * @param members               The ConsumerGroupMembers.
     * @param subscriptionType      The group's subscription type.
     * @param topicResolver         The TopicResolver to use.
     * @return The new GroupSpec.
     */
    public static GroupSpec createGroupSpec(
        Map<String, ConsumerGroupMember> members,
        SubscriptionType subscriptionType,
        TopicIds.TopicResolver topicResolver
    ) {
        Map<String, MemberSubscriptionAndAssignmentImpl> memberSpecs = new HashMap<>();

        // Prepare the member spec for all members.
        for (Map.Entry<String, ConsumerGroupMember> memberEntry : members.entrySet()) {
            String memberId = memberEntry.getKey();
            ConsumerGroupMember member = memberEntry.getValue();

            memberSpecs.put(memberId, new MemberSubscriptionAndAssignmentImpl(
                Optional.ofNullable(member.rackId()),
                Optional.ofNullable(member.instanceId()),
                new TopicIds(member.subscribedTopicNames(), topicResolver),
                new Assignment(member.assignedPartitions())
            ));
        }

        return new GroupSpecImpl(
            memberSpecs,
            subscriptionType,
            Collections.emptyMap()
        );
    }

    /**
     * Creates a ConsumerGroupMembers map where all members have the same topic subscriptions.
     *
     * @param memberCount           The number of members in the group.
     * @param getMemberId           A function to map member indices to member ids.
     * @param getMemberRackId       A function to map member indices to rack ids.
     * @param topicNames            The topics to subscribe to.
     * @return The new ConsumerGroupMembers map.
     */
    public static Map<String, ConsumerGroupMember> createHomogeneousMembers(
        int memberCount,
        Function<Integer, String> getMemberId,
        Function<Integer, Optional<String>> getMemberRackId,
        List<String> topicNames
    ) {
        Map<String, ConsumerGroupMember> members = new HashMap<>();

        for (int i = 0; i < memberCount; i++) {
            String memberId = getMemberId.apply(i);
            Optional<String> rackId = getMemberRackId.apply(i);

            members.put(memberId, new ConsumerGroupMember.Builder("member" + i)
                .setRackId(rackId.orElse(null))
                .setSubscribedTopicNames(topicNames)
                .build()
            );
        }

        return members;
    }

    /**
     * Creates a ConsumerGroupMembers map where members have different topic subscriptions.
     *
     * Divides members and topics into a given number of buckets. Within each bucket, members are
     * subscribed to the same topics.
     *
     * @param memberCount           The number of members in the group.
     * @param bucketCount           The number of buckets.
     * @param getMemberId           A function to map member indices to member ids.
     * @param getMemberRackId       A function to map member indices to rack ids.
     * @param topicNames            The topics to subscribe to.
     * @return The new ConsumerGroupMembers map.
     */
    public static Map<String, ConsumerGroupMember> createHeterogeneousBucketedMembers(
        int memberCount,
        int bucketCount,
        Function<Integer, String> getMemberId,
        Function<Integer, Optional<String>> getMemberRackId,
        List<String> topicNames
    ) {
        Map<String, ConsumerGroupMember> members = new HashMap<>();

        // Adjust bucket count based on member count when member count < max bucket count.
        bucketCount = Math.min(bucketCount, memberCount);

        // Check minimum topics requirement
        if (topicNames.size() < bucketCount) {
            throw new IllegalArgumentException("At least " + bucketCount + " topics are recommended for effective bucketing.");
        }

        int bucketSizeTopics = (int) Math.ceil((double) topicNames.size() / bucketCount);
        int bucketSizeMembers = (int) Math.ceil((double) memberCount / bucketCount);

        // Define buckets for each member and assign topics from the same bucket
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            int memberStartIndex = bucket * bucketSizeMembers;
            int memberEndIndex = Math.min((bucket + 1) * bucketSizeMembers, memberCount);

            int topicStartIndex = bucket * bucketSizeTopics;
            int topicEndIndex = Math.min((bucket + 1) * bucketSizeTopics, topicNames.size());

            List<String> bucketTopicNames = topicNames.subList(topicStartIndex, topicEndIndex);

            // Assign topics to each member in the current bucket
            for (int i = memberStartIndex; i < memberEndIndex; i++) {
                String memberId = getMemberId.apply(i);
                Optional<String> rackId = getMemberRackId.apply(i);

                members.put(memberId, new ConsumerGroupMember.Builder("member" + i)
                    .setRackId(rackId.orElse(null))
                    .setSubscribedTopicNames(bucketTopicNames)
                    .build()
                );
            }
        }

        return members;
    }

    public static void addTopic(
        MetadataDelta delta,
        Uuid topicId,
        String topicName,
        int numPartitions
    ) {
        // For testing purposes, the following criteria are used:
        // - Number of replicas for each partition: 2
        // - Number of brokers available in the cluster: 4
        delta.replay(new TopicRecord().setTopicId(topicId).setName(topicName));
        for (int i = 0; i < numPartitions; i++) {
            delta.replay(new PartitionRecord()
                .setTopicId(topicId)
                .setPartitionId(i)
                .setReplicas(Arrays.asList(i % 4, (i + 1) % 4)));
        }
    }
}
