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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.BrokerRegistration;

import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Interface common for all groups.
 */
public interface Group {
    enum GroupType {
        CONSUMER("consumer"),
        CLASSIC("classic"),
        SHARE("share"),
        STREAMS("streams"),
        UNKNOWN("unknown");

        private final String name;

        GroupType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        private static final Map<String, GroupType> NAME_TO_ENUM = Arrays.stream(values())
            .collect(Collectors.toMap(type -> type.name.toLowerCase(Locale.ROOT), Function.identity()));

        /**
         * Parse a string into the corresponding {@code GroupType} enum value, in a case-insensitive manner.
         *
         * @return The {{@link GroupType}} according to the string passed. Unknown group type is returned if
         * the string doesn't correspond to a valid group type.
         */
        public static GroupType parse(String name) {
            if (name == null) {
                return UNKNOWN;
            }
            GroupType type = NAME_TO_ENUM.get(name.toLowerCase(Locale.ROOT));

            return type == null ? UNKNOWN : type;
        }
        
        static String[] documentValidValues() {
            return Arrays.stream(GroupType.values())
                .filter(type -> type != UNKNOWN)
                .map(GroupType::toString)
                .toArray(String[]::new);
        }
    }

    /**
     * @return The {{@link GroupType}}.
     */
    GroupType type();

    /**
     * @return The {{@link GroupType}}'s String representation.
     */
    String stateAsString();

    /**
     * @return The {{@link GroupType}}'s String representation based on the committed offset.
     */
    String stateAsString(long committedOffset);

    /**
     * @return the group formatted as a list group response based on the committed offset.
     */
    ListGroupsResponseData.ListedGroup asListedGroup(long committedOffset);

    /**
     * @return The group id.
     */
    String groupId();

    /**
     * Validates the OffsetCommit request.
     *
     * @param memberId                  The member id.
     * @param groupInstanceId           The group instance id.
     * @param generationIdOrMemberEpoch The generation id for genetic groups or the member epoch
     *                                  for consumer groups.
     * @param isTransactional           Whether the offset commit is transactional or not.
     * @param apiVersion                The api version.
     */
    void validateOffsetCommit(
        String memberId,
        String groupInstanceId,
        int generationIdOrMemberEpoch,
        boolean isTransactional,
        int apiVersion

    ) throws KafkaException;

    /**
     * Validates the OffsetFetch request.
     *
     * @param memberId              The member id for consumer groups.
     * @param memberEpoch           The member epoch for consumer groups.
     * @param lastCommittedOffset   The last committed offsets in the timeline.
     */
    void validateOffsetFetch(
        String memberId,
        int memberEpoch,
        long lastCommittedOffset
    ) throws KafkaException;

    /**
     * Validates the OffsetDelete request.
     */
    void validateOffsetDelete() throws KafkaException;

    /**
     * Validates the DeleteGroups request.
     */
    void validateDeleteGroup() throws KafkaException;

    /**
     * Returns true if the group is actively subscribed to the topic.
     *
     * @param topic  The topic name.
     *
     * @return Whether the group is subscribed to the topic.
     */
    boolean isSubscribedToTopic(String topic);

    /**
     * Populates the list of records with tombstone(s) for deleting the group.
     *
     * @param records The list of records.
     */
    void createGroupTombstoneRecords(List<CoordinatorRecord> records);

    /**
     * @return Whether the group is in Empty state.
     */
    boolean isEmpty();

    /**
     * See {@link OffsetExpirationCondition}
     *
     * @return The offset expiration condition for the group or Empty if no such condition exists.
     */
    Optional<OffsetExpirationCondition> offsetExpirationCondition();

    /**
     * Returns true if the statesFilter contains the current state with given committedOffset.
     *
     * @param statesFilter The states to filter, which must be lowercase.
     * @return true if the state includes, false otherwise.
     */
    boolean isInStates(Set<String> statesFilter, long committedOffset);

    /**
     * Returns true if the member exists.
     *
     * @param memberId The member id.
     *
     * @return A boolean indicating whether the member exists or not.
     */
    boolean hasMember(String memberId);

    /**
     * Returns number of members in the group.
     *
     * @return The number of members.
     */
    int numMembers();

    /**
     * Requests a metadata refresh.
     */
    void requestMetadataRefresh();

    /**
     * Returns whether this group should be expired or not.
     *
     * @return whether the group should be expired.
     */
    default boolean shouldExpire() {
        return true;
    }

    /**
     * The magic byte used to identify the version of topic hash function.
     */
    byte TOPIC_HASH_MAGIC_BYTE = 0x00;
    XXHash64 LZ4_HASH_INSTANCE = XXHashFactory.fastestInstance().hash64();

    /**
     * Computes the hash of the topics in a group.
     *
     * @param topicHashes The map of topic hashes. Key is topic name and value is the topic hash.
     * @return The hash of the group.
     */
    static long computeGroupHash(Map<String, Long> topicHashes) {
        // Convert long to byte array. This is taken from guava LongHashCode#asBytes.
        // https://github.com/google/guava/blob/bdf2a9d05342fca852645278d474082905e09d94/guava/src/com/google/common/hash/HashCode.java#L187-L199
        LongFunction<byte[]> longToBytes = (long value) -> new byte[] {
            (byte) value,
            (byte) (value >> 8),
            (byte) (value >> 16),
            (byte) (value >> 24),
            (byte) (value >> 32),
            (byte) (value >> 40),
            (byte) (value >> 48),
            (byte) (value >> 56)
        };

        // Combine the sorted topic hashes.
        byte[] resultBytes = new byte[8];
        topicHashes.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey()) // sort by topic name
            .map(Map.Entry::getValue)
            .map(longToBytes::apply)
            .forEach(nextBytes -> {
                // Combine ordered hashes. This is taken from guava Hashing#combineOrdered.
                // https://github.com/google/guava/blob/bdf2a9d05342fca852645278d474082905e09d94/guava/src/com/google/common/hash/Hashing.java#L689-L712
                for (int i = 0; i < nextBytes.length; i++) {
                    resultBytes[i] = (byte) (resultBytes[i] * 37 ^ nextBytes[i]);
                }
            });

        // Convert the byte array to long. This is taken from guava BytesHashCode#asLong.
        // https://github.com/google/guava/blob/bdf2a9d05342fca852645278d474082905e09d94/guava/src/com/google/common/hash/HashCode.java#L279-L295
        long retVal = (resultBytes[0] & 0xFF);
        for (int i = 1; i < resultBytes.length; i++) {
            retVal |= (resultBytes[i] & 0xFFL) << (i * 8);
        }
        return retVal;
    }

    /**
     * Computes the hash of the topic id, name, number of partitions, and partition racks by Murmur3.
     *
     * @param topicImage   The topic image.
     * @param clusterImage The cluster image.
     * @return The hash of the topic.
     */
    static long computeTopicHash(TopicImage topicImage, ClusterImage clusterImage) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeByte(TOPIC_HASH_MAGIC_BYTE); // magic byte
            dos.writeLong(topicImage.id().hashCode()); // topic ID
            dos.writeUTF(topicImage.name()); // topic name
            dos.writeInt(topicImage.partitions().size()); // number of partitions
            for (int i = 0; i < topicImage.partitions().size(); i++) {
                dos.writeInt(i); // partition id
                List<String> sortedRacksList = Arrays.stream(topicImage.partitions().get(i).replicas)
                    .mapToObj(clusterImage::broker)
                    .filter(Objects::nonNull)
                    .map(BrokerRegistration::rack)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .sorted()
                    .toList();

                String racks = IntStream.range(0, sortedRacksList.size())
                    .mapToObj(idx -> idx + ":" + sortedRacksList.get(idx)) // Format: "index:value"
                    .collect(Collectors.joining(",")); // Separator between "index:value" pairs
                dos.writeUTF(racks); // sorted racks
            }
            dos.flush();
            byte[] topicBytes = baos.toByteArray();
            return LZ4_HASH_INSTANCE.hash(topicBytes, 0, topicBytes.length, 0);
        }
    }
}
