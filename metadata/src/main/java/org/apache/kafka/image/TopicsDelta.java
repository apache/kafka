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

package org.apache.kafka.image;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.ClearElrRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.immutable.ImmutableMap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Represents changes to the topics in the metadata image.
 */
public final class TopicsDelta {
    private final TopicsImage image;

    /**
     * A map from topic IDs to the topic deltas for each topic. Topics which have been
     * deleted will not appear in this map.
     */
    private final Map<Uuid, TopicDelta> changedTopics = new HashMap<>();

    /**
     * The IDs of topics that exist in the image but that have been deleted. Note that if
     * a topic does not exist in the image, it will also not exist in this set. Topics
     * that are created and then deleted within the same delta will leave no trace.
     */
    private final Set<Uuid> deletedTopicIds = new HashSet<>();

    private final Map<String, Uuid> createdTopics = new HashMap<>();

    public TopicsDelta(TopicsImage image) {
        this.image = image;
    }

    public TopicsImage image() {
        return image;
    }

    public Map<Uuid, TopicDelta> changedTopics() {
        return changedTopics;
    }

    public void replay(TopicRecord record) {
        TopicDelta delta = new TopicDelta(
            new TopicImage(record.name(), record.topicId(), Collections.emptyMap()));
        changedTopics.put(record.topicId(), delta);
        createdTopics.put(record.name(), record.topicId());
    }

    TopicDelta getOrCreateTopicDelta(Uuid id) {
        TopicDelta topicDelta = changedTopics.get(id);
        if (topicDelta == null) {
            topicDelta = new TopicDelta(image.getTopic(id));
            changedTopics.put(id, topicDelta);
        }
        return topicDelta;
    }

    public void replay(PartitionRecord record) {
        TopicDelta topicDelta = getOrCreateTopicDelta(record.topicId());
        topicDelta.replay(record);
    }

    public void replay(PartitionChangeRecord record) {
        TopicDelta topicDelta = getOrCreateTopicDelta(record.topicId());
        topicDelta.replay(record);
    }

    public void replay(ClearElrRecord record) {
        if (!record.topicName().isEmpty()) {
            Uuid topicId;
            if (image.getTopic(record.topicName()) != null) {
                topicId = image.getTopic(record.topicName()).id();
            } else {
                topicId = createdTopics.get(record.topicName());
            }
            if (topicId == null) {
                throw new RuntimeException("Unable to clear elr for topic with name " +
                    record.topicName() + ": no such topic found.");
            }
            TopicDelta topicDelta = getOrCreateTopicDelta(topicId);
            topicDelta.replay(record);
        } else {
            // Update all the existing topics
            image.topicsById().forEach((topicId, image) -> {
                TopicDelta topicDelta = getOrCreateTopicDelta(topicId);
                topicDelta.replay(record);
            });
        }
    }

    public String replay(RemoveTopicRecord record) {
        TopicDelta topicDelta = changedTopics.remove(record.topicId());
        String topicName;
        if (topicDelta != null) {
            topicName = topicDelta.image().name();
            if (image.topicsById().containsKey(record.topicId())) {
                deletedTopicIds.add(record.topicId());
            }
        } else {
            TopicImage topicImage = image.getTopic(record.topicId());
            if (topicImage == null) {
                throw new RuntimeException("Unable to delete topic with id " +
                    record.topicId() + ": no such topic found.");
            }
            topicName = topicImage.name();
            deletedTopicIds.add(record.topicId());
        }
        return topicName;
    }

    public void finishSnapshot() {
        for (Uuid topicId : image.topicsById().keySet()) {
            if (!changedTopics.containsKey(topicId)) {
                deletedTopicIds.add(topicId);
            }
        }
    }

    public void handleMetadataVersionChange(MetadataVersion newVersion) {
        // no-op
    }

    public TopicsImage apply() {
        ImmutableMap<Uuid, TopicImage> newTopicsById = image.topicsById();
        ImmutableMap<String, TopicImage> newTopicsByName = image.topicsByName();
        // apply all the deletes
        for (Uuid topicId: deletedTopicIds) {
            // it was deleted, so we have to remove it from the maps
            TopicImage originalTopicToBeDeleted = image.topicsById().get(topicId);
            if (originalTopicToBeDeleted == null) {
                throw new IllegalStateException("Missing topic id " + topicId);
            } else {
                newTopicsById = newTopicsById.removed(topicId);
                newTopicsByName = newTopicsByName.removed(originalTopicToBeDeleted.name());
            }
        }
        // apply all the updates/additions
        for (Map.Entry<Uuid, TopicDelta> entry: changedTopics.entrySet()) {
            Uuid topicId = entry.getKey();
            TopicImage newTopicToBeAddedOrUpdated = entry.getValue().apply();
            // put new information into the maps
            String topicName = newTopicToBeAddedOrUpdated.name();
            newTopicsById = newTopicsById.updated(topicId, newTopicToBeAddedOrUpdated);
            newTopicsByName = newTopicsByName.updated(topicName, newTopicToBeAddedOrUpdated);
        }
        return new TopicsImage(newTopicsById, newTopicsByName);
    }

    public TopicDelta changedTopic(Uuid topicId) {
        return changedTopics.get(topicId);
    }

    /**
     * Returns true if the topic with the given name was deleted. Note: this will return
     * true even if a new topic with the same name was subsequently created.
     */
    public boolean topicWasDeleted(String topicName) {
        TopicImage topicImage = image.getTopic(topicName);
        if (topicImage == null) {
            return false;
        }
        return deletedTopicIds.contains(topicImage.id());
    }

    public Set<Uuid> deletedTopicIds() {
        return deletedTopicIds;
    }

    public Collection<Uuid> createdTopicIds() {
        return createdTopics.values();
    }

    /**
     * Find the topic partitions that have change based on the replica given.
     * <p>
     * The changes identified are:
     * <ul>
     *   <li>deletes: partitions for which the broker is not a replica anymore</li>
     *   <li>electedLeaders: partitions for which the broker is now a leader (leader epoch bump on the leader)</li>
     *   <li>leaders: partitions for which the isr or replicas change if the broker is a leader (partition epoch bump on the leader)</li>
     *   <li>followers: partitions for which the broker is now a follower or follower with isr or replica updates (partition epoch bump on follower)</li>
     *   <li>topicIds: a map of topic names to topic IDs in leaders and followers changes</li>
     *   <li>directoryIds: partitions for which directory id changes or newly added to the broker</li>
     * </ul>
     * <p>
     * Leader epoch bumps are a strict subset of all partition epoch bumps, so all partitions in electedLeaders will be in leaders.
     *
     * @param brokerId the broker id
     * @return the LocalReplicaChanges that cover changes in the broker
     */
    public LocalReplicaChanges localChanges(int brokerId) {
        Set<TopicPartition> deletes = new HashSet<>();
        Map<TopicPartition, LocalReplicaChanges.PartitionInfo> electedLeaders = new HashMap<>();
        Map<TopicPartition, LocalReplicaChanges.PartitionInfo> leaders = new HashMap<>();
        Map<TopicPartition, LocalReplicaChanges.PartitionInfo> followers = new HashMap<>();
        Map<String, Uuid> topicIds = new HashMap<>();
        Map<TopicIdPartition, Uuid> directoryIds = new HashMap<>();

        for (TopicDelta delta : changedTopics.values()) {
            LocalReplicaChanges changes = delta.localChanges(brokerId);

            deletes.addAll(changes.deletes());
            electedLeaders.putAll(changes.electedLeaders());
            leaders.putAll(changes.leaders());
            followers.putAll(changes.followers());
            topicIds.putAll(changes.topicIds());
            directoryIds.putAll(changes.directoryIds());
        }

        // Add all of the removed topic partitions to the set of locally removed partitions
        deletedTopicIds().forEach(topicId -> {
            TopicImage topicImage = image().getTopic(topicId);
            topicImage.partitions().forEach((partitionId, prevPartition) -> {
                if (Replicas.contains(prevPartition.replicas, brokerId)) {
                    deletes.add(new TopicPartition(topicImage.name(), partitionId));
                }
            });
        });

        return new LocalReplicaChanges(deletes, electedLeaders, leaders, followers, topicIds, directoryIds);
    }

    @Override
    public String toString() {
        return "TopicsDelta(" +
            "changedTopics=" + changedTopics +
            ", deletedTopicIds=" + deletedTopicIds +
            ", createdTopics=" + createdTopics +
            ')';
    }
}
