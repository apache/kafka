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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
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

    public TopicsImage apply() {
        Map<Uuid, TopicImage> newTopicsById = new HashMap<>(image.topicsById().size());
        Map<String, TopicImage> newTopicsByName = new HashMap<>(image.topicsByName().size());
        for (Entry<Uuid, TopicImage> entry : image.topicsById().entrySet()) {
            Uuid id = entry.getKey();
            TopicImage prevTopicImage = entry.getValue();
            TopicDelta delta = changedTopics.get(id);
            if (delta == null) {
                if (!deletedTopicIds.contains(id)) {
                    newTopicsById.put(id, prevTopicImage);
                    newTopicsByName.put(prevTopicImage.name(), prevTopicImage);
                }
            } else {
                TopicImage newTopicImage = delta.apply();
                newTopicsById.put(id, newTopicImage);
                newTopicsByName.put(delta.name(), newTopicImage);
            }
        }
        for (Entry<Uuid, TopicDelta> entry : changedTopics.entrySet()) {
            if (!newTopicsById.containsKey(entry.getKey())) {
                TopicImage newTopicImage = entry.getValue().apply();
                newTopicsById.put(newTopicImage.id(), newTopicImage);
                newTopicsByName.put(newTopicImage.name(), newTopicImage);
            }
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
}
