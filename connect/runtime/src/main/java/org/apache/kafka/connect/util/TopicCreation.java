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
package org.apache.kafka.connect.util;

import org.apache.kafka.connect.runtime.WorkerConfig;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_GROUP;

/**
 * Utility to be used by worker source tasks in order to create topics, if topic creation is
 * enabled for source connectors at the worker and the connector configurations.
 */
public class TopicCreation {
    private static final TopicCreation EMPTY =
            new TopicCreation(false, null, Collections.emptyMap(), Collections.emptySet());

    private final boolean isTopicCreationEnabled;
    private final TopicCreationGroup defaultTopicGroup;
    private final Map<String, TopicCreationGroup> topicGroups;
    private final Set<String> topicCache;

    protected TopicCreation(boolean isTopicCreationEnabled,
                            TopicCreationGroup defaultTopicGroup,
                            Map<String, TopicCreationGroup> topicGroups,
                            Set<String> topicCache) {
        this.isTopicCreationEnabled = isTopicCreationEnabled;
        this.defaultTopicGroup = defaultTopicGroup;
        this.topicGroups = topicGroups;
        this.topicCache = topicCache;
    }

    public static TopicCreation newTopicCreation(WorkerConfig workerConfig,
            Map<String, TopicCreationGroup> topicGroups) {
        if (!workerConfig.topicCreationEnable() || topicGroups == null) {
            return EMPTY;
        }
        Map<String, TopicCreationGroup> groups = new LinkedHashMap<>(topicGroups);
        groups.remove(DEFAULT_TOPIC_CREATION_GROUP);
        return new TopicCreation(true, topicGroups.get(DEFAULT_TOPIC_CREATION_GROUP), groups, new HashSet<>());
    }

    /**
     * Return an instance of this utility that represents what the state of the internal data
     * structures should be when topic creation is disabled.
     *
     * @return the utility when topic creation is disabled
     */
    public static TopicCreation empty() {
        return EMPTY;
    }

    /**
     * Check whether topic creation is enabled for this utility instance. This state is set at
     * instantiation time and remains unchanged for the lifetime of every {@link TopicCreation}
     * object.
     *
     * @return true if topic creation is enabled; false otherwise
     */
    public boolean isTopicCreationEnabled() {
        return isTopicCreationEnabled;
    }

    /**
     * Check whether topic creation may be required for a specific topic name.
     *
     * @return true if topic creation is enabled and the topic name is not in the topic cache;
     * false otherwise
     */
    public boolean isTopicCreationRequired(String topic) {
        return isTopicCreationEnabled && !topicCache.contains(topic);
    }

    /**
     * Return the default topic creation group. This group is always defined when topic creation is
     * enabled but is {@code null} if topic creation is disabled.
     *
     * @return the default topic creation group if topic creation is enabled; {@code null} otherwise
     */
    public TopicCreationGroup defaultTopicGroup() {
        return defaultTopicGroup;
    }

    /**
     * Return the topic creation groups defined for a source connector as a map of topic creation
     * group name to topic creation group instance. This map maintains all the optionally defined
     * groups besides the default group which is defined for any connector when topic creation is
     * enabled.
     *
     * @return the map of all the topic creation groups besides the default group; may be empty
     * but not {@code null}
     */
    public Map<String, TopicCreationGroup> topicGroups() {
        return topicGroups;
    }

    /**
     * Inform this utility instance that a topic has been created and its creation will no
     * longer be required. After this method is called for a given {@code topic},
     * any subsequent calls to {@link #isTopicCreationRequired} will return {@code false} for the
     * same topic.
     *
     * @param topic the topic name to mark as created
     */
    public void addTopic(String topic) {
        if (isTopicCreationEnabled) {
            topicCache.add(topic);
        }
    }

    /**
     * Get the first topic creation group that is configured to match the given {@code topic}
     * name. If topic creation is enabled, any topic should match at least the default topic
     * creation group.
     *
     * @param topic the topic name to match against group configurations
     *
     * @return the first group that matches the given topic
     */
    public TopicCreationGroup findFirstGroup(String topic) {
        return topicGroups.values().stream()
                .filter(group -> group.matches(topic))
                .findFirst()
                .orElse(defaultTopicGroup);
    }
}
