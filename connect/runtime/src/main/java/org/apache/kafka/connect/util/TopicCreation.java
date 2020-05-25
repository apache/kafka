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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_GROUP;
import static org.apache.kafka.connect.util.TopicAdmin.NewTopicCreationGroup;

/**
 * Utility to be used by worker source tasks in order to create topics, if topic creation is enabled for source connectors
 * at the worker and the connector configurations.
 */
public class TopicCreation {
    private static final Logger log = LoggerFactory.getLogger(TopicCreation.class);
    private static final TopicCreation EMPTY =
            new TopicCreation(false, null, Collections.emptyMap(), Collections.emptySet());

    private final boolean isTopicCreationEnabled;
    private final NewTopicCreationGroup defaultTopicGroup;
    private final Map<String, NewTopicCreationGroup> topicGroups;
    private final Set<String> topicCache;

    protected TopicCreation(boolean isTopicCreationEnabled,
                            NewTopicCreationGroup defaultTopicGroup,
                            Map<String, NewTopicCreationGroup> topicGroups,
                            Set<String> topicCache) {
        this.isTopicCreationEnabled = isTopicCreationEnabled;
        this.defaultTopicGroup = defaultTopicGroup;
        this.topicGroups = topicGroups;
        this.topicCache = topicCache;
    }

    public static TopicCreation newTopicCreation(WorkerConfig workerConfig,
            Map<String, NewTopicCreationGroup> topicGroups) {
        if (!workerConfig.topicCreationEnable() || topicGroups == null) {
            return EMPTY;
        }
        Map<String, NewTopicCreationGroup> groups = new LinkedHashMap<>(topicGroups);
        groups.remove(DEFAULT_TOPIC_CREATION_GROUP);
        return new TopicCreation(true, topicGroups.get(DEFAULT_TOPIC_CREATION_GROUP), groups, new HashSet<>());
    }

    public static TopicCreation empty() {
        return EMPTY;
    }

    public boolean isTopicCreationEnabled() {
        return isTopicCreationEnabled;
    }

    public NewTopicCreationGroup defaultTopicGroup() {
        return defaultTopicGroup;
    }

    public Map<String, NewTopicCreationGroup> topicGroups() {
        return topicGroups;
    }

    public Set<String> topicCache() {
        return topicCache;
    }
}
