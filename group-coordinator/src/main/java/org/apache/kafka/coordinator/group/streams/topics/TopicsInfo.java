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
package org.apache.kafka.coordinator.group.streams.topics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Internal representation of the topics used by a subtopology.
 */
public class TopicsInfo {

    private Set<String> repartitionSinkTopics;
    private Set<String> sourceTopics;
    private Map<String, InternalTopicConfig> stateChangelogTopics;
    private Map<String, InternalTopicConfig> repartitionSourceTopics;

    public TopicsInfo() {
        this.repartitionSinkTopics = new HashSet<>();
        this.sourceTopics = new HashSet<>();
        this.stateChangelogTopics = new HashMap<>();
        this.repartitionSourceTopics = new HashMap<>();
    }

    public TopicsInfo(
        final Set<String> repartitionSinkTopics,
        final Set<String> sourceTopics,
        final Map<String, InternalTopicConfig> repartitionSourceTopics,
        final Map<String, InternalTopicConfig> stateChangelogTopics
    ) {
        this.repartitionSinkTopics = repartitionSinkTopics;
        this.sourceTopics = sourceTopics;
        this.stateChangelogTopics = stateChangelogTopics;
        this.repartitionSourceTopics = repartitionSourceTopics;
    }

    public Set<String> repartitionSinkTopics() {
        return repartitionSinkTopics;
    }

    public Set<String> sourceTopics() {
        return sourceTopics;
    }

    public Map<String, InternalTopicConfig> stateChangelogTopics() {
        return stateChangelogTopics;
    }

    public Map<String, InternalTopicConfig> repartitionSourceTopics() {
        return repartitionSourceTopics;
    }

    public TopicsInfo setRepartitionSinkTopics(final Set<String> repartitionSinkTopics) {
        this.repartitionSinkTopics = repartitionSinkTopics;
        return this;
    }

    public TopicsInfo setSourceTopics(final Set<String> sourceTopics) {
        this.sourceTopics = sourceTopics;
        return this;
    }

    public TopicsInfo setStateChangelogTopics(
        final Map<String, InternalTopicConfig> stateChangelogTopics) {
        this.stateChangelogTopics = stateChangelogTopics;
        return this;
    }

    public TopicsInfo setRepartitionSourceTopics(
        final Map<String, InternalTopicConfig> repartitionSourceTopics) {
        this.repartitionSourceTopics = repartitionSourceTopics;
        return this;
    }

    /**
     * Returns the config for any changelogs that must be prepared for this topic group, ie
     * excluding any source topics that are reused as a changelog
     */
    public Set<InternalTopicConfig> nonSourceChangelogTopics() {
        final Set<InternalTopicConfig> topicConfigs = new HashSet<>();
        for (final Map.Entry<String, InternalTopicConfig> changelogTopicEntry : stateChangelogTopics.entrySet()) {
            if (!sourceTopics.contains(changelogTopicEntry.getKey())) {
                topicConfigs.add(changelogTopicEntry.getValue());
            }
        }
        return topicConfigs;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TopicsInfo that = (TopicsInfo) o;
        return Objects.equals(repartitionSinkTopics, that.repartitionSinkTopics)
            && Objects.equals(sourceTopics, that.sourceTopics)
            && Objects.equals(stateChangelogTopics, that.stateChangelogTopics)
            && Objects.equals(repartitionSourceTopics, that.repartitionSourceTopics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(repartitionSinkTopics, sourceTopics,
            stateChangelogTopics, repartitionSourceTopics);
    }

    @Override
    public String toString() {
        return "TopicsInfo{" +
            "repartitionSinkTopics=" + repartitionSinkTopics +
            ", sourceTopics=" + sourceTopics +
            ", stateChangelogTopics=" + stateChangelogTopics +
            ", repartitionSourceTopics=" + repartitionSourceTopics +
            '}';
    }

}