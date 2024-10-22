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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Internal representation of a subtopology.
 * <p>
 * The subtopology is configured according to the number of partitions available in the source topics. It has regular expressions already
 * resolved and defined exactly the information that is being used by streams groups assignment reconciliation.
 * <p>
 * Configured subtopologies may be recreated every time the input topics used by the subtopology are modified.
 */
public class ConfiguredSubtopology {

    private Set<String> repartitionSinkTopics;
    private Set<String> sourceTopics;
    private Map<String, ConfiguredInternalTopic> stateChangelogTopics;
    private Map<String, ConfiguredInternalTopic> repartitionSourceTopics;

    public ConfiguredSubtopology() {
        this.repartitionSinkTopics = new HashSet<>();
        this.sourceTopics = new HashSet<>();
        this.stateChangelogTopics = new HashMap<>();
        this.repartitionSourceTopics = new HashMap<>();
    }

    public ConfiguredSubtopology(
        final Set<String> repartitionSinkTopics,
        final Set<String> sourceTopics,
        final Map<String, ConfiguredInternalTopic> repartitionSourceTopics,
        final Map<String, ConfiguredInternalTopic> stateChangelogTopics
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

    public Map<String, ConfiguredInternalTopic> stateChangelogTopics() {
        return stateChangelogTopics;
    }

    public Map<String, ConfiguredInternalTopic> repartitionSourceTopics() {
        return repartitionSourceTopics;
    }

    public ConfiguredSubtopology setRepartitionSinkTopics(final Set<String> repartitionSinkTopics) {
        this.repartitionSinkTopics = repartitionSinkTopics;
        return this;
    }

    public ConfiguredSubtopology setSourceTopics(final Set<String> sourceTopics) {
        this.sourceTopics = sourceTopics;
        return this;
    }

    public ConfiguredSubtopology setStateChangelogTopics(
        final Map<String, ConfiguredInternalTopic> stateChangelogTopics
    ) {
        this.stateChangelogTopics = stateChangelogTopics;
        return this;
    }

    public ConfiguredSubtopology setRepartitionSourceTopics(
        final Map<String, ConfiguredInternalTopic> repartitionSourceTopics
    ) {
        this.repartitionSourceTopics = repartitionSourceTopics;
        return this;
    }

    /**
     * Returns the config for any changelogs that must be prepared for this topic group, ie excluding any source topics that are reused as a
     * changelog
     */
    public Set<ConfiguredInternalTopic> nonSourceChangelogTopics() {
        final Set<ConfiguredInternalTopic> topicConfigs = new HashSet<>();
        for (final Map.Entry<String, ConfiguredInternalTopic> changelogTopicEntry : stateChangelogTopics.entrySet()) {
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
        final ConfiguredSubtopology that = (ConfiguredSubtopology) o;
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
        return "ConfiguredSubtopology{" +
            "repartitionSinkTopics=" + repartitionSinkTopics +
            ", sourceTopics=" + sourceTopics +
            ", stateChangelogTopics=" + stateChangelogTopics +
            ", repartitionSourceTopics=" + repartitionSourceTopics +
            '}';
    }

}