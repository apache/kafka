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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.config.TopicConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * VersionedChangelogTopicConfig captures the properties required for configuring
 * the versioned store changelog topics.
 */
public class VersionedChangelogTopicConfig extends InternalTopicConfig {
    private static final Map<String, String> VERSIONED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES;
    static {
        final Map<String, String> tempTopicDefaultOverrides = new HashMap<>(INTERNAL_TOPIC_DEFAULT_OVERRIDES);
        tempTopicDefaultOverrides.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        VERSIONED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempTopicDefaultOverrides);
    }
    private static final long VERSIONED_STORE_CHANGE_LOG_ADDITIONAL_COMPACTION_LAG_MS = 24 * 60 * 60 * 1000L;

    private final long minCompactionLagMs;

    VersionedChangelogTopicConfig(final String name,
                                  final Map<String, String> topicConfigs,
                                  final long minCompactionLagMs) {
        super(name, topicConfigs);
        this.minCompactionLagMs = minCompactionLagMs;
    }

    /**
     * Get the configured properties for this topic. If no {@code minCompactionLagMs} is
     * provided from the topic configs, then we add
     * {@code VERSIONED_STORE_CHANGE_LOG_ADDITIONAL_COMPACTION_LAG_MS} to work out the
     * desired min compaction lag for cleanup.policy=compact
     *
     * @param windowStoreAdditionalRetentionMs added to retention for window store changelog
     *                                         topics to allow for clock drift etc. We do not reuse
     *                                         this value for versioned store changelog topic
     *                                         compaction lag as the user-facing config name is
     *                                         window store specific.
     * @return Properties to be used when creating the topic
     */
    @Override
    public Map<String, String> getProperties(final Map<String, String> defaultProperties, final long windowStoreAdditionalRetentionMs) {
        // internal topic config override rule: library overrides < global config overrides < per-topic config overrides
        final Map<String, String> topicConfig = new HashMap<>(VERSIONED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES);

        topicConfig.putAll(defaultProperties);

        topicConfig.putAll(topicConfigs);

        if (!topicConfigs.containsKey(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG)) {
            long compactionLagValue;
            try {
                compactionLagValue = Math.addExact(minCompactionLagMs, VERSIONED_STORE_CHANGE_LOG_ADDITIONAL_COMPACTION_LAG_MS);
            } catch (final ArithmeticException swallow) {
                compactionLagValue = Long.MAX_VALUE;
            }
            topicConfig.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, String.valueOf(compactionLagValue));
        }

        return topicConfig;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final VersionedChangelogTopicConfig that = (VersionedChangelogTopicConfig) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(topicConfigs, that.topicConfigs) &&
            Objects.equals(minCompactionLagMs, that.minCompactionLagMs) &&
            Objects.equals(enforceNumberOfPartitions, that.enforceNumberOfPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, topicConfigs, minCompactionLagMs, enforceNumberOfPartitions);
    }

    @Override
    public String toString() {
        return "VersionedChangelogTopicConfig(" +
            "name=" + name +
            ", topicConfigs=" + topicConfigs +
            ", minCompactionLagMs=" + minCompactionLagMs +
            ", enforceNumberOfPartitions=" + enforceNumberOfPartitions +
            ")";
    }
}