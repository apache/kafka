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
 * UnwindowedUnversionedChangelogTopicConfig captures the properties required for configuring
 * the un-windowed, un-versioned store changelog topics.
 */
public class UnwindowedUnversionedChangelogTopicConfig extends InternalTopicConfig {
    private static final Map<String, String> UNWINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES;
    static {
        final Map<String, String> tempTopicDefaultOverrides = new HashMap<>(INTERNAL_TOPIC_DEFAULT_OVERRIDES);
        tempTopicDefaultOverrides.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        UNWINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempTopicDefaultOverrides);
    }

    UnwindowedUnversionedChangelogTopicConfig(final String name, final Map<String, String> topicConfigs) {
        super(name, topicConfigs);
    }

    /**
     * Get the configured properties for this topic. If retentionMs is set then
     * we add additionalRetentionMs to work out the desired retention when cleanup.policy=compact,delete
     *
     * @param additionalRetentionMs - added to retention to allow for clock drift etc
     * @return Properties to be used when creating the topic
     */
    @Override
    public Map<String, String> properties(final Map<String, String> defaultProperties, final long additionalRetentionMs) {
        // internal topic config overridden rule: library overrides < global config overrides < per-topic config overrides
        final Map<String, String> topicConfig = new HashMap<>(UNWINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES);

        topicConfig.putAll(defaultProperties);

        topicConfig.putAll(topicConfigs);

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
        final UnwindowedUnversionedChangelogTopicConfig that = (UnwindowedUnversionedChangelogTopicConfig) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(topicConfigs, that.topicConfigs) &&
               Objects.equals(enforceNumberOfPartitions, that.enforceNumberOfPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, topicConfigs, enforceNumberOfPartitions);
    }

    @Override
    public String toString() {
        return "UnwindowedUnversionedChangelogTopicConfig(" +
                "name=" + name +
                ", topicConfigs=" + topicConfigs +
                ", enforceNumberOfPartitions=" + enforceNumberOfPartitions +
                ")";
    }
}
