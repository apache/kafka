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

import org.apache.kafka.common.internals.Topic;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * InternalTopicConfig captures the properties required for configuring
 * the internal topics we create for change-logs and repartitioning etc.
 */
public class InternalTopicConfig {
    public enum CleanupPolicy { compact, delete }

    private final String name;
    private final Map<String, String> logConfig;
    private final Set<CleanupPolicy> cleanupPolicies;

    private Long retentionMs;

    public InternalTopicConfig(final String name, final Set<CleanupPolicy> defaultCleanupPolicies, final Map<String, String> logConfig) {
        Objects.requireNonNull(name, "name can't be null");
        Topic.validate(name);

        if (defaultCleanupPolicies.isEmpty()) {
            throw new IllegalArgumentException("Must provide at least one cleanup policy");
        }
        this.name = name;
        this.cleanupPolicies = defaultCleanupPolicies;
        this.logConfig = logConfig;
    }

    /* for test use only */
    boolean isCompacted() {
        return cleanupPolicies.contains(CleanupPolicy.compact);
    }

    private boolean isCompactDelete() {
        return cleanupPolicies.contains(CleanupPolicy.compact) && cleanupPolicies.contains(CleanupPolicy.delete);
    }

    /**
     * Get the configured properties for this topic. If rententionMs is set then
     * we add additionalRetentionMs to work out the desired retention when cleanup.policy=compact,delete
     *
     * @param additionalRetentionMs - added to retention to allow for clock drift etc
     * @return Properties to be used when creating the topic
     */
    public Properties toProperties(final long additionalRetentionMs) {
        final Properties result = new Properties();
        for (Map.Entry<String, String> configEntry : logConfig.entrySet()) {
            result.put(configEntry.getKey(), configEntry.getValue());
        }
        if (retentionMs != null && isCompactDelete()) {
            result.put(InternalTopicManager.RETENTION_MS, String.valueOf(retentionMs + additionalRetentionMs));
        }

        if (!logConfig.containsKey(InternalTopicManager.CLEANUP_POLICY_PROP)) {
            final StringBuilder builder = new StringBuilder();
            for (CleanupPolicy cleanupPolicy : cleanupPolicies) {
                builder.append(cleanupPolicy.name()).append(",");
            }
            builder.deleteCharAt(builder.length() - 1);

            result.put(InternalTopicManager.CLEANUP_POLICY_PROP, builder.toString());
        }


        return result;
    }

    public String name() {
        return name;
    }

    public void setRetentionMs(final long retentionMs) {
        if (!logConfig.containsKey(InternalTopicManager.RETENTION_MS)) {
            this.retentionMs = retentionMs;
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final InternalTopicConfig that = (InternalTopicConfig) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(logConfig, that.logConfig) &&
                Objects.equals(retentionMs, that.retentionMs) &&
                Objects.equals(cleanupPolicies, that.cleanupPolicies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, logConfig, retentionMs, cleanupPolicies);
    }

    @Override
    public String toString() {
        return "InternalTopicConfig(" +
                "name=" + name +
                ", logConfig=" + logConfig +
                ", cleanupPolicies=" + cleanupPolicies +
                ", retentionMs=" + retentionMs +
                ")";
    }
}
