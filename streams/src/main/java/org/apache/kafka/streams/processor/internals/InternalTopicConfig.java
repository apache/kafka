/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.utils.Utils;

import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * InternalTopicConfig captures the properties required for configuring
 * the internal topics we create for change-logs and repartitioning etc.
 */
public class InternalTopicConfig {
    private static final long FOREVER = -1;
    private static final Set VALID_CLEANUP_POLICIES = Utils.mkSet(InternalTopicManager.COMPACT_AND_DELETE,
                                                                  InternalTopicManager.COMPACT,
                                                                  InternalTopicManager.DELETE);
    private final String name;
    private String cleanupPolicy;
    private long retentionMs = FOREVER;

    public InternalTopicConfig(final String name) {
        this(name, null);
    }

    public InternalTopicConfig(final String name, final String cleanupPolicy) {
        Objects.requireNonNull(name, "name can't be null");
        this.name = name;
        setCleanupPolicy(cleanupPolicy);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final InternalTopicConfig that = (InternalTopicConfig) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "InternalTopicConfig{" +
                "name='" + name + '\'' +
                ", cleanupPolicy='" + cleanupPolicy + '\'' +
                ", retentionMs=" + retentionMs +
                '}';
    }

    boolean isCompacted() {
        return InternalTopicManager.COMPACT_AND_DELETE.equals(cleanupPolicy) || InternalTopicManager.COMPACT.equals(cleanupPolicy);
    }

    /**
     * Get the configured properties for this topic. If rententionMs is set then
     * we use the retentionMultiplier to work out the desired retention when cleanup.policy=compact_and_delete
     * @param additionalRetentionMs - added to retention to allow for clock drift etc
     * @return Properties to be used when creating the topic
     */
    public Properties toProperties(final long additionalRetentionMs) {
        final Properties result = new Properties();
        if (cleanupPolicy != null) {
            result.put(InternalTopicManager.CLEANUP_POLICY_PROP, cleanupPolicy);
        }
        if (retentionMs != FOREVER && InternalTopicManager.COMPACT_AND_DELETE.equals(cleanupPolicy)) {
            result.put(InternalTopicManager.RETENTION_MS, String.valueOf(retentionMs + additionalRetentionMs));
        }
        return result;
    }

    public String name() {
        return name;
    }

    public void setRetentionMs(final long retentionMs) {
        this.retentionMs = retentionMs;
    }

    public void setCleanupPolicy(final String cleanupPolicy) {
        if (cleanupPolicy != null && !VALID_CLEANUP_POLICIES.contains(cleanupPolicy)) {
            throw new IllegalArgumentException("cleanupPolicy=" + cleanupPolicy + " is not valid. Must be in: " + VALID_CLEANUP_POLICIES);
        }
        this.cleanupPolicy = cleanupPolicy;
    }
}
