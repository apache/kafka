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

package org.apache.kafka.controller;

import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;


class ElectionStrategizer {
    private static final Logger log = LoggerFactory.getLogger(ElectionStrategizer.class);

    private final int nodeId;
    private Boolean nodeUncleanConfig = null;
    private Boolean clusterUncleanConfig = null;
    private Function<String, String> topicUncleanConfigAccessor = __ -> "false";
    private Map<String, String> topicUncleanOverrides = new HashMap<>();

    ElectionStrategizer(int nodeId) {
        this.nodeId = nodeId;
    }

    ElectionStrategizer setNodeUncleanConfig(String nodeUncleanConfig) {
        this.nodeUncleanConfig = parseBoolean("node", nodeUncleanConfig);
        return this;
    }

    ElectionStrategizer setClusterUncleanConfig(String clusterUncleanConfig) {
        this.clusterUncleanConfig = parseBoolean("cluster", clusterUncleanConfig);
        return this;
    }

    ElectionStrategizer setTopicUncleanConfigAccessor(
            Function<String, String> topicUncleanConfigAccessor) {
        this.topicUncleanConfigAccessor = topicUncleanConfigAccessor;
        return this;
    }

    ElectionStrategizer setTopicUncleanOverride(String topicName, String value) {
        this.topicUncleanOverrides.put(topicName, value);
        return this;
    }

    boolean shouldBeUnclean(String topicName) {
        Boolean topicConfig = (topicUncleanOverrides.containsKey(topicName)) ?
            parseBoolean("topic", topicUncleanOverrides.get(topicName)) :
            parseBoolean("topic", topicUncleanConfigAccessor.apply(topicName));
        if (topicConfig != null) return topicConfig.booleanValue();
        if (nodeUncleanConfig != null) return nodeUncleanConfig.booleanValue();
        if (clusterUncleanConfig != null) return clusterUncleanConfig.booleanValue();
        return false;
    }

    // VisibleForTesting
    Boolean parseBoolean(String what, String value) {
        if (value == null) return null;
        if (value.equalsIgnoreCase("true")) return true;
        if (value.equalsIgnoreCase("false")) return false;
        if (value.trim().isEmpty()) return null;
        log.warn("Invalid value for {} config {} on node {}: '{}'. Expected true or false.",
            what, TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, nodeId, value);
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ElectionStrategizer)) return false;
        ElectionStrategizer other = (ElectionStrategizer) o;
        return Objects.equals(other.nodeUncleanConfig, nodeUncleanConfig) &&
            Objects.equals(other.clusterUncleanConfig, clusterUncleanConfig) &&
            other.topicUncleanConfigAccessor.equals(topicUncleanConfigAccessor) &&
            other.topicUncleanOverrides.equals(topicUncleanOverrides);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeUncleanConfig,
            clusterUncleanConfig,
            topicUncleanConfigAccessor,
            topicUncleanOverrides);
    }

    @Override
    public String toString() {
        return "ElectionStrategizer(nodeUncleanConfig=" + nodeUncleanConfig +
            ", clusterUncleanConfig=" + clusterUncleanConfig +
            ", topicUncleanConfigAccessor=" + topicUncleanConfigAccessor +
            ", topicUncleanOverrides=" + topicUncleanOverrides + ")";
    }
}
