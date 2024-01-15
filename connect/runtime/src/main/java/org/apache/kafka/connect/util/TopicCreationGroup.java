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

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.connect.runtime.SourceConnectorConfig;
import org.apache.kafka.connect.runtime.TopicCreationConfig;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TOPIC_CREATION_GROUPS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_GROUP;

/**
 * Represents a group of topics defined by inclusion/exclusion regex patterns along with the group's topic creation
 * configurations.
 *
 * @see TopicCreationConfig
 * @see TopicCreation
 */
public class TopicCreationGroup {
    private final String name;
    private final Pattern inclusionPattern;
    private final Pattern exclusionPattern;
    private final int numPartitions;
    private final short replicationFactor;
    private final Map<String, Object> otherConfigs;

    protected TopicCreationGroup(String group, SourceConnectorConfig config) {
        this.name = group;
        this.inclusionPattern = Pattern.compile(String.join("|", config.topicCreationInclude(group)));
        this.exclusionPattern = Pattern.compile(String.join("|", config.topicCreationExclude(group)));
        this.numPartitions = config.topicCreationPartitions(group);
        this.replicationFactor = config.topicCreationReplicationFactor(group);
        this.otherConfigs = config.topicCreationOtherConfigs(group);
    }

    /**
     * Parses the configuration of a source connector and returns the topic creation groups
     * defined in the given configuration as a map of group names to {@link TopicCreationGroup} objects.
     *
     * @param config the source connector configuration
     *
     * @return the map of topic creation groups; may be empty but not {@code null}
     */
    public static Map<String, TopicCreationGroup> configuredGroups(SourceConnectorConfig config) {
        if (!config.usesTopicCreation()) {
            return Collections.emptyMap();
        }
        List<String> groupNames = config.getList(TOPIC_CREATION_GROUPS_CONFIG);
        Map<String, TopicCreationGroup> groups = new LinkedHashMap<>();
        for (String group : groupNames) {
            groups.put(group, new TopicCreationGroup(group, config));
        }
        // Even if there was a group called 'default' in the config, it will be overridden here.
        // Order matters for all the topic groups besides the default, since it will be
        // removed from this collection by the Worker
        groups.put(DEFAULT_TOPIC_CREATION_GROUP, new TopicCreationGroup(DEFAULT_TOPIC_CREATION_GROUP, config));
        return groups;
    }

    /**
     * Return the name of the topic creation group.
     *
     * @return the name of the topic creation group
     */
    public String name() {
        return name;
    }

    /**
     * Answer whether this topic creation group is configured to allow the creation of the given
     * {@code topic} name.
     *
     * @param topic the topic name to check against the groups configuration
     *
     * @return true if the topic name matches the inclusion regex and does
     * not match the exclusion regex of this group's configuration; false otherwise
     */
    public boolean matches(String topic) {
        return !exclusionPattern.matcher(topic).matches() && inclusionPattern.matcher(topic)
                .matches();
    }

    /**
     * Return the description for a new topic with the given {@code topic} name with the topic
     * settings defined for this topic creation group.
     *
     * @param topic the name of the topic to be created
     *
     * @return the topic description of the given topic with settings of this topic creation group
     */
    public NewTopic newTopic(String topic) {
        TopicAdmin.NewTopicBuilder builder = new TopicAdmin.NewTopicBuilder(topic);
        return builder.partitions(numPartitions)
                .replicationFactor(replicationFactor)
                .config(otherConfigs)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopicCreationGroup)) {
            return false;
        }
        TopicCreationGroup that = (TopicCreationGroup) o;
        return Objects.equals(name, that.name)
                && numPartitions == that.numPartitions
                && replicationFactor == that.replicationFactor
                && Objects.equals(inclusionPattern.pattern(), that.inclusionPattern.pattern())
                && Objects.equals(exclusionPattern.pattern(), that.exclusionPattern.pattern())
                && Objects.equals(otherConfigs, that.otherConfigs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, numPartitions, replicationFactor, inclusionPattern.pattern(),
                exclusionPattern.pattern(), otherConfigs
        );
    }

    @Override
    public String toString() {
        return "TopicCreationGroup{" +
                "name='" + name + '\'' +
                ", inclusionPattern=" + inclusionPattern +
                ", exclusionPattern=" + exclusionPattern +
                ", numPartitions=" + numPartitions +
                ", replicationFactor=" + replicationFactor +
                ", otherConfigs=" + otherConfigs +
                '}';
    }
}
