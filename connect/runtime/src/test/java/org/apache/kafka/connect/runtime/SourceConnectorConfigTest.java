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

package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.util.TopicAdmin;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfigTest.MOCK_PLUGINS;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TOPIC_CREATION_GROUPS_CONFIG;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_GROUP;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.EXCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.INCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SourceConnectorConfigTest {

    private static final String FOO_CONNECTOR = "foo-source";
    private static final String FOO_GROUP = "foo";
    private static final String FOO_TOPIC = "foo-topic";
    private static final String FOO_REGEX = ".*foo.*";

    private static final String BAR_GROUP = "bar";
    private static final String BAR_TOPIC = "bar-topic";
    private static final String BAR_REGEX = ".*bar.*";

    private static final short DEFAULT_REPLICATION_FACTOR = -1;
    private static final int DEFAULT_PARTITIONS = -1;

    public Map<String, String> defaultConnectorProps() {
        Map<String, String> props = new HashMap<>();
        props.put(NAME_CONFIG, FOO_CONNECTOR);
        props.put(CONNECTOR_CLASS_CONFIG, ConnectorConfigTest.TestConnector.class.getName());
        return props;
    }

    public Map<String, String> defaultConnectorPropsWithTopicCreation() {
        Map<String, String> props = defaultConnectorProps();
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(DEFAULT_REPLICATION_FACTOR));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(DEFAULT_PARTITIONS));
        return props;
    }

    @Test
    public void noTopicCreation() {
        Map<String, String> props = defaultConnectorProps();
        SourceConnectorConfig config = new SourceConnectorConfig(MOCK_PLUGINS, props, false);
        assertFalse(config.usesTopicCreation());
    }

    @Test
    public void shouldNotAllowZeroPartitionsOrReplicationFactor() {
        Map<String, String> props = defaultConnectorPropsWithTopicCreation();
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(0));
        Exception e = assertThrows(ConfigException.class, () -> new SourceConnectorConfig(MOCK_PLUGINS, props, true));
        assertThat(e.getMessage(), containsString("Number of partitions must be positive, or -1"));

        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(DEFAULT_PARTITIONS));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(0));

        e = assertThrows(ConfigException.class, () -> new SourceConnectorConfig(MOCK_PLUGINS, props, true));
        assertThat(e.getMessage(), containsString("Replication factor must be positive, or -1"));
    }

    @Test
    public void shouldNotAllowPartitionsOrReplicationFactorLessThanNegativeOne() {
        Map<String, String> props = defaultConnectorPropsWithTopicCreation();
        for (int i = -2; i > -100; --i) {
            props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(i));
            props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(DEFAULT_REPLICATION_FACTOR));
            Exception e = assertThrows(ConfigException.class, () -> new SourceConnectorConfig(MOCK_PLUGINS, props, true));
            assertThat(e.getMessage(), containsString("Number of partitions must be positive, or -1"));

            props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(DEFAULT_PARTITIONS));
            props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(i));
            e = assertThrows(ConfigException.class, () -> new SourceConnectorConfig(MOCK_PLUGINS, props, true));
            assertThat(e.getMessage(), containsString("Replication factor must be positive, or -1"));
        }
    }

    @Test
    public void shouldAllowNegativeOneAndPositiveForReplicationFactor() {
        Map<String, String> props = defaultConnectorPropsWithTopicCreation();
        SourceConnectorConfig config = new SourceConnectorConfig(MOCK_PLUGINS, props, true);
        assertTrue(config.usesTopicCreation());

        for (int i = 1; i <= 100; ++i) {
            props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(i));
            props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(DEFAULT_REPLICATION_FACTOR));
            config = new SourceConnectorConfig(MOCK_PLUGINS, props, true);
            assertTrue(config.usesTopicCreation());

            props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(DEFAULT_PARTITIONS));
            props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(i));
            config = new SourceConnectorConfig(MOCK_PLUGINS, props, true);
            assertTrue(config.usesTopicCreation());
        }
    }

    @Test
    public void shouldAllowSettingTopicProperties() {
        Map<String, String> topicProps = new HashMap<>();
        topicProps.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
        topicProps.put(COMPRESSION_TYPE_CONFIG, "lz4");
        topicProps.put(RETENTION_MS_CONFIG, String.valueOf(TimeUnit.DAYS.toMillis(30)));

        Map<String, String> props = defaultConnectorPropsWithTopicCreation();
        topicProps.forEach((k, v) -> props.put(DEFAULT_TOPIC_CREATION_PREFIX + k, v));

        SourceConnectorConfig config = new SourceConnectorConfig(MOCK_PLUGINS, props, true);
        assertEquals(topicProps,
                convertToStringValues(config.topicCreationOtherConfigs(DEFAULT_TOPIC_CREATION_GROUP)));
    }

    @Test
    public void withDefaultTopicCreation() {
        Map<String, String> props = defaultConnectorPropsWithTopicCreation();
        // Setting here but they should be ignored for the default group
        props.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + INCLUDE_REGEX_CONFIG, FOO_REGEX);
        props.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + EXCLUDE_REGEX_CONFIG, BAR_REGEX);

        SourceConnectorConfig config = new SourceConnectorConfig(MOCK_PLUGINS, props, true);
        assertTrue(config.usesTopicCreation());
        assertEquals(DEFAULT_REPLICATION_FACTOR, (short) config.topicCreationReplicationFactor(DEFAULT_TOPIC_CREATION_GROUP));
        assertEquals(DEFAULT_PARTITIONS, (int) config.topicCreationPartitions(DEFAULT_TOPIC_CREATION_GROUP));
        assertThat(config.topicCreationInclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.singletonList(".*")));
        assertThat(config.topicCreationExclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyList()));
        assertThat(config.topicCreationOtherConfigs(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyMap()));

        Map<String, TopicAdmin.NewTopicCreationGroup> groups =
                TopicAdmin.NewTopicCreationGroup.configuredGroups(config);
        assertEquals(1, groups.size());
        assertThat(groups.keySet(), hasItem(DEFAULT_TOPIC_CREATION_GROUP));

        TopicAdmin.NewTopicCreationGroup group = groups.get(DEFAULT_TOPIC_CREATION_GROUP);
        // Default group will match all topics besides empty string
        assertTrue(group.matches(" "));
        assertTrue(group.matches(FOO_TOPIC));
        assertTrue(DEFAULT_TOPIC_CREATION_GROUP.equals(group.name()));

        NewTopic topicSpec = group.newTopic(FOO_TOPIC);
        assertEquals(FOO_TOPIC, topicSpec.name());
        assertEquals(DEFAULT_REPLICATION_FACTOR, topicSpec.replicationFactor());
        assertEquals(DEFAULT_PARTITIONS, topicSpec.numPartitions());
        assertThat(topicSpec.configs(), is(Collections.emptyMap()));
    }

    @Test
    public void topicCreationWithDefaultGroupAndCustomProps() {
        short replicas = 3;
        int partitions = 5;
        long retentionMs = TimeUnit.DAYS.toMillis(30);
        String compressionType = "lz4";
        Map<String, String> topicProps = new HashMap<>();
        topicProps.put(COMPRESSION_TYPE_CONFIG, compressionType);
        topicProps.put(RETENTION_MS_CONFIG, String.valueOf(retentionMs));

        Map<String, String> props = defaultConnectorPropsWithTopicCreation();
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(replicas));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(partitions));
        topicProps.forEach((k, v) -> props.put(DEFAULT_TOPIC_CREATION_PREFIX + k, v));
        // Setting here but they should be ignored for the default group
        props.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + INCLUDE_REGEX_CONFIG, FOO_REGEX);
        props.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + EXCLUDE_REGEX_CONFIG, BAR_REGEX);

        SourceConnectorConfig config = new SourceConnectorConfig(MOCK_PLUGINS, props, true);
        assertTrue(config.usesTopicCreation());
        assertEquals(replicas, (short) config.topicCreationReplicationFactor(DEFAULT_TOPIC_CREATION_GROUP));
        assertEquals(partitions, (int) config.topicCreationPartitions(DEFAULT_TOPIC_CREATION_GROUP));
        assertThat(config.topicCreationInclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.singletonList(".*")));
        assertThat(config.topicCreationExclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyList()));
        assertThat(config.topicCreationOtherConfigs(DEFAULT_TOPIC_CREATION_GROUP), is(topicProps));

        Map<String, TopicAdmin.NewTopicCreationGroup> groups =
                TopicAdmin.NewTopicCreationGroup.configuredGroups(config);
        assertEquals(1, groups.size());
        assertThat(groups.keySet(), hasItem(DEFAULT_TOPIC_CREATION_GROUP));
        TopicAdmin.NewTopicCreationGroup group = groups.get(DEFAULT_TOPIC_CREATION_GROUP);
        // Default group will match all topics besides empty string
        assertTrue(group.matches(" "));
        assertTrue(group.matches(FOO_TOPIC));
        assertTrue(DEFAULT_TOPIC_CREATION_GROUP.equals(group.name()));

        NewTopic topicSpec = group.newTopic(FOO_TOPIC);
        assertEquals(FOO_TOPIC, topicSpec.name());
        assertEquals(replicas, topicSpec.replicationFactor());
        assertEquals(partitions, topicSpec.numPartitions());
        assertThat(topicSpec.configs(), is(topicProps));
    }

    @Test
    public void topicCreationWithOneGroup() {
        short fooReplicas = 3;
        int partitions = 5;
        Map<String, String> props = defaultConnectorPropsWithTopicCreation();
        props.put(TOPIC_CREATION_GROUPS_CONFIG, String.join(",", FOO_GROUP));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(partitions));
        // Setting here but they should be ignored for the default group
        props.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + INCLUDE_REGEX_CONFIG, FOO_REGEX);
        props.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + EXCLUDE_REGEX_CONFIG, BAR_REGEX);
        props.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + REPLICATION_FACTOR_CONFIG, String.valueOf(fooReplicas));

        Map<String, String> topicProps = new HashMap<>();
        topicProps.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
        topicProps.forEach((k, v) -> props.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + k, v));

        SourceConnectorConfig config = new SourceConnectorConfig(MOCK_PLUGINS, props, true);
        assertTrue(config.usesTopicCreation());
        assertEquals(DEFAULT_REPLICATION_FACTOR, (short) config.topicCreationReplicationFactor(DEFAULT_TOPIC_CREATION_GROUP));
        assertEquals(partitions, (int) config.topicCreationPartitions(DEFAULT_TOPIC_CREATION_GROUP));
        assertThat(config.topicCreationInclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.singletonList(".*")));
        assertThat(config.topicCreationExclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyList()));
        assertThat(config.topicCreationOtherConfigs(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyMap()));

        Map<String, TopicAdmin.NewTopicCreationGroup> groups =
                TopicAdmin.NewTopicCreationGroup.configuredGroups(config);
        assertEquals(2, groups.size());
        assertThat(groups.keySet(), hasItems(DEFAULT_TOPIC_CREATION_GROUP, FOO_GROUP));

        TopicAdmin.NewTopicCreationGroup fooGroup = groups.get(FOO_GROUP);
        TopicAdmin.NewTopicCreationGroup defaultGroup = groups.get(DEFAULT_TOPIC_CREATION_GROUP);
        assertFalse(fooGroup.matches(" "));
        assertTrue(fooGroup.matches(FOO_TOPIC));
        assertFalse(fooGroup.matches(BAR_TOPIC));
        assertTrue(FOO_GROUP.equals(fooGroup.name()));
        // Default group will match all topics besides empty string
        assertTrue(defaultGroup.matches(" "));
        assertTrue(defaultGroup.matches(FOO_TOPIC));
        assertTrue(defaultGroup.matches(BAR_TOPIC));
        assertTrue(DEFAULT_TOPIC_CREATION_GROUP.equals(defaultGroup.name()));

        NewTopic defaultTopicSpec = defaultGroup.newTopic(BAR_TOPIC);
        assertEquals(BAR_TOPIC, defaultTopicSpec.name());
        assertEquals(DEFAULT_REPLICATION_FACTOR, defaultTopicSpec.replicationFactor());
        assertEquals(partitions, defaultTopicSpec.numPartitions());
        assertThat(defaultTopicSpec.configs(), is(Collections.emptyMap()));

        NewTopic fooTopicSpec = fooGroup.newTopic(FOO_TOPIC);
        assertEquals(FOO_TOPIC, fooTopicSpec.name());
        assertEquals(fooReplicas, fooTopicSpec.replicationFactor());
        assertEquals(partitions, fooTopicSpec.numPartitions());
        assertThat(fooTopicSpec.configs(), is(topicProps));
    }

    @Test
    public void topicCreationWithOneGroupAndCombinedRegex() {
        short fooReplicas = 3;
        int partitions = 5;
        Map<String, String> props = defaultConnectorPropsWithTopicCreation();
        props.put(TOPIC_CREATION_GROUPS_CONFIG, String.join(",", FOO_GROUP));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(partitions));
        // Setting here but they should be ignored for the default group
        props.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + INCLUDE_REGEX_CONFIG, String.join("|", FOO_REGEX, BAR_REGEX));
        props.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + REPLICATION_FACTOR_CONFIG, String.valueOf(fooReplicas));

        Map<String, String> topicProps = new HashMap<>();
        topicProps.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
        topicProps.forEach((k, v) -> props.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + k, v));

        SourceConnectorConfig config = new SourceConnectorConfig(MOCK_PLUGINS, props, true);
        assertTrue(config.usesTopicCreation());
        assertEquals(DEFAULT_REPLICATION_FACTOR, (short) config.topicCreationReplicationFactor(DEFAULT_TOPIC_CREATION_GROUP));
        assertEquals(partitions, (int) config.topicCreationPartitions(DEFAULT_TOPIC_CREATION_GROUP));
        assertThat(config.topicCreationInclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.singletonList(".*")));
        assertThat(config.topicCreationExclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyList()));
        assertThat(config.topicCreationOtherConfigs(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyMap()));

        Map<String, TopicAdmin.NewTopicCreationGroup> groups =
                TopicAdmin.NewTopicCreationGroup.configuredGroups(config);
        assertEquals(2, groups.size());
        assertThat(groups.keySet(), hasItems(DEFAULT_TOPIC_CREATION_GROUP, FOO_GROUP));

        TopicAdmin.NewTopicCreationGroup fooGroup = groups.get(FOO_GROUP);
        TopicAdmin.NewTopicCreationGroup defaultGroup = groups.get(DEFAULT_TOPIC_CREATION_GROUP);
        assertFalse(fooGroup.matches(" "));
        assertTrue(fooGroup.matches(FOO_TOPIC));
        assertTrue(fooGroup.matches(BAR_TOPIC));
        assertTrue(FOO_GROUP.equals(fooGroup.name()));
        // Default group will match all topics besides empty string
        assertTrue(defaultGroup.matches(" "));
        assertTrue(defaultGroup.matches(FOO_TOPIC));
        assertTrue(defaultGroup.matches(BAR_TOPIC));
        assertTrue(DEFAULT_TOPIC_CREATION_GROUP.equals(defaultGroup.name()));

        NewTopic defaultTopicSpec = defaultGroup.newTopic(BAR_TOPIC);
        assertEquals(BAR_TOPIC, defaultTopicSpec.name());
        assertEquals(DEFAULT_REPLICATION_FACTOR, defaultTopicSpec.replicationFactor());
        assertEquals(partitions, defaultTopicSpec.numPartitions());
        assertThat(defaultTopicSpec.configs(), is(Collections.emptyMap()));

        NewTopic fooTopicSpec = fooGroup.newTopic(FOO_TOPIC);
        assertEquals(FOO_TOPIC, fooTopicSpec.name());
        assertEquals(fooReplicas, fooTopicSpec.replicationFactor());
        assertEquals(partitions, fooTopicSpec.numPartitions());
        assertThat(fooTopicSpec.configs(), is(topicProps));
    }

    private static Map<String, String> convertToStringValues(Map<String, Object> config) {
        // null values are not allowed
        return config.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    Objects.requireNonNull(e.getValue());
                    return e.getValue().toString();
                }));
    }
}
