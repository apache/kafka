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

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

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
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_GROUP;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SourceConnectorConfigTest {

    private static final String FOO_CONNECTOR = "foo-source";
    private static final String TOPIC_CREATION_GROUP_1 = "group1";
    private static final String TOPIC_CREATION_GROUP_2 = "group2";
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
    public void shouldNotFailWithExplicitlySpecifiedDefaultTopicCreationGroup() {
        Map<String, String> props = defaultConnectorProps();
        props.put(TOPIC_CREATION_GROUPS_CONFIG, String.join(",", DEFAULT_TOPIC_CREATION_GROUP,
            TOPIC_CREATION_GROUP_1, TOPIC_CREATION_GROUP_2));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, "1");
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, "1");
        SourceConnectorConfig config = new SourceConnectorConfig(MOCK_PLUGINS, props, true);
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
        assertThat(e.getMessage(), containsString("Replication factor must be positive and not "
                + "larger than the number of brokers in the Kafka cluster, or -1 to use the "
                + "broker's default"));
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
            assertThat(e.getMessage(), containsString("Replication factor must be positive and not "
                    + "larger than the number of brokers in the Kafka cluster, or -1 to use the "
                    + "broker's default"));
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

    private static Map<String, String> convertToStringValues(Map<String, Object> config) {
        // null values are not allowed
        return config.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    Objects.requireNonNull(e.getValue());
                    return e.getValue().toString();
                }));
    }
}
