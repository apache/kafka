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

package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.errors.InvalidConfigurationException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GroupConfigTest {

    @Test
    public void testFromPropsInvalid() {
        GroupConfig.configNames().forEach(name -> {
            if (GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2");
            } else if (GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2");
            } else {
                assertPropertyInvalid(name, "not_a_number", "-1");
            }
        });
    }

    private void assertPropertyInvalid(String name, Object... values) {
        for (Object value : values) {
            Properties props = new Properties();
            props.setProperty(name, value.toString());
            assertThrows(Exception.class, () -> new GroupConfig(props));
        }
    }

    @Test
    public void testInvalidProps() {
        GroupCoordinatorConfig defaultGroupCoordinatorConfig = createGroupCoordinatorConfig();
        // Check for invalid sessionTimeoutMs, < MIN
        doTestInvalidProps(1, 5000, defaultGroupCoordinatorConfig);

        // Check for invalid sessionTimeoutMs, > MAX
        doTestInvalidProps(70000, 5000, defaultGroupCoordinatorConfig);

        // Check for invalid heartbeatIntervalMs, < MIN
        doTestInvalidProps(50000, 1, defaultGroupCoordinatorConfig);

        // Check for invalid heartbeatIntervalMs, > MAX
        doTestInvalidProps(50000, 70000, defaultGroupCoordinatorConfig);

        // Check for invalid heartbeatIntervalMs >= sessionTimeoutMs
        Map<String, Object> configs = new HashMap<>();
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, 45000);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, 60000);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, 45000);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, 50000);
        doTestInvalidProps(45000, 45000, GroupCoordinatorConfigTest.createConfig(configs));
    }

    private void doTestInvalidProps(int sessionTimeoutMs, int heartbeatIntervalMs, GroupCoordinatorConfig groupCoordinatorConfig) {
        Properties props = new Properties();
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        assertThrows(InvalidConfigurationException.class, () -> GroupConfig.validate(props, groupCoordinatorConfig));
    }

    @Test
    public void testFromPropsWithDefaultValue() {
        Map<String, String> defaultValue = new HashMap<>();
        defaultValue.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "10");

        Properties props = new Properties();
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "20");
        GroupConfig config = GroupConfig.fromProps(defaultValue, props);

        assertEquals(10, config.getInt(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG));
        assertEquals(20, config.getInt(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG));
    }

    @Test
    public void testInvalidConfigName() {
        Properties props = new Properties();
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "10");
        props.put("invalid.config.name", "10");
        assertThrows(InvalidConfigurationException.class, () -> GroupConfig.validate(props, createGroupCoordinatorConfig()));
    }

    private GroupCoordinatorConfig createGroupCoordinatorConfig() {
        return GroupCoordinatorConfigTest.createGroupCoordinatorConfig(4096, 1000L, 24 * 60);
    }
}
