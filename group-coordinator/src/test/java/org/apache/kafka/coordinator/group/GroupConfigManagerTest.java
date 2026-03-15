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

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.coordinator.group.GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GroupConfigManagerTest {

    private GroupConfigManager configManager;

    @BeforeEach
    public void setUp() {
        configManager = createConfigManager();
    }

    @AfterEach
    public void tearDown() {
        if (configManager != null) {
            configManager.close();
        }
    }

    @Test
    public void testUpdateConfigWithInvalidGroupId() {
        assertThrows(InvalidRequestException.class,
            () -> configManager.updateGroupConfig("", new Properties()));
    }

    @Test
    public void testGetNonExistentGroupConfig() {
        Optional<GroupConfig> groupConfig = configManager.groupConfig("foo");
        assertFalse(groupConfig.isPresent());
    }

    @Test
    public void testUpdateConfigWithNullGroupId() {
        assertThrows(InvalidRequestException.class,
            () -> configManager.updateGroupConfig(null, new Properties()));
    }

    @Test
    public void testUpdateGroupConfig() {
        String groupId = "foo";
        Properties props = new Properties();
        props.put(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, 50000);
        props.put(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, 6000);
        configManager.updateGroupConfig(groupId, props);

        Optional<GroupConfig> configOptional = configManager.groupConfig(groupId);
        assertTrue(configOptional.isPresent());

        GroupConfig config = configOptional.get();
        assertEquals(50000, config.getInt(CONSUMER_SESSION_TIMEOUT_MS_CONFIG));
        assertEquals(6000, config.getInt(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG));
    }

    @Test
    public void testClampWithCustomBrokerBounds() {
        Map<String, Object> overrides = new HashMap<>();
        overrides.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, 50000);
        overrides.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 46000);
        overrides.put(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, 48000);
        configManager = createConfigManager(overrides);

        String groupId = "test-group";

        // Value above custom max is clamped to custom max.
        Properties props1 = new Properties();
        props1.put(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, 55000);
        configManager.updateGroupConfig(groupId, props1);
        assertEquals(50000, configManager.groupConfig(groupId).get().getInt(CONSUMER_SESSION_TIMEOUT_MS_CONFIG));

        // Value below custom min is clamped to custom min.
        Properties props2 = new Properties();
        props2.put(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, 44000);
        configManager.updateGroupConfig(groupId, props2);
        assertEquals(46000, configManager.groupConfig(groupId).get().getInt(CONSUMER_SESSION_TIMEOUT_MS_CONFIG));

        // Value within custom range is stored as-is.
        Properties props3 = new Properties();
        props3.put(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, 49000);
        configManager.updateGroupConfig(groupId, props3);
        assertEquals(49000, configManager.groupConfig(groupId).get().getInt(CONSUMER_SESSION_TIMEOUT_MS_CONFIG));
    }

    @Test
    public void testGroupIsRemovedWhenDynamicConfigsAreRemoved() {
        String groupId1 = "foo";
        String groupId2 = "bar";
        Properties props = new Properties();
        props.put(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, 50000);
        configManager.updateGroupConfig(groupId1, props);
        configManager.updateGroupConfig(groupId2, props);
        assertTrue(configManager.groupIds().contains(groupId1));

        configManager.updateGroupConfig(groupId1, new Properties());
        assertFalse(configManager.groupIds().contains(groupId1));
        assertTrue(configManager.groupIds().contains(groupId2));
    }

    public static GroupConfigManager createConfigManager() {
        return createConfigManager(new HashMap<>());
    }

    public static GroupConfigManager createConfigManager(Map<String, Object> overrides) {
        GroupCoordinatorConfig groupCoordinatorConfig = GroupCoordinatorConfig.fromProps(overrides);
        ShareGroupConfig shareGroupConfig = ShareGroupConfig.fromProps(overrides);

        Map<String, Integer> defaultConfig = new HashMap<>(groupCoordinatorConfig.extractGroupConfigMap(shareGroupConfig));

        return new GroupConfigManager(defaultConfig, groupCoordinatorConfig, shareGroupConfig);
    }
}
