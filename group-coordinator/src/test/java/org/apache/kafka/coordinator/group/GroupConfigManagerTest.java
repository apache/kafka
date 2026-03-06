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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

import static org.apache.kafka.coordinator.group.GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MAX_STANDBY_REPLICAS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_DEFAULT;
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_DEFAULT;
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DEFAULT;
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

    /**
     * Data source for {@link #testConfigClampedToMax} and {@link #testConfigClampedToMin}.
     * Each entry: (configKey, tooLow, expectedMin, tooHigh, expectedMax, getter).
     */
    private static Stream<Arguments> clampConfigProvider() {
        return Stream.of(
            // Consumer group configs
            Arguments.of(
                CONSUMER_SESSION_TIMEOUT_MS_CONFIG,
                40000, CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT,
                70000, CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT,
                (ToIntFunction<GroupConfig>) GroupConfig::consumerSessionTimeoutMs
            ),
            Arguments.of(
                CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG,
                3000, CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT,
                20000, CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT,
                (ToIntFunction<GroupConfig>) GroupConfig::consumerHeartbeatIntervalMs
            ),
            // Share group configs
            Arguments.of(
                SHARE_SESSION_TIMEOUT_MS_CONFIG,
                40000, SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT,
                70000, SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT,
                (ToIntFunction<GroupConfig>) GroupConfig::shareSessionTimeoutMs
            ),
            Arguments.of(
                SHARE_HEARTBEAT_INTERVAL_MS_CONFIG,
                3000, SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT,
                20000, SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT,
                (ToIntFunction<GroupConfig>) GroupConfig::shareHeartbeatIntervalMs
            ),
            Arguments.of(
                SHARE_RECORD_LOCK_DURATION_MS_CONFIG,
                10000, SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DEFAULT,
                70000, SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DEFAULT,
                (ToIntFunction<GroupConfig>) GroupConfig::shareRecordLockDurationMs
            ),
            Arguments.of(
                SHARE_DELIVERY_COUNT_LIMIT_CONFIG,
                1, SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_DEFAULT,
                15, SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_DEFAULT,
                (ToIntFunction<GroupConfig>) GroupConfig::shareDeliveryCountLimit
            ),
            // Streams group configs
            Arguments.of(
                STREAMS_SESSION_TIMEOUT_MS_CONFIG,
                40000, STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT,
                70000, STREAMS_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT,
                (ToIntFunction<GroupConfig>) GroupConfig::streamsSessionTimeoutMs
            ),
            Arguments.of(
                STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG,
                3000, STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT,
                20000, STREAMS_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT,
                (ToIntFunction<GroupConfig>) GroupConfig::streamsHeartbeatIntervalMs
            ),
            Arguments.of(
                STREAMS_NUM_STANDBY_REPLICAS_CONFIG,
                -1, 0,
                5, STREAMS_GROUP_MAX_STANDBY_REPLICAS_DEFAULT,
                (ToIntFunction<GroupConfig>) GroupConfig::streamsNumStandbyReplicas
            )
        );
    }

    @ParameterizedTest(name = "testConfigClampedToMax[{0}]")
    @MethodSource("clampConfigProvider")
    public void testConfigClampedToMax(String key, int tooLow, int expectedMin,
                                       int tooHigh, int expectedMax,
                                       ToIntFunction<GroupConfig> getter) {
        String groupId = "test-group";
        Properties props = new Properties();
        props.put(key, tooHigh);
        configManager.updateGroupConfig(groupId, props);

        assertEquals(expectedMax, getter.applyAsInt(configManager.groupConfig(groupId).get()));
    }

    @ParameterizedTest(name = "testConfigClampedToMin[{0}]")
    @MethodSource("clampConfigProvider")
    public void testConfigClampedToMin(String key, int tooLow, int expectedMin,
                                       int tooHigh, int expectedMax,
                                       ToIntFunction<GroupConfig> getter) {
        String groupId = "test-group";
        Properties props = new Properties();
        props.put(key, tooLow);
        configManager.updateGroupConfig(groupId, props);

        assertEquals(expectedMin, getter.applyAsInt(configManager.groupConfig(groupId).get()));
    }

    public static GroupConfigManager createConfigManager() {
        return createConfigManager(new HashMap<>());
    }

    public static GroupConfigManager createConfigManager(Map<String, Object> overrides) {
        GroupCoordinatorConfig groupCoordinatorConfig = createGroupCoordinatorConfig(overrides);
        ShareGroupConfig shareGroupConfig = createShareGroupConfig(overrides);

        Map<String, Integer> defaultConfig = new HashMap<>(groupCoordinatorConfig.extractGroupConfigMap(shareGroupConfig));

        return new GroupConfigManager(defaultConfig, groupCoordinatorConfig, shareGroupConfig);
    }

    private static GroupCoordinatorConfig createGroupCoordinatorConfig(Map<String, Object> overrides) {
        return new GroupCoordinatorConfig(new AbstractConfig(
            GroupCoordinatorConfig.CONFIG_DEF,
            overrides,
            false
        ));
    }

    private static ShareGroupConfig createShareGroupConfig(Map<String, Object> overrides) {
        return new ShareGroupConfig(new AbstractConfig(
            Utils.mergeConfigs(Arrays.asList(ShareGroupConfig.CONFIG_DEF, GroupCoordinatorConfig.CONFIG_DEF)),
            overrides,
            false
        ));
    }
}
