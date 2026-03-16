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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfigTest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

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
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MAX_PARTITION_MAX_RECORD_LOCKS_DEFAULT;
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_DEFAULT;
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MIN_PARTITION_MAX_RECORD_LOCKS_DEFAULT;
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GroupConfigTest {

    private static final int OFFSET_METADATA_MAX_SIZE = 4096;
    private static final long OFFSETS_RETENTION_CHECK_INTERVAL_MS = 1000L;
    private static final int OFFSETS_RETENTION_MINUTES = 24 * 60;

    private static final int SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS = 200;
    private static final int SHARE_GROUP_MIN_PARTITION_MAX_RECORD_LOCKS = 100;
    private static final int SHARE_GROUP_MAX_PARTITION_MAX_RECORD_LOCKS = 10000;
    private static final int SHARE_GROUP_DELIVERY_COUNT_LIMIT = 5;
    private static final int SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT = 2;
    private static final int SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT = 10;
    private static final int SHARE_GROUP_RECORD_LOCK_DURATION_MS = 30000;
    private static final int SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS = 15000;
    private static final int SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS = 60000;

    @Test
    public void testFromPropsInvalid() {
        GroupConfig.configNames().forEach(name -> {
            if (GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2");
            } else if (GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2");
            } else if (GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2");
            } else if (GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2");
            } else if (GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2");
            } else if (GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2");
            } else if (GroupConfig.SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2");
            } else if (GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "hello", "1.0");
            } else if (GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "hello", "1.0");
            } else if (GroupConfig.SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_boolean", "1");
            } else if (GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "1.0");
            } else if (GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "1.0");
            } else if (GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "1.0");
            } else if (GroupConfig.STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-1", "1.0");
            } else {
                assertPropertyInvalid(name, "not_a_number", "-0.1");
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
    public void testValidShareAutoOffsetResetValues() {

        Properties props = createValidGroupConfig();

        // Check for value "latest"
        props.put(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "latest");
        doTestValidProps(props);
        props = createValidGroupConfig();

        // Check for value "earliest"
        props.put(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "earliest");
        doTestValidProps(props);

        // Check for value "by_duration"
        props.put(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "by_duration:PT10S");
        doTestValidProps(props);
    }

    @Test
    public void testValidShareIsolationLevelValues() {
        // Check for value READ_UNCOMMITTED
        Properties props = createValidGroupConfig();
        props.put(GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG, "read_committed");
        doTestValidProps(props);

        // Check for value READ_COMMITTED
        props = createValidGroupConfig();
        props.put(GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        doTestValidProps(props);
    }

    @Test
    public void testInvalidProps() {

        Properties props = createValidGroupConfig();

        // Check for invalid consumerSessionTimeoutMs, < MIN
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "1");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid consumerSessionTimeoutMs, > MAX
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "70000");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid consumerHeartbeatIntervalMs, < MIN
        props.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "1");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid consumerHeartbeatIntervalMs, > MAX
        props.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "70000");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid shareSessionTimeoutMs, < MIN
        props.put(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG, "1");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid shareSessionTimeoutMs, > MAX
        props.put(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG, "70000");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid shareHeartbeatIntervalMs, < MIN
        props.put(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, "1");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid shareHeartbeatIntervalMs, > MAX
        props.put(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, "70000");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid shareRecordLockDurationMs, < MIN
        props.put(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "10000");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid shareRecordLockDurationMs, > MAX
        props.put(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "70000");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid shareDeliveryCountLimit, < MIN
        props.put(GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG, "1");
        doTestInvalidProps(props, ConfigException.class);
        props = createValidGroupConfig();

        // Check for invalid shareDeliveryCountLimit, > MAX
        props.put(GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG, "11");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid sharePartitionMaxRecordLocks, < MIN
        props.put(GroupConfig.SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG, "50");
        doTestInvalidProps(props, ConfigException.class);
        props = createValidGroupConfig();

        // Check for invalid sharePartitionMaxRecordLocks, > MAX
        props.put(GroupConfig.SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG, "11000");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid shareAutoOffsetReset
        props.put(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "hello");
        doTestInvalidProps(props, ConfigException.class);

        // Check for invalid shareAutoOffsetReset, by_duration without duration
        props.put(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "by_duration");
        doTestInvalidProps(props, ConfigException.class);

        // Check for invalid shareAutoOffsetReset, by_duration with negative duration
        props.put(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "by_duration:-PT10S");
        doTestInvalidProps(props, ConfigException.class);

        // Check for invalid shareAutoOffsetReset, by_duration with invalid duration
        props.put(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "by_duration:invalid");
        doTestInvalidProps(props, ConfigException.class);
        props = createValidGroupConfig();

        // Check for invalid streamsSessionTimeoutMs, < MIN
        props.put(GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG, "1");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid streamsSessionTimeoutMs, > MAX
        props.put(GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG, "70000");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid streamsHeartbeatIntervalMs, < MIN
        props.put(GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid streamsHeartbeatIntervalMs, > MAX
        props.put(GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, "70000");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid shareIsolationLevel.
        props.put(GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG, "read_commit");
        doTestInvalidProps(props, ConfigException.class);
        props = createValidGroupConfig();

        // Check for invalid shareIsolationLevel.
        props.put(GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG, "read_uncommit");
        doTestInvalidProps(props, ConfigException.class);
        props = createValidGroupConfig();

        // Check for invalid shareRenewAcknowledgeEnable.
        props.put(GroupConfig.SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG, "1");
        doTestInvalidProps(props, ConfigException.class);
    }

    private void doTestInvalidProps(Properties props, Class<? extends Exception> exceptionClassName) {
        assertThrows(exceptionClassName, () -> GroupConfig.validate(props, createGroupCoordinatorConfig(), createShareGroupConfig()));
    }

    private void doTestValidProps(Properties props) {
        assertDoesNotThrow(() -> GroupConfig.validate(props, createGroupCoordinatorConfig(), createShareGroupConfig()));
    }

    @Test
    public void testFromPropsWithDefaultValue() {
        Map<String, String> defaultValue = new HashMap<>();
        defaultValue.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "2000");
        defaultValue.put(GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG, "2");
        defaultValue.put(GroupConfig.SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG, "500");
        defaultValue.put(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "latest");
        defaultValue.put(GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        defaultValue.put(GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG, "2000");
        defaultValue.put(GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG, "1");
        defaultValue.put(GroupConfig.STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG, "3000");
        defaultValue.put(GroupConfig.SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG, "true");

        Properties props = new Properties();
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "20");
        GroupConfig config = GroupConfig.fromProps(defaultValue, props);

        assertEquals(10, config.getInt(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG));
        assertEquals(20, config.getInt(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG));
        assertEquals(10, config.getInt(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG));
        assertEquals(10, config.getInt(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG));
        assertEquals(2000, config.getInt(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG));
        assertEquals(2, config.getInt(GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG));
        assertEquals(500, config.getInt(GroupConfig.SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG));
        assertEquals("latest", config.getString(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG));
        assertEquals("read_uncommitted", config.getString(GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG));
        assertEquals(10, config.getInt(GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG));
        assertEquals(2000, config.getInt(GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG));
        assertEquals(1, config.getInt(GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG));
        assertEquals(3000, config.getInt(GroupConfig.STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG));
        assertEquals(true, config.getBoolean(GroupConfig.SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG));
    }

    @Test
    public void testInvalidConfigName() {
        Properties props = new Properties();
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "10");
        props.put("invalid.config.name", "10");
        assertThrows(InvalidConfigurationException.class, () -> GroupConfig.validate(props, createGroupCoordinatorConfig(), createShareGroupConfig()));
    }

    @Test
    public void testValidateWithAllGroupTypeConfigs() {
        Map<String, Object> overrides = new HashMap<>();
        // Consumer
        overrides.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 46000);
        overrides.put(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, 46000);
        // Streams
        overrides.put(GroupCoordinatorConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 46000);
        overrides.put(GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG, 46000);
        // Share
        overrides.put(GroupCoordinatorConfig.SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 46000);
        overrides.put(GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG, 46000);

        GroupCoordinatorConfig groupCoordinatorConfig = GroupCoordinatorConfig.fromProps(overrides);
        ShareGroupConfig shareGroupConfig = ShareGroupConfig.fromProps(overrides);

        assertDoesNotThrow(() ->
            GroupConfig.validate(new Properties(), groupCoordinatorConfig, shareGroupConfig));
    }

    @Test
    public void testEvaluateEmptyPropsReturnsEmpty() {
        Properties result = GroupConfig.evaluate(
            new Properties(), "test-group",
            GroupCoordinatorConfig.fromProps(new HashMap<>()), ShareGroupConfig.fromProps(new HashMap<>()));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testEvaluateDoesNotModifyInput() {
        Properties props = new Properties();
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, 70000);

        Properties propsSnapshot = new Properties();
        propsSnapshot.putAll(props);

        GroupConfig.evaluate(props, "test-group",
            GroupCoordinatorConfig.fromProps(new HashMap<>()), ShareGroupConfig.fromProps(new HashMap<>()));
        assertEquals(propsSnapshot, props);
    }

    /**
     * Data source for configs with bidirectional [min, max] evaluation.
     * Each entry: (configKey, tooLow, expectedMin, tooHigh, expectedMax).
     */
    private static Stream<Arguments> rangeBoundedConfigs() {
        return Stream.of(
            // Consumer group configs
            Arguments.of(
                GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG,
                40000, CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT,
                70000, CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT
            ),
            Arguments.of(
                GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG,
                3000, CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT,
                20000, CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT
            ),
            // Share group configs
            Arguments.of(
                GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG,
                40000, SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT,
                70000, SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT
            ),
            Arguments.of(
                GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG,
                3000, SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT,
                20000, SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT
            ),
            Arguments.of(
                GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG,
                10000, SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DEFAULT,
                70000, SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DEFAULT
            ),
            Arguments.of(
                GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG,
                1, SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_DEFAULT,
                15, SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_DEFAULT
            ),
            Arguments.of(
                GroupConfig.SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG,
                50, SHARE_GROUP_MIN_PARTITION_MAX_RECORD_LOCKS_DEFAULT,
                5000, SHARE_GROUP_MAX_PARTITION_MAX_RECORD_LOCKS_DEFAULT
            ),
            // Streams group configs
            Arguments.of(
                GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG,
                40000, STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT,
                70000, STREAMS_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT
            ),
            Arguments.of(
                GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG,
                3000, STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT,
                20000, STREAMS_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT
            )
        );
    }

    /**
     * Data source for configs with max-only evaluation (no min bound enforced by evaluate).
     * Each entry: (configKey, tooHigh, expectedMax).
     */
    private static Stream<Arguments> maxBoundedConfigs() {
        return Stream.of(
            Arguments.of(
                GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG,
                5, STREAMS_GROUP_MAX_STANDBY_REPLICAS_DEFAULT
            )
        );
    }

    @ParameterizedTest(name = "testEvaluateValueAboveMaxIsCapped[{0}]")
    @MethodSource("rangeBoundedConfigs")
    public void testEvaluateValueAboveMaxIsCapped(
        String key,
        int tooLow,
        int expectedMin,
        int tooHigh,
        int expectedMax
    ) {
        Properties props = new Properties();
        props.put(key, tooHigh);
        Properties result = GroupConfig.evaluate(props, "test-group",
            GroupCoordinatorConfig.fromProps(new HashMap<>()), ShareGroupConfig.fromProps(new HashMap<>()));
        assertEquals(expectedMax, result.get(key));
    }

    @ParameterizedTest(name = "testEvaluateValueBelowMinIsCapped[{0}]")
    @MethodSource("rangeBoundedConfigs")
    public void testEvaluateValueBelowMinIsCapped(
        String key,
        int tooLow,
        int expectedMin,
        int tooHigh,
        int expectedMax
    ) {
        Properties props = new Properties();
        props.put(key, tooLow);
        Properties result = GroupConfig.evaluate(props, "test-group",
            GroupCoordinatorConfig.fromProps(new HashMap<>()), ShareGroupConfig.fromProps(new HashMap<>()));
        assertEquals(expectedMin, result.get(key));
    }

    @ParameterizedTest(name = "testEvaluateMaxBoundedValueAboveMaxIsCapped[{0}]")
    @MethodSource("maxBoundedConfigs")
    public void testEvaluateMaxBoundedValueAboveMaxIsCapped(
        String key,
        int tooHigh,
        int expectedMax
    ) {
        Properties props = new Properties();
        props.put(key, tooHigh);
        Properties result = GroupConfig.evaluate(props, "test-group",
            GroupCoordinatorConfig.fromProps(new HashMap<>()), ShareGroupConfig.fromProps(new HashMap<>()));
        assertEquals(expectedMax, result.get(key));
    }

    private Properties createValidGroupConfig() {
        Properties props = new Properties();
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "45000");
        props.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "5000");
        props.put(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG, "45000");
        props.put(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, "5000");
        props.put(GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG, "5");
        props.put(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "30000");
        props.put(GroupConfig.SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG, "2000");
        props.put(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        props.put(GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG, "50000");
        props.put(GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, "6000");
        props.put(GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG, "1");
        props.put(GroupConfig.STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG, "3000");
        props.put(GroupConfig.SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG, "true");
        return props;
    }

    private GroupCoordinatorConfig createGroupCoordinatorConfig() {
        return GroupCoordinatorConfigTest.createGroupCoordinatorConfig(OFFSET_METADATA_MAX_SIZE, OFFSETS_RETENTION_CHECK_INTERVAL_MS, OFFSETS_RETENTION_MINUTES);
    }

    private ShareGroupConfig createShareGroupConfig() {
        return ShareGroupConfigTest.createShareGroupConfig(SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS,
                SHARE_GROUP_MIN_PARTITION_MAX_RECORD_LOCKS, SHARE_GROUP_MAX_PARTITION_MAX_RECORD_LOCKS,
                SHARE_GROUP_DELIVERY_COUNT_LIMIT, SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT, SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT,
                SHARE_GROUP_RECORD_LOCK_DURATION_MS, SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS, SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS);
    }
}
