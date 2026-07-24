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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfigTest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_MAX_ASSIGNMENT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.SHARE_GROUP_MAX_ASSIGNMENT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MAX_ASSIGNMENT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MAX_STANDBY_REPLICAS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MAX_WARMUP_REPLICAS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MIN_TASK_OFFSET_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_DEFAULT;
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MAX_PARTITION_MAX_RECORD_LOCKS_DEFAULT;
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DEFAULT;
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_DEFAULT;
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MIN_PARTITION_MAX_RECORD_LOCKS_DEFAULT;
import static org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    @SuppressWarnings("CyclomaticComplexity")
    public void testFromPropsInvalid() {
        GroupConfig.configNames().forEach(name -> {
            if (GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2");
            } else if (GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2");
            } else if (GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-1", "1.2");
            } else if (GroupConfig.CONSUMER_ASSIGNOR_OFFLOAD_ENABLE_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_boolean");
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
            } else if (GroupConfig.SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-1", "1.2");
            } else if (GroupConfig.SHARE_ASSIGNOR_OFFLOAD_ENABLE_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_boolean");
            } else if (GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "1.0");
            } else if (GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "1.0");
            } else if (GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "1.0");
            } else if (GroupConfig.STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-1", "1.0");
            } else if (GroupConfig.STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-1", "1.2");
            } else if (GroupConfig.STREAMS_ASSIGNOR_OFFLOAD_ENABLE_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_boolean");
            } else if (GroupConfig.STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "1.0");
            } else if (GroupConfig.STREAMS_NUM_WARMUP_REPLICAS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "1.0");
            } else if (GroupConfig.ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_boolean");
            } else if (!GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG.equals(name)) {
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

        Map<String, String> props = createValidGroupConfig();

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
        Map<String, String> props = createValidGroupConfig();
        props.put(GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG, "read_committed");
        doTestValidProps(props);

        // Check for value READ_COMMITTED
        props = createValidGroupConfig();
        props.put(GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        doTestValidProps(props);
    }

    @Test
    public void testInvalidProps() {

        Map<String, String> props = createValidGroupConfig();

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

        // Check for invalid consumerAssignmentIntervalMs, < MIN
        props.put(GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG, "500");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid consumerAssignmentIntervalMs, > MAX
        props.put(GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG, "20000");
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

        // Check for invalid shareAssignmentIntervalMs, < MIN
        props.put(GroupConfig.SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG, "500");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid shareAssignmentIntervalMs, > MAX
        props.put(GroupConfig.SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG, "20000");
        doTestInvalidProps(props, InvalidConfigurationException.class);
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

        // Check for invalid streamsAssignmentIntervalMs, < MIN
        props.put(GroupConfig.STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG, "500");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid streamsAssignmentIntervalMs, > MAX
        props.put(GroupConfig.STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG, "20000");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid streamsTaskOffsetIntervalMs, < MIN
        props.put(GroupConfig.STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG, "1000");
        doTestInvalidProps(props, InvalidConfigurationException.class);
        props = createValidGroupConfig();

        // Check for invalid streamsNumWarmupReplicas, > MAX
        props.put(GroupConfig.STREAMS_NUM_WARMUP_REPLICAS_CONFIG, "50");
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

    private void doTestInvalidProps(Map<String, String> props, Class<? extends Exception> exceptionClassName) {
        assertThrows(exceptionClassName, () -> GroupConfig.validate(props, createGroupCoordinatorConfig(), createShareGroupConfig()));
    }

    private void doTestValidProps(Map<String, String> props) {
        assertDoesNotThrow(() -> GroupConfig.validate(props, createGroupCoordinatorConfig(), createShareGroupConfig()));
    }

    private static Stream<Arguments> outOfRangeValuesAndExpectedMessages() {
        return Stream.of(
            // Consumer group configs.
            Arguments.of(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "1",
                "consumer.heartbeat.interval.ms must be in the range 5 to 15000 inclusive."),
            Arguments.of(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "20000",
                "consumer.heartbeat.interval.ms must be in the range 5 to 15000 inclusive."),
            Arguments.of(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "1",
                "consumer.session.timeout.ms must be in the range 45 to 60000 inclusive."),
            Arguments.of(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "70000",
                "consumer.session.timeout.ms must be in the range 45 to 60000 inclusive."),
            Arguments.of(GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG, "500",
                "consumer.assignment.interval.ms must be in the range 1000 to 15000 inclusive."),
            Arguments.of(GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG, "20000",
                "consumer.assignment.interval.ms must be in the range 1000 to 15000 inclusive."),

            // Share group configs.
            Arguments.of(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, "1",
                "share.heartbeat.interval.ms must be in the range 5 to 15000 inclusive."),
            Arguments.of(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, "20000",
                "share.heartbeat.interval.ms must be in the range 5 to 15000 inclusive."),
            Arguments.of(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG, "1",
                "share.session.timeout.ms must be in the range 45 to 60000 inclusive."),
            Arguments.of(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG, "70000",
                "share.session.timeout.ms must be in the range 45 to 60000 inclusive."),
            Arguments.of(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "10000",
                "share.record.lock.duration.ms must be in the range 15000 to 60000 inclusive."),
            Arguments.of(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "70000",
                "share.record.lock.duration.ms must be in the range 15000 to 60000 inclusive."),
            Arguments.of(GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG, "11",
                "share.delivery.count.limit must be in the range 2 to 10 inclusive."),
            Arguments.of(GroupConfig.SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG, "11000",
                "share.partition.max.record.locks must be in the range 100 to 10000 inclusive."),
            Arguments.of(GroupConfig.SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG, "500",
                "share.assignment.interval.ms must be in the range 1000 to 15000 inclusive."),
            Arguments.of(GroupConfig.SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG, "20000",
                "share.assignment.interval.ms must be in the range 1000 to 15000 inclusive."),

            // Streams group configs.
            Arguments.of(GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, "1000",
                "streams.heartbeat.interval.ms must be in the range 5000 to 15000 inclusive."),
            Arguments.of(GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, "20000",
                "streams.heartbeat.interval.ms must be in the range 5000 to 15000 inclusive."),
            Arguments.of(GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG, "1",
                "streams.session.timeout.ms must be in the range 45000 to 60000 inclusive."),
            Arguments.of(GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG, "70000",
                "streams.session.timeout.ms must be in the range 45000 to 60000 inclusive."),
            Arguments.of(GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG, "5",
                "streams.num.standby.replicas must be less than or equal to 2"),
            Arguments.of(GroupConfig.STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG, "500",
                "streams.assignment.interval.ms must be in the range 1000 to 15000 inclusive."),
            Arguments.of(GroupConfig.STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG, "20000",
                "streams.assignment.interval.ms must be in the range 1000 to 15000 inclusive."),
            Arguments.of(GroupConfig.STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG, "1000",
                "streams.task.offset.interval.ms must be greater than or equal to 15000"),
            Arguments.of(GroupConfig.STREAMS_NUM_WARMUP_REPLICAS_CONFIG, "50",
                "streams.num.warmup.replicas must be less than or equal to 20")
        );
    }

    @ParameterizedTest(name = "testValidationErrorMessageIncludesBound[{0}={1}]")
    @MethodSource("outOfRangeValuesAndExpectedMessages")
    public void testValidationErrorMessageIncludesBound(String key, String value, String expectedMessage) {
        var props = Map.of(key, value);
        var exception = assertThrows(
            InvalidConfigurationException.class,
            () -> GroupConfig.validate(props, createGroupCoordinatorConfig(), createShareGroupConfig())
        );
        assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    public void testFromPropsWithDefaultValue() {
        Map<String, String> defaultValue = new HashMap<>();
        defaultValue.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG, "5000");
        defaultValue.put(GroupConfig.CONSUMER_ASSIGNOR_OFFLOAD_ENABLE_CONFIG, "false");
        defaultValue.put(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "2000");
        defaultValue.put(GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG, "2");
        defaultValue.put(GroupConfig.SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG, "500");
        defaultValue.put(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "latest");
        defaultValue.put(GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        defaultValue.put(GroupConfig.SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG, "2500");
        defaultValue.put(GroupConfig.SHARE_ASSIGNOR_OFFLOAD_ENABLE_CONFIG, "false");
        defaultValue.put(GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG, "2000");
        defaultValue.put(GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG, "1");
        defaultValue.put(GroupConfig.STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG, "3000");
        defaultValue.put(GroupConfig.STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG, "1250");
        defaultValue.put(GroupConfig.STREAMS_ASSIGNOR_OFFLOAD_ENABLE_CONFIG, "false");
        defaultValue.put(GroupConfig.STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG, "30000");
        defaultValue.put(GroupConfig.STREAMS_NUM_WARMUP_REPLICAS_CONFIG, "5");
        defaultValue.put(GroupConfig.SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG, "true");

        Properties props = new Properties();
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "20");
        GroupConfig config = GroupConfig.fromProps(defaultValue, props);

        assertEquals(10, config.getInt(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG));
        assertEquals(20, config.getInt(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG));
        assertEquals(5000, config.getInt(GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG));
        assertEquals(false, config.getBoolean(GroupConfig.CONSUMER_ASSIGNOR_OFFLOAD_ENABLE_CONFIG));
        assertEquals(10, config.getInt(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG));
        assertEquals(10, config.getInt(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG));
        assertEquals(2000, config.getInt(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG));
        assertEquals(2, config.getInt(GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG));
        assertEquals(500, config.getInt(GroupConfig.SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG));
        assertEquals("latest", config.getString(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG));
        assertEquals("read_uncommitted", config.getString(GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG));
        assertEquals(2500, config.getInt(GroupConfig.SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG));
        assertEquals(false, config.getBoolean(GroupConfig.SHARE_ASSIGNOR_OFFLOAD_ENABLE_CONFIG));
        assertEquals(10, config.getInt(GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG));
        assertEquals(2000, config.getInt(GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG));
        assertEquals(1, config.getInt(GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG));
        assertEquals(3000, config.getInt(GroupConfig.STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG));
        assertEquals(1250, config.getInt(GroupConfig.STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG));
        assertEquals(false, config.getBoolean(GroupConfig.STREAMS_ASSIGNOR_OFFLOAD_ENABLE_CONFIG));
        assertEquals(30000, config.getInt(GroupConfig.STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG));
        assertEquals(5, config.getInt(GroupConfig.STREAMS_NUM_WARMUP_REPLICAS_CONFIG));
        assertEquals(true, config.getBoolean(GroupConfig.SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG));
    }

    @Test
    public void testAllFieldsAbsentWhenNotConfigured() {
        // When no config is provided, all group-level values are empty.
        GroupConfig config = new GroupConfig(Map.of());

        // Consumer group configs
        assertEquals(Optional.empty(), config.consumerSessionTimeoutMs());
        assertEquals(Optional.empty(), config.consumerHeartbeatIntervalMs());
        assertEquals(Optional.empty(), config.consumerAssignmentIntervalMs());
        assertEquals(Optional.empty(), config.consumerAssignorOffloadEnable());

        // Share group configs
        assertEquals(Optional.empty(), config.shareSessionTimeoutMs());
        assertEquals(Optional.empty(), config.shareHeartbeatIntervalMs());
        assertEquals(Optional.empty(), config.shareRecordLockDurationMs());
        assertEquals(Optional.empty(), config.shareDeliveryCountLimit());
        assertEquals(Optional.empty(), config.sharePartitionMaxRecordLocks());
        assertEquals(Optional.empty(), config.shareAutoOffsetReset());
        assertEquals(Optional.empty(), config.shareAssignmentIntervalMs());
        assertEquals(Optional.empty(), config.shareAssignorOffloadEnable());
        assertEquals(Optional.empty(), config.shareIsolationLevel());
        assertEquals(Optional.empty(), config.shareRenewAcknowledgeEnable());

        // Streams group configs
        assertEquals(Optional.empty(), config.streamsSessionTimeoutMs());
        assertEquals(Optional.empty(), config.streamsHeartbeatIntervalMs());
        assertEquals(Optional.empty(), config.streamsNumStandbyReplicas());
        assertEquals(Optional.empty(), config.streamsInitialRebalanceDelayMs());
        assertEquals(Optional.empty(), config.streamsAssignmentIntervalMs());
        assertEquals(Optional.empty(), config.streamsAssignorOffloadEnable());
        assertEquals(Optional.empty(), config.streamsTaskOffsetIntervalMs());

        // DLQ configs - have defaults from CONFIG_DEF
        assertEquals("", config.errorsDLQTopicName());
        assertFalse(config.errorsDLQCopyRecordEnable());
    }

    @Test
    public void testAllFieldsPresentWhenConfigured() {
        // When all configs are provided, all group-level values are present.
        Map<String, String> props = new HashMap<>();
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "50000");
        props.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "6000");
        props.put(GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG, "5000");
        props.put(GroupConfig.CONSUMER_ASSIGNOR_OFFLOAD_ENABLE_CONFIG, "true");
        props.put(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG, "50000");
        props.put(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, "6000");
        props.put(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "20000");
        props.put(GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG, "5");
        props.put(GroupConfig.SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG, "1000");
        props.put(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(GroupConfig.SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG, "2500");
        props.put(GroupConfig.SHARE_ASSIGNOR_OFFLOAD_ENABLE_CONFIG, "true");
        props.put(GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(GroupConfig.SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG, "false");
        props.put(GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG, "50000");
        props.put(GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, "6000");
        props.put(GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG, "2");
        props.put(GroupConfig.STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG, "3000");
        props.put(GroupConfig.STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG, "1250");
        props.put(GroupConfig.STREAMS_ASSIGNOR_OFFLOAD_ENABLE_CONFIG, "false");
        props.put(GroupConfig.STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG, "30000");
        props.put(GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, "my-dlq-topic");
        props.put(GroupConfig.ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG, "true");

        GroupConfig config = new GroupConfig(props);

        // Consumer group configs
        assertEquals(Optional.of(50000), config.consumerSessionTimeoutMs());
        assertEquals(Optional.of(6000), config.consumerHeartbeatIntervalMs());
        assertEquals(Optional.of(5000), config.consumerAssignmentIntervalMs());
        assertEquals(Optional.of(true), config.consumerAssignorOffloadEnable());

        // Share group configs
        assertEquals(Optional.of(50000), config.shareSessionTimeoutMs());
        assertEquals(Optional.of(6000), config.shareHeartbeatIntervalMs());
        assertEquals(Optional.of(20000), config.shareRecordLockDurationMs());
        assertEquals(Optional.of(5), config.shareDeliveryCountLimit());
        assertEquals(Optional.of(1000), config.sharePartitionMaxRecordLocks());
        assertEquals(Optional.of(ShareGroupAutoOffsetResetStrategy.EARLIEST), config.shareAutoOffsetReset());
        assertEquals(Optional.of(2500), config.shareAssignmentIntervalMs());
        assertEquals(Optional.of(true), config.shareAssignorOffloadEnable());
        assertEquals(Optional.of(IsolationLevel.READ_COMMITTED), config.shareIsolationLevel());
        assertEquals(Optional.of(false), config.shareRenewAcknowledgeEnable());

        // Streams group configs
        assertEquals(Optional.of(50000), config.streamsSessionTimeoutMs());
        assertEquals(Optional.of(6000), config.streamsHeartbeatIntervalMs());
        assertEquals(Optional.of(2), config.streamsNumStandbyReplicas());
        assertEquals(Optional.of(3000), config.streamsInitialRebalanceDelayMs());
        assertEquals(Optional.of(1250), config.streamsAssignmentIntervalMs());
        assertEquals(Optional.of(false), config.streamsAssignorOffloadEnable());
        assertEquals(Optional.of(30000), config.streamsTaskOffsetIntervalMs());

        // DLQ configs
        assertEquals("my-dlq-topic", config.errorsDLQTopicName());
        assertTrue(config.errorsDLQCopyRecordEnable());
    }

    @Test
    public void testNotValidatedWhenNotConfigured() {
        // When configs are absent, validation should not use their default values.
        GroupCoordinatorConfig groupCoordinatorConfig = GroupCoordinatorConfig.fromProps(Map.of(
            GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, 2000,
            GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, 2000,
            GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, 2000,
            GroupCoordinatorConfig.CONSUMER_GROUP_MIN_ASSIGNMENT_INTERVAL_MS_CONFIG, 2000,
            GroupCoordinatorConfig.SHARE_GROUP_MIN_ASSIGNMENT_INTERVAL_MS_CONFIG, 2000,
            GroupCoordinatorConfig.STREAMS_GROUP_MIN_ASSIGNMENT_INTERVAL_MS_CONFIG, 2000
        ));
        assertDoesNotThrow(() -> GroupConfig.validate(Map.of(), groupCoordinatorConfig, createShareGroupConfig()));
    }

    @Test
    public void testInvalidConfigName() {
        Map<String, String> props = new HashMap<>();
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
            GroupConfig.validate(new HashMap<>(), groupCoordinatorConfig, shareGroupConfig));
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
            Arguments.of(
                GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
                0, /* CONSUMER_GROUP_MIN_ASSIGNMENT_INTERVAL_MS_CONFIG = */ 500,
                20000, CONSUMER_GROUP_MAX_ASSIGNMENT_INTERVAL_MS_DEFAULT
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
            Arguments.of(
                GroupConfig.SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG,
                0, /* SHARE_GROUP_MIN_ASSIGNMENT_INTERVAL_MS_CONFIG = */ 500,
                20000, SHARE_GROUP_MAX_ASSIGNMENT_INTERVAL_MS_DEFAULT
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
            ),
            Arguments.of(
                GroupConfig.STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG,
                0, /* STREAMS_GROUP_MIN_ASSIGNMENT_INTERVAL_MS_CONFIG = */ 500,
                20000, STREAMS_GROUP_MAX_ASSIGNMENT_INTERVAL_MS_DEFAULT
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
            ),
            Arguments.of(
                GroupConfig.STREAMS_NUM_WARMUP_REPLICAS_CONFIG,
                25, STREAMS_GROUP_MAX_WARMUP_REPLICAS_DEFAULT
            )
        );
    }

    /**
     * Data source for configs with min-only evaluation (no max bound enforced by evaluate).
     * Each entry: (configKey, tooLow, expectedMin).
     */
    private static Stream<Arguments> minBoundedConfigs() {
        return Stream.of(
            Arguments.of(
                GroupConfig.STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG,
                1000, STREAMS_GROUP_MIN_TASK_OFFSET_INTERVAL_MS_DEFAULT
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
            GroupCoordinatorConfig.fromProps(Map.of(
                GroupCoordinatorConfig.CONSUMER_GROUP_MIN_ASSIGNMENT_INTERVAL_MS_CONFIG, 500,
                GroupCoordinatorConfig.SHARE_GROUP_MIN_ASSIGNMENT_INTERVAL_MS_CONFIG, 500,
                GroupCoordinatorConfig.STREAMS_GROUP_MIN_ASSIGNMENT_INTERVAL_MS_CONFIG, 500
            )),
            ShareGroupConfig.fromProps(Map.of()));
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

    @ParameterizedTest(name = "testEvaluateMinBoundedValueBelowMinIsCapped[{0}]")
    @MethodSource("minBoundedConfigs")
    public void testEvaluateMinBoundedValueBelowMinIsCapped(
        String key,
        int tooLow,
        int expectedMin
    ) {
        Properties props = new Properties();
        props.put(key, tooLow);
        Properties result = GroupConfig.evaluate(props, "test-group",
            GroupCoordinatorConfig.fromProps(new HashMap<>()), ShareGroupConfig.fromProps(new HashMap<>()));
        assertEquals(expectedMin, result.get(key));
    }

    @Test
    public void testAllGroupConfigSynonyms() {
        // Every GroupConfig entry should have an entry in ALL_GROUP_CONFIG_SYNONYMS.
        for (String groupConfigName : GroupConfig.CONFIG_DEF.names()) {
            assertTrue(GroupConfig.ALL_GROUP_CONFIG_SYNONYMS.containsKey(groupConfigName),
                "GroupConfig entry '" + groupConfigName + "' is not in ALL_GROUP_CONFIG_SYNONYMS. " +
                    "Add it with Optional.of(brokerConfigName) or Optional.empty() if it has no broker synonym.");
        }

        // Every key in ALL_GROUP_CONFIG_SYNONYMS should be a valid GroupConfig entry.
        for (String key : GroupConfig.ALL_GROUP_CONFIG_SYNONYMS.keySet()) {
            assertTrue(GroupConfig.CONFIG_DEF.names().contains(key),
                "ALL_GROUP_CONFIG_SYNONYMS contains '" + key + "' which is not a valid GroupConfig entry.");
        }

        // Every present synonym mapping should point to a valid broker config.
        Set<String> brokerConfigNames = new HashSet<>();
        brokerConfigNames.addAll(GroupCoordinatorConfig.CONFIG_DEF.names());
        brokerConfigNames.addAll(ShareGroupConfig.CONFIG_DEF.names());

        for (Map.Entry<String, Optional<String>> entry : GroupConfig.ALL_GROUP_CONFIG_SYNONYMS.entrySet()) {
            entry.getValue().ifPresent(brokerConfigName ->
                assertTrue(brokerConfigNames.contains(brokerConfigName),
                    "ALL_GROUP_CONFIG_SYNONYMS maps '" + entry.getKey() + "' to '" +
                        brokerConfigName + "' but this broker config does not exist."));
        }
    }

    private Map<String, String> createValidGroupConfig() {
        Map<String, String> props = new HashMap<>();
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
        props.put(GroupConfig.STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG, "45000");
        props.put(GroupConfig.STREAMS_NUM_WARMUP_REPLICAS_CONFIG, "3");
        props.put(GroupConfig.SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG, "true");
        return props;
    }

    private GroupCoordinatorConfig createGroupCoordinatorConfig() {
        return GroupCoordinatorConfigTest.createGroupCoordinatorConfig(
            OFFSET_METADATA_MAX_SIZE,
            OFFSETS_RETENTION_CHECK_INTERVAL_MS,
            OFFSETS_RETENTION_MINUTES,
            Map.of(
                GroupCoordinatorConfig.CONSUMER_GROUP_MIN_ASSIGNMENT_INTERVAL_MS_CONFIG, 1000,
                GroupCoordinatorConfig.SHARE_GROUP_MIN_ASSIGNMENT_INTERVAL_MS_CONFIG, 1000,
                GroupCoordinatorConfig.STREAMS_GROUP_MIN_ASSIGNMENT_INTERVAL_MS_CONFIG, 1000
            )
        );
    }

    private ShareGroupConfig createShareGroupConfig() {
        return ShareGroupConfigTest.createShareGroupConfig(SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS,
                SHARE_GROUP_MIN_PARTITION_MAX_RECORD_LOCKS, SHARE_GROUP_MAX_PARTITION_MAX_RECORD_LOCKS,
                SHARE_GROUP_DELIVERY_COUNT_LIMIT, SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT, SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT,
                SHARE_GROUP_RECORD_LOCK_DURATION_MS, SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS, SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS);
    }

    @Test
    public void testDLQConfigDefaults() {
        // Test default DLQ configuration values (KIP-1191)
        Map<String, String> configs = new HashMap<>();
        GroupConfig config = new GroupConfig(configs);

        assertEquals("", config.errorsDLQTopicName());
        assertFalse(config.errorsDLQCopyRecordEnable());
    }

    @Test
    public void testDLQConfigCustomValues() {
        // Test custom DLQ configuration values
        Map<String, String> configs = new HashMap<>();
        configs.put(GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, "my-dlq-topic");
        configs.put(GroupConfig.ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG, "true");

        GroupConfig config = new GroupConfig(configs);

        assertEquals("my-dlq-topic", config.errorsDLQTopicName());
        assertTrue(config.errorsDLQCopyRecordEnable());
    }

    @Test
    public void testDLQTopicNameCannotStartWithDoubleUnderscore() {
        // DLQ topic name must not start with "__" (reserved for internal topics)
        Map<String, String> configs = new HashMap<>();
        configs.put(GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, "__my-dlq");

        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () ->
            GroupConfig.validate(configs, createGroupCoordinatorConfig(), createShareGroupConfig()));
        assertTrue(exception.getMessage().contains("DLQ topic name must not start with '__'"));
    }

    @Test
    public void testDLQBlankTopicNameIsValid() {
        // Blank DLQ topic name is valid (means DLQ is disabled for that group)
        Map<String, String> configs = new HashMap<>();
        configs.put(GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, "");

        GroupConfig config = new GroupConfig(configs);

        assertEquals("", config.errorsDLQTopicName());
    }
}
