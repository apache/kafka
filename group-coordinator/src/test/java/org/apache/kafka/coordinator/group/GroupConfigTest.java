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
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfigTest;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GroupConfigTest {

    private static final int OFFSET_METADATA_MAX_SIZE = 4096;
    private static final long OFFSETS_RETENTION_CHECK_INTERVAL_MS = 1000L;
    private static final int OFFSETS_RETENTION_MINUTES = 24 * 60;

    private static final boolean SHARE_GROUP_ENABLE = true;
    private static final int SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS = 200;
    private static final int SHARE_GROUP_DELIVERY_COUNT_LIMIT = 5;
    private static final short SHARE_GROUP_MAX_GROUPS = 10;
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

        Properties props = createValidGroupConfig();

        // Check for invalid consumerSessionTimeoutMs, < MIN
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "1");
        doTestInvalidProps(props);
        props = createValidGroupConfig();

        // Check for invalid consumerSessionTimeoutMs, > MAX
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "70000");
        doTestInvalidProps(props);
        props = createValidGroupConfig();

        // Check for invalid consumerHeartbeatIntervalMs, < MIN
        props.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "1");
        doTestInvalidProps(props);
        props = createValidGroupConfig();

        // Check for invalid consumerHeartbeatIntervalMs, > MAX
        props.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "70000");
        doTestInvalidProps(props);
        props = createValidGroupConfig();

        // Check for invalid shareSessionTimeoutMs, < MIN
        props.put(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG, "1");
        doTestInvalidProps(props);
        props = createValidGroupConfig();

        // Check for invalid shareSessionTimeoutMs, > MAX
        props.put(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG, "70000");
        doTestInvalidProps(props);
        props = createValidGroupConfig();

        // Check for invalid shareHeartbeatIntervalMs, < MIN
        props.put(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, "1");
        doTestInvalidProps(props);
        props = createValidGroupConfig();

        // Check for invalid shareHeartbeatIntervalMs, > MAX
        props.put(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, "70000");
        doTestInvalidProps(props);
        props = createValidGroupConfig();

        // Check for invalid shareRecordLockDurationMs, < MIN
        props.put(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "10000");
        doTestInvalidProps(props);
        props = createValidGroupConfig();

        // Check for invalid shareRecordLockDurationMs, > MAX
        props.put(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "70000");
        doTestInvalidProps(props);
    }

    private void doTestInvalidProps(Properties props) {
        assertThrows(InvalidConfigurationException.class, () -> GroupConfig.validate(props, createGroupCoordinatorConfig(), createShareGroupConfig()));
    }

    @Test
    public void testFromPropsWithDefaultValue() {
        Map<String, String> defaultValue = new HashMap<>();
        defaultValue.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, "10");
        defaultValue.put(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "2000");

        Properties props = new Properties();
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "20");
        GroupConfig config = GroupConfig.fromProps(defaultValue, props);

        assertEquals(10, config.getInt(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG));
        assertEquals(20, config.getInt(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG));
        assertEquals(10, config.getInt(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG));
        assertEquals(10, config.getInt(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG));
        assertEquals(2000, config.getInt(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG));
    }

    @Test
    public void testInvalidConfigName() {
        Properties props = new Properties();
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "10");
        props.put("invalid.config.name", "10");
        assertThrows(InvalidConfigurationException.class, () -> GroupConfig.validate(props, createGroupCoordinatorConfig(), createShareGroupConfig()));
    }

    private Properties createValidGroupConfig() {
        Properties props = new Properties();
        props.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "45000");
        props.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "5000");
        props.put(GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG, "45000");
        props.put(GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, "5000");
        props.put(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "30000");
        return props;
    }

    private GroupCoordinatorConfig createGroupCoordinatorConfig() {
        return GroupCoordinatorConfigTest.createGroupCoordinatorConfig(OFFSET_METADATA_MAX_SIZE, OFFSETS_RETENTION_CHECK_INTERVAL_MS, OFFSETS_RETENTION_MINUTES);
    }

    private ShareGroupConfig createShareGroupConfig() {
        return ShareGroupConfigTest.createShareGroupConfig(SHARE_GROUP_ENABLE, SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS, SHARE_GROUP_DELIVERY_COUNT_LIMIT,
            SHARE_GROUP_MAX_GROUPS, SHARE_GROUP_RECORD_LOCK_DURATION_MS, SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS, SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS);
    }
}
