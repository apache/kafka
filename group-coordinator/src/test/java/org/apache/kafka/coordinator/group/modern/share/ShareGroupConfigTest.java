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
package org.apache.kafka.coordinator.group.modern.share;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShareGroupConfigTest {

    @Test
    public void testConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG, true);
        configs.put(ShareGroupConfig.SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG, 200);
        configs.put(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, 5);
        configs.put(ShareGroupConfig.SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_CONFIG, 2);
        configs.put(ShareGroupConfig.SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_CONFIG, 9);
        configs.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, 30000);
        configs.put(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, 15000);
        configs.put(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, 60000);
        configs.put(ShareGroupConfig.SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG, 1000);
        configs.put(ShareGroupConfig.SHARE_GROUP_MAX_SHARE_SESSIONS_CONFIG, 1000);

        ShareGroupConfig config = createConfig(configs);

        assertTrue(config.isShareGroupEnabled());
        assertEquals(200, config.shareGroupPartitionMaxRecordLocks());
        assertEquals(5, config.shareGroupDeliveryCountLimit());
        assertEquals(30000, config.shareGroupRecordLockDurationMs());
        assertEquals(15000, config.shareGroupMinRecordLockDurationMs());
        assertEquals(60000, config.shareGroupMaxRecordLockDurationMs());
        assertEquals(1000, config.shareFetchPurgatoryPurgeIntervalRequests());
        assertEquals(1000, config.shareGroupMaxShareSessions());
    }

    @Test
    public void testInvalidConfigs() {
        Map<String, Object> configs = new HashMap<>();
        // test for when SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG is less than SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG
        configs.put(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, 60000);
        configs.put(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, 15000);
        configs.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, 10000);
        assertEquals("group.share.record.lock.duration.ms must be greater than or equal to group.share.min.record.lock.duration.ms",
            assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        // test for when SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG is greater than SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG
        configs.put(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, 50000);
        configs.put(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, 15000);
        configs.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, 60000);
        assertEquals("group.share.max.record.lock.duration.ms must be greater than or equal to group.share.record.lock.duration.ms",
            assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        // test for when SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG is less than SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_CONFIG
        configs.put(ShareGroupConfig.SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_CONFIG, 3);
        configs.put(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, 2);
        assertEquals("group.share.delivery.count.limit must be greater than or equal to group.share.min.delivery.count.limit",
                assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        // test for when SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG is greater than SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_CONFIG
        configs.put(ShareGroupConfig.SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_CONFIG, 9);
        configs.put(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, 10);
        assertEquals("group.share.max.delivery.count.limit must be greater than or equal to group.share.delivery.count.limit",
                assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        // test for when SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG is out of bounds
        configs.put(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, 1);
        assertEquals("Invalid value 1 for configuration group.share.delivery.count.limit: Value must be at least 2",
            assertThrows(ConfigException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        // test for when SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG is out of bounds
        configs.put(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, 11);
        assertEquals("Invalid value 11 for configuration group.share.delivery.count.limit: Value must be no more than 10",
            assertThrows(ConfigException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        // test for when SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG is out of bounds
        configs.put(ShareGroupConfig.SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG, 50);
        assertEquals("Invalid value 50 for configuration group.share.partition.max.record.locks: Value must be at least 100",
            assertThrows(ConfigException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        // test for when SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG is out of bounds
        configs.put(ShareGroupConfig.SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG, 20000);
        assertEquals("Invalid value 20000 for configuration group.share.partition.max.record.locks: Value must be no more than 10000",
            assertThrows(ConfigException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        // test for when SHARE_GROUP_MAX_SHARE_SESSIONS_CONFIG is less than SHARE_GROUP_MAX_SIZE_CONFIG
        configs.put(GroupCoordinatorConfig.SHARE_GROUP_MAX_SIZE_CONFIG, 2000);
        configs.put(ShareGroupConfig.SHARE_GROUP_MAX_SHARE_SESSIONS_CONFIG, 1000);
        assertEquals("Invalid value 2000 for configuration group.share.max.size: Value must be no more than 1000",
            assertThrows(ConfigException.class, () -> createConfig(configs)).getMessage());
    }

    public static ShareGroupConfig createShareGroupConfig(
        boolean shareGroupEnable,
        int shareGroupPartitionMaxRecordLocks,
        int shareGroupDeliveryCountLimit,
        int shareGroupMinDeliveryCountLimit,
        int shareGroupMaxDeliveryCountLimit,
        int shareGroupRecordLockDurationsMs,
        int shareGroupMinRecordLockDurationMs,
        int shareGroupMaxRecordLockDurationMs
    ) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG, shareGroupEnable);
        configs.put(ShareGroupConfig.SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG, shareGroupPartitionMaxRecordLocks);
        configs.put(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, shareGroupDeliveryCountLimit);
        configs.put(ShareGroupConfig.SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_CONFIG, shareGroupMinDeliveryCountLimit);
        configs.put(ShareGroupConfig.SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_CONFIG, shareGroupMaxDeliveryCountLimit);
        configs.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, shareGroupRecordLockDurationsMs);
        configs.put(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, shareGroupMinRecordLockDurationMs);
        configs.put(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, shareGroupMaxRecordLockDurationMs);
        configs.put(GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG, 1);
        configs.put(GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, 10);

        return createConfig(configs);
    }

    private static ShareGroupConfig createConfig(Map<String, Object> configs) {
        return new ShareGroupConfig(
            new AbstractConfig(Utils.mergeConfigs(Arrays.asList(ShareGroupConfig.CONFIG_DEF, GroupCoordinatorConfig.CONFIG_DEF)), configs, false));
    }
}
