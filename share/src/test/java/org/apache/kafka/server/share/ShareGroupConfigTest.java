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
package org.apache.kafka.server.share;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorCommonConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ShareGroupConfigTest {
    public static ShareGroupConfig createShareGroupConfig(
        boolean shareGroupEnable,
        int shareGroupPartitionMaxRecordLocks,
        int shareGroupDeliveryCountLimit,
        short shareGroupsMaxGroups,
        int shareGroupRecordLockDurationsMs,
        int shareGroupMinRecordLockDurationMs,
        int shareGroupMaxRecordLockDurationMs
    ) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG, shareGroupEnable);
        configs.put(ShareGroupConfig.SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG, shareGroupPartitionMaxRecordLocks);
        configs.put(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, shareGroupDeliveryCountLimit);
        configs.put(ShareGroupConfig.SHARE_GROUP_MAX_GROUPS_CONFIG, shareGroupsMaxGroups);
        configs.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, shareGroupRecordLockDurationsMs);
        configs.put(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, shareGroupMinRecordLockDurationMs);
        configs.put(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, shareGroupMaxRecordLockDurationMs);
        configs.put(CoordinatorCommonConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,consumer,share");

        return createConfig(configs);
    }

    private static ShareGroupConfig createConfig(Map<String, Object> configs) {
        return new ShareGroupConfig(
            new AbstractConfig(Utils.mergeConfigs(Arrays.asList(ShareGroupConfig.CONFIG_DEF, CoordinatorCommonConfig.COMMON_GROUP_COORDINATOR_CONFIG_DEF)), configs, false));
    }
}
