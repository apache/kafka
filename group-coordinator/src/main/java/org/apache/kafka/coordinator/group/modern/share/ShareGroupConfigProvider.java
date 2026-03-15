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

import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupConfigManager;

/**
 * A provider that retrieves share group dynamic configuration values,
 * falling back to default values when group-specific configurations are not present.
 */
public class ShareGroupConfigProvider {
    private final GroupConfigManager manager;

    public ShareGroupConfigProvider(GroupConfigManager manager) {
        this.manager = manager;
    }

    /**
     * The method is used to get the record lock duration for the group. If the group config is present,
     * then the record lock duration is returned. Otherwise, the default value is returned.
     *
     * @param groupId The group id for which the record lock duration is to be fetched.
     * @param defaultValue The default value to be returned if the group config is not present.
     * @return The record lock duration for the group.
     */
    public int recordLockDurationMsOrDefault(String groupId, int defaultValue) {
        return manager.groupConfig(groupId)
            .map(GroupConfig::shareRecordLockDurationMs)
            .orElse(defaultValue);
    }

    /**
     * The method is used to get the delivery count limit for the group. If the group config is present,
     * then the delivery count limit is returned. Otherwise, the default value is returned.
     *
     * @param groupId The group id for which the delivery count limit is to be fetched.
     * @param defaultValue The default value to be returned if the group config is not present.
     * @return The delivery count limit for the group.
     */
    public int deliveryCountLimitOrDefault(String groupId, int defaultValue) {
        return manager.groupConfig(groupId)
            .map(GroupConfig::shareDeliveryCountLimit)
            .orElse(defaultValue);
    }

    /**
     * The method is used to get the partition max record locks for the group. If the group config is present,
     * then the partition max record locks is returned. Otherwise, the default value is returned.
     *
     * @param groupId The group id for which the partition max record locks is to be fetched.
     * @param defaultValue The default value to be returned if the group config is not present.
     * @return The partition max record locks for the group.
     */
    public int partitionMaxRecordLocksOrDefault(String groupId, int defaultValue) {
        return manager.groupConfig(groupId)
            .map(GroupConfig::sharePartitionMaxRecordLocks)
            .orElse(defaultValue);
    }

    /**
     * The method is used to check if renew acknowledge is enabled for the group. If the group config
     * is present, then the value from the group config is used. Otherwise, the default value is used.
     *
     * @param groupId The group id for which the renew acknowledge enable is to be checked.
     * @return true if renew acknowledge is enabled for the group, false otherwise.
     */
    public boolean isRenewAcknowledgeEnabled(String groupId) {
        return manager.groupConfig(groupId)
            .map(GroupConfig::shareRenewAcknowledgeEnable)
            .orElse(GroupConfig.SHARE_RENEW_ACKNOWLEDGE_ENABLE_DEFAULT);
    }
}
