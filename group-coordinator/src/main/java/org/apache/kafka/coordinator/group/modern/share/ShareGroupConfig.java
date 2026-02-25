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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class ShareGroupConfig {
    /** Share Group Configurations **/

    // Internal configuration used by integration and system tests.
    public static final String SHARE_GROUP_ENABLE_CONFIG = "group.share.enable";
    public static final boolean SHARE_GROUP_ENABLE_DEFAULT = false;
    public static final String SHARE_GROUP_ENABLE_DOC = "Enable share groups on the broker.";

    public static final String SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG = "group.share.partition.max.record.locks";
    public static final int SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DEFAULT = 2000;
    public static final String SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DOC = "Share-group record lock limit per share-partition.";

    public static final String SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG = "group.share.delivery.count.limit";
    public static final int SHARE_GROUP_DELIVERY_COUNT_LIMIT_DEFAULT = 5;
    public static final String SHARE_GROUP_DELIVERY_COUNT_LIMIT_DOC = "The maximum number of delivery attempts for a record delivered to a share group.";

    public static final String SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_CONFIG = "group.share.max.delivery.count.limit";
    public static final int SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_DEFAULT = 10;
    public static final String SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_DOC = "The maximum value of a group configuration for the maximum number of delivery attempts for a record delivered to a share group.";

    public static final String SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_CONFIG = "group.share.min.delivery.count.limit";
    public static final int SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_DEFAULT = 2;
    public static final String SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_DOC = "The minimum value of a group configuration for the maximum number of delivery attempts for a record delivered to a share group.";

    public static final String SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG = "group.share.record.lock.duration.ms";
    public static final int SHARE_GROUP_RECORD_LOCK_DURATION_MS_DEFAULT = 30000;
    public static final String SHARE_GROUP_RECORD_LOCK_DURATION_MS_DOC = "The record acquisition lock duration in milliseconds for share groups.";

    public static final String SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG = "group.share.min.record.lock.duration.ms";
    public static final int SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DEFAULT = 15000;
    public static final String SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DOC = "The minimum value of a group configuration for record acquisition lock duration in milliseconds.";

    public static final String SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG = "group.share.max.record.lock.duration.ms";
    public static final int SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DEFAULT = 60000;
    public static final String SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DOC = "The maximum value of a group configuration for record acquisition lock duration in milliseconds.";

    public static final String SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG = "share.fetch.purgatory.purge.interval.requests";
    public static final int SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT = 1000;
    public static final String SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC = "The purge interval (in number of requests) of the share fetch request purgatory";

    public static final String SHARE_GROUP_MAX_SHARE_SESSIONS_CONFIG = "group.share.max.share.sessions";
    public static final int SHARE_GROUP_MAX_SHARE_SESSIONS_DEFAULT = 2000;
    public static final String SHARE_GROUP_MAX_SHARE_SESSIONS_DOC = "The maximum number of share sessions per broker.";

    public static final String SHARE_GROUP_PERSISTER_CLASS_NAME_CONFIG = "group.share.persister.class.name";
    public static final String SHARE_GROUP_PERSISTER_CLASS_NAME_DEFAULT = "org.apache.kafka.server.share.persister.DefaultStatePersister";
    public static final String SHARE_GROUP_PERSISTER_CLASS_NAME_DOC = "The fully qualified name of a class which implements " +
        "the <code>org.apache.kafka.server.share.Persister</code> interface.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .defineInternal(SHARE_GROUP_ENABLE_CONFIG, BOOLEAN, SHARE_GROUP_ENABLE_DEFAULT, null, MEDIUM, SHARE_GROUP_ENABLE_DOC)
            .define(SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, INT, SHARE_GROUP_DELIVERY_COUNT_LIMIT_DEFAULT, between(2, 10), MEDIUM, SHARE_GROUP_DELIVERY_COUNT_LIMIT_DOC)
            .define(SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_CONFIG, INT, SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_DEFAULT, between(5, 25), MEDIUM, SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_DOC)
            .define(SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_CONFIG, INT, SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_DEFAULT, between(2, 5), MEDIUM, SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_DOC)
            .define(SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, INT, SHARE_GROUP_RECORD_LOCK_DURATION_MS_DEFAULT, between(1000, 3600000), MEDIUM, SHARE_GROUP_RECORD_LOCK_DURATION_MS_DOC)
            .define(SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, INT, SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DEFAULT, between(1000, 30000), MEDIUM, SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DOC)
            .define(SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, INT, SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DEFAULT, between(30000, 3600000), MEDIUM, SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DOC)
            .define(SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG, INT, SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DEFAULT, between(100, 10000), MEDIUM, SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DOC)
            .define(SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG, INT, SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT, MEDIUM, SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC)
            .define(SHARE_GROUP_MAX_SHARE_SESSIONS_CONFIG, INT, SHARE_GROUP_MAX_SHARE_SESSIONS_DEFAULT, atLeast(1), MEDIUM, SHARE_GROUP_MAX_SHARE_SESSIONS_DOC)
            .defineInternal(SHARE_GROUP_PERSISTER_CLASS_NAME_CONFIG, STRING, SHARE_GROUP_PERSISTER_CLASS_NAME_DEFAULT, null, MEDIUM, SHARE_GROUP_PERSISTER_CLASS_NAME_DOC);

    private final boolean isShareGroupEnabled;
    private final int shareGroupPartitionMaxRecordLocks;
    private final int shareGroupDeliveryCountLimit;
    private final int shareGroupMaxDeliveryCountLimit;
    private final int shareGroupMinDeliveryCountLimit;
    private final int shareGroupRecordLockDurationMs;
    private final int shareGroupMaxRecordLockDurationMs;
    private final int shareGroupMinRecordLockDurationMs;
    private final int shareFetchPurgatoryPurgeIntervalRequests;
    private final int shareGroupMaxShareSessions;
    private final String shareGroupPersisterClassName;
    private final AbstractConfig config;

    public ShareGroupConfig(AbstractConfig config) {
        this.config = config;
        // The proper way to enable share groups is to use the share.version feature with v1 or later.
        isShareGroupEnabled = config.getBoolean(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG);
        shareGroupPartitionMaxRecordLocks = config.getInt(ShareGroupConfig.SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG);
        shareGroupDeliveryCountLimit = config.getInt(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG);
        shareGroupMaxDeliveryCountLimit = config.getInt(SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_CONFIG);
        shareGroupMinDeliveryCountLimit = config.getInt(SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_CONFIG);
        shareGroupRecordLockDurationMs = config.getInt(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG);
        shareGroupMaxRecordLockDurationMs = config.getInt(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG);
        shareGroupMinRecordLockDurationMs = config.getInt(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG);
        shareFetchPurgatoryPurgeIntervalRequests = config.getInt(ShareGroupConfig.SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG);
        shareGroupMaxShareSessions = config.getInt(ShareGroupConfig.SHARE_GROUP_MAX_SHARE_SESSIONS_CONFIG);
        shareGroupPersisterClassName = config.getString(ShareGroupConfig.SHARE_GROUP_PERSISTER_CLASS_NAME_CONFIG);
        validate();
    }

    /** Share group configuration **/
    public boolean isShareGroupEnabled() {
        return isShareGroupEnabled;
    }

    public int shareGroupPartitionMaxRecordLocks() {
        return shareGroupPartitionMaxRecordLocks;
    }

    public int shareGroupDeliveryCountLimit() {
        return shareGroupDeliveryCountLimit;
    }

    public int shareGroupMaxDeliveryCountLimit() {
        return shareGroupMaxDeliveryCountLimit;
    }

    public int shareGroupMinDeliveryCountLimit() {
        return shareGroupMinDeliveryCountLimit;
    }

    public int shareGroupRecordLockDurationMs() {
        return shareGroupRecordLockDurationMs;
    }

    public int shareGroupMaxRecordLockDurationMs() {
        return shareGroupMaxRecordLockDurationMs;
    }

    public int shareGroupMinRecordLockDurationMs() {
        return shareGroupMinRecordLockDurationMs;
    }

    public int shareFetchPurgatoryPurgeIntervalRequests() {
        return shareFetchPurgatoryPurgeIntervalRequests;
    }

    public int shareGroupMaxShareSessions() {
        return shareGroupMaxShareSessions;
    }

    public String shareGroupPersisterClassName() {
        return shareGroupPersisterClassName;
    }

    private void validate() {
        Utils.require(shareGroupMaxDeliveryCountLimit >= shareGroupDeliveryCountLimit,
                String.format("%s must be greater than or equal to %s",
                        SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT_CONFIG, SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG));
        Utils.require(shareGroupDeliveryCountLimit >= shareGroupMinDeliveryCountLimit,
                String.format("%s must be greater than or equal to %s",
                        SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT_CONFIG));
        Utils.require(shareGroupRecordLockDurationMs >= shareGroupMinRecordLockDurationMs,
                String.format("%s must be greater than or equal to %s",
                        SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG));
        Utils.require(shareGroupMaxRecordLockDurationMs >= shareGroupRecordLockDurationMs,
                String.format("%s must be greater than or equal to %s",
                        SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG));
        Utils.require(shareGroupMaxShareSessions >= config.getInt(GroupCoordinatorConfig.SHARE_GROUP_MAX_SIZE_CONFIG),
                String.format("%s must be greater than or equal to %s",
                        SHARE_GROUP_MAX_SHARE_SESSIONS_CONFIG, GroupCoordinatorConfig.SHARE_GROUP_MAX_SIZE_CONFIG));
    }

    /**
     * Copy the subset of properties that are relevant to share group. These configs include those which can be set
     * statically (for all groups) or dynamically (for a specific group). In those cases, the default value for the
     * group specific dynamic config (Ex. share.session.timeout.ms) should be the value set for the static config
     * (Ex. group.share.session.timeout.ms).
     */
    public Map<String, Integer> extractShareGroupConfigMap(GroupCoordinatorConfig groupCoordinatorConfig) {
        return Map.of(
            GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG, groupCoordinatorConfig.shareGroupSessionTimeoutMs(),
            GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, groupCoordinatorConfig.shareGroupHeartbeatIntervalMs(),
            GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, shareGroupRecordLockDurationMs(),
            GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG, shareGroupDeliveryCountLimit()
        );
    }
}
