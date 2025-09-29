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

import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.SHORT;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class ShareGroupConfig {
    /** Share Group Configurations **/

    // Internal configuration used by integration and system tests.
    public static final String SHARE_GROUP_ENABLE_CONFIG = "group.share.enable";
    public static final boolean SHARE_GROUP_ENABLE_DEFAULT = false;
    public static final String SHARE_GROUP_ENABLE_DOC = "Enable share groups on the broker.";

    public static final String SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG = "group.share.partition.max.record.locks";
    public static final int SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DEFAULT = 200;
    public static final String SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DOC = "Share-group record lock limit per share-partition.";

    public static final String SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG = "group.share.delivery.count.limit";
    public static final int SHARE_GROUP_DELIVERY_COUNT_LIMIT_DEFAULT = 5;
    public static final String SHARE_GROUP_DELIVERY_COUNT_LIMIT_DOC = "The maximum number of delivery attempts for a record delivered to a share group.";

    public static final String SHARE_GROUP_MAX_GROUPS_CONFIG = "group.share.max.groups";
    public static final short SHARE_GROUP_MAX_GROUPS_DEFAULT = 10;
    public static final String SHARE_GROUP_MAX_GROUPS_DOC = "The maximum number of share groups.";

    public static final String SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG = "group.share.record.lock.duration.ms";
    public static final int SHARE_GROUP_RECORD_LOCK_DURATION_MS_DEFAULT = 30000;
    public static final String SHARE_GROUP_RECORD_LOCK_DURATION_MS_DOC = "The record acquisition lock duration in milliseconds for share groups.";

    public static final String SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG = "group.share.min.record.lock.duration.ms";
    public static final int SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DEFAULT = 15000;
    public static final String SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DOC = "The record acquisition lock minimum duration in milliseconds for share groups.";

    public static final String SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG = "group.share.max.record.lock.duration.ms";
    public static final int SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DEFAULT = 60000;
    public static final String SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DOC = "The record acquisition lock maximum duration in milliseconds for share groups.";

    public static final String SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG = "share.fetch.purgatory.purge.interval.requests";
    public static final int SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT = 1000;
    public static final String SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC = "The purge interval (in number of requests) of the share fetch request purgatory";

    public static final String SHARE_GROUP_PERSISTER_CLASS_NAME_CONFIG = "group.share.persister.class.name";
    public static final String SHARE_GROUP_PERSISTER_CLASS_NAME_DEFAULT = "org.apache.kafka.server.share.persister.DefaultStatePersister";
    public static final String SHARE_GROUP_PERSISTER_CLASS_NAME_DOC = "The class name of share persister for share group. The class should implement " +
        "the <code>org.apache.kafka.server.share.Persister</code> interface.";

    // Broker temporary configuration to limit the number of records fetched by a share fetch request.
    public static final String SHARE_FETCH_MAX_FETCH_RECORDS_CONFIG = "share.fetch.max.fetch.records";
    public static final int SHARE_FETCH_MAX_FETCH_RECORDS_DEFAULT = Integer.MAX_VALUE;
    public static final String SHARE_FETCH_MAX_FETCH_RECORDS_DOC = "The maximum number of records that can be fetched by a share fetch request.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .defineInternal(SHARE_GROUP_ENABLE_CONFIG, BOOLEAN, SHARE_GROUP_ENABLE_DEFAULT, null, MEDIUM, SHARE_GROUP_ENABLE_DOC)
            .defineInternal(SHARE_FETCH_MAX_FETCH_RECORDS_CONFIG, INT, SHARE_FETCH_MAX_FETCH_RECORDS_DEFAULT, null, HIGH, SHARE_FETCH_MAX_FETCH_RECORDS_DOC)
            .define(SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, INT, SHARE_GROUP_DELIVERY_COUNT_LIMIT_DEFAULT, between(2, 10), MEDIUM, SHARE_GROUP_DELIVERY_COUNT_LIMIT_DOC)
            .define(SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, INT, SHARE_GROUP_RECORD_LOCK_DURATION_MS_DEFAULT, between(1000, 3600000), MEDIUM, SHARE_GROUP_RECORD_LOCK_DURATION_MS_DOC)
            .define(SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, INT, SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DEFAULT, between(1000, 30000), MEDIUM, SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DOC)
            .define(SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, INT, SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DEFAULT, between(30000, 3600000), MEDIUM, SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DOC)
            .define(SHARE_GROUP_MAX_GROUPS_CONFIG, SHORT, SHARE_GROUP_MAX_GROUPS_DEFAULT, between(1, 100), MEDIUM, SHARE_GROUP_MAX_GROUPS_DOC)
            .define(SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG, INT, SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DEFAULT, between(100, 10000), MEDIUM, SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DOC)
            .define(SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG, INT, SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT, MEDIUM, SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC)
            .defineInternal(SHARE_GROUP_PERSISTER_CLASS_NAME_CONFIG, STRING, SHARE_GROUP_PERSISTER_CLASS_NAME_DEFAULT, null, MEDIUM, SHARE_GROUP_PERSISTER_CLASS_NAME_DOC);

    private final boolean isShareGroupEnabled;
    private final int shareGroupPartitionMaxRecordLocks;
    private final int shareGroupDeliveryCountLimit;
    private final short shareGroupMaxGroups;
    private final int shareGroupRecordLockDurationMs;
    private final int shareGroupMaxRecordLockDurationMs;
    private final int shareGroupMinRecordLockDurationMs;
    private final int shareFetchPurgatoryPurgeIntervalRequests;
    private final String shareGroupPersisterClassName;
    private final int shareFetchMaxFetchRecords;

    public ShareGroupConfig(AbstractConfig config) {
        // Share groups are enabled in two cases: 1) The internal configuration to enable it is
        // explicitly set; or 2) the share rebalance protocol is enabled.
        Set<String> protocols = config.getList(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG)
            .stream().map(String::toUpperCase).collect(Collectors.toSet());
        isShareGroupEnabled = config.getBoolean(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG) ||
            protocols.contains(GroupType.SHARE.name());
        shareGroupPartitionMaxRecordLocks = config.getInt(ShareGroupConfig.SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG);
        shareGroupDeliveryCountLimit = config.getInt(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG);
        shareGroupMaxGroups = config.getShort(ShareGroupConfig.SHARE_GROUP_MAX_GROUPS_CONFIG);
        shareGroupRecordLockDurationMs = config.getInt(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG);
        shareGroupMaxRecordLockDurationMs = config.getInt(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG);
        shareGroupMinRecordLockDurationMs = config.getInt(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG);
        shareFetchPurgatoryPurgeIntervalRequests = config.getInt(ShareGroupConfig.SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG);
        shareGroupPersisterClassName = config.getString(ShareGroupConfig.SHARE_GROUP_PERSISTER_CLASS_NAME_CONFIG);
        shareFetchMaxFetchRecords = config.getInt(ShareGroupConfig.SHARE_FETCH_MAX_FETCH_RECORDS_CONFIG);
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

    public short shareGroupMaxGroups() {
        return shareGroupMaxGroups;
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

    public String shareGroupPersisterClassName() {
        return shareGroupPersisterClassName;
    }

    public int shareFetchMaxFetchRecords() {
        return shareFetchMaxFetchRecords;
    }

    private void validate() {
        Utils.require(shareGroupRecordLockDurationMs >= shareGroupMinRecordLockDurationMs,
                String.format("%s must be greater than or equal to %s",
                        SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG));
        Utils.require(shareGroupMaxRecordLockDurationMs >= shareGroupRecordLockDurationMs,
                String.format("%s must be greater than or equal to %s",
                        SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG));
    }

    /**
     * Copy the subset of properties that are relevant to share group.
     */
    public Map<String, Integer> extractShareGroupConfigMap() {
        return Map.of(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, shareGroupRecordLockDurationMs());
    }
}
