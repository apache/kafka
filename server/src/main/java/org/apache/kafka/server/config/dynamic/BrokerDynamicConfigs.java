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

package org.apache.kafka.server.config.dynamic;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.server.config.ReplicationQuotaManagerConfig;
import org.apache.kafka.storage.internals.log.LogConfig;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;

public class BrokerDynamicConfigs {
    // Properties
    public final static String LEADER_REPLICATION_THROTTLED_RATE_PROP = "leader.replication.throttled.rate";
    public final static String FOLLOWER_REPLICATION_THROTTLED_RATE_PROP = "follower.replication.throttled.rate";
    public final static String REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_PROP = "replica.alter.log.dirs.io.max.bytes.per.second";

    // Defaults
    public final static long DEFAULT_REPLICATION_THROTTLED_RATE = ReplicationQuotaManagerConfig.DEFAULT_QUOTA_BYTES_PER_SECOND;

    // Documentation
    public final static String LEADER_REPLICATION_THROTTLED_RATE_DOC = "A long representing the upper bound (bytes/sec) on replication traffic for leaders enumerated in the " +
            "property " + LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG + " (for each topic). This property can be only set dynamically. It is suggested that the " +
            "limit be kept above 1MB/s for accurate behaviour.";
    public final static String FOLLOWER_REPLICATION_THROTTLED_RATE_DOC = "A long representing the upper bound (bytes/sec) on replication traffic for followers enumerated in the " +
            "property " + LogConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG + " (for each topic). This property can be only set dynamically. It is suggested that the " +
            "limit be kept above 1MB/s for accurate behaviour.";
    public final static String REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_DOC = "A long representing the upper bound (bytes/sec) on disk IO used for moving replica between log directories on the same broker. " +
            "This property can be only set dynamically. It is suggested that the limit be kept above 1MB/s for accurate behaviour.";

    // Definitions
    public final static ConfigDef BROKER_CONFIG_DEF = new ConfigDef()
            // Round minimum value down, to make it easier for users.
            .define(LEADER_REPLICATION_THROTTLED_RATE_PROP, LONG, DEFAULT_REPLICATION_THROTTLED_RATE, atLeast(0), MEDIUM, LEADER_REPLICATION_THROTTLED_RATE_DOC)
            .define(FOLLOWER_REPLICATION_THROTTLED_RATE_PROP, LONG, DEFAULT_REPLICATION_THROTTLED_RATE, atLeast(0), MEDIUM, FOLLOWER_REPLICATION_THROTTLED_RATE_DOC)
            .define(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_PROP, LONG, DEFAULT_REPLICATION_THROTTLED_RATE, atLeast(0), MEDIUM, REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_DOC);
}
