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
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class KRaftConfigs {
    /** KRaft mode configs */
    public static final String PROCESS_ROLES_CONFIG = "process.roles";
    public static final String PROCESS_ROLES_DOC = "The roles that this process plays: 'broker', 'controller', or 'broker,controller' if it is both. " +
            "This configuration is only applicable for clusters in KRaft (Kafka Raft) mode (instead of ZooKeeper). Leave this config undefined or empty for ZooKeeper clusters.";

    public static final String INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_CONFIG = "initial.broker.registration.timeout.ms";
    public static final int INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DEFAULT = 60000;
    public static final String INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DOC = "When initially registering with the controller quorum, the number of milliseconds to wait before declaring failure and exiting the broker process.";

    public static final String BROKER_HEARTBEAT_INTERVAL_MS_CONFIG = "broker.heartbeat.interval.ms";
    public static final int BROKER_HEARTBEAT_INTERVAL_MS_DEFAULT = 2000;
    public static final String BROKER_HEARTBEAT_INTERVAL_MS_DOC = "The length of time in milliseconds between broker heartbeats. Used when running in KRaft mode.";

    public static final String BROKER_SESSION_TIMEOUT_MS_CONFIG = "broker.session.timeout.ms";
    public static final int BROKER_SESSION_TIMEOUT_MS_DEFAULT = 9000;
    public static final String BROKER_SESSION_TIMEOUT_MS_DOC = "The length of time in milliseconds that a broker lease lasts if no heartbeats are made. Used when running in KRaft mode.";


    public static final String NODE_ID_CONFIG = "node.id";
    public static final int EMPTY_NODE_ID = -1;
    public static final String NODE_ID_DOC = "The node ID associated with the roles this process is playing when <code>process.roles</code> is non-empty. " +
            "This is required configuration when running in KRaft mode.";

    public static final String METADATA_LOG_DIR_CONFIG = "metadata.log.dir";
    public static final String METADATA_LOG_DIR_DOC = "This configuration determines where we put the metadata log for clusters in KRaft mode. " +
            "If it is not set, the metadata log is placed in the first log directory from log.dirs.";

    public static final String METADATA_SNAPSHOT_MAX_INTERVAL_MS_CONFIG = "metadata.log.max.snapshot.interval.ms";
    public static final long METADATA_SNAPSHOT_MAX_INTERVAL_MS_DEFAULT = TimeUnit.HOURS.toMillis(1);
    public static final String METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG = "metadata.log.max.record.bytes.between.snapshots";
    public static final int METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES = 20 * 1024 * 1024;
    public static final String METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_DOC = "This is the maximum number of bytes in the log between the latest " +
            "snapshot and the high-watermark needed before generating a new snapshot. The default value is " +
            METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES + ". To generate snapshots based on the time elapsed, see the <code>" +
            METADATA_SNAPSHOT_MAX_INTERVAL_MS_CONFIG + "</code> configuration. The Kafka node will generate a snapshot when " +
            "either the maximum time interval is reached or the maximum bytes limit is reached.";
    public static final String METADATA_SNAPSHOT_MAX_INTERVAL_MS_DOC = "This is the maximum number of milliseconds to wait to generate a snapshot " +
            "if there are committed records in the log that are not included in the latest snapshot. A value of zero disables " +
            "time based snapshot generation. The default value is " + METADATA_SNAPSHOT_MAX_INTERVAL_MS_DEFAULT + ". To generate " +
            "snapshots based on the number of metadata bytes, see the <code>" + METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG + "</code> " +
            "configuration. The Kafka node will generate a snapshot when either the maximum time interval is reached or the " +
            "maximum bytes limit is reached.";

    public static final String CONTROLLER_LISTENER_NAMES_CONFIG = "controller.listener.names";
    public static final String CONTROLLER_LISTENER_NAMES_DOC = "A comma-separated list of the names of the listeners used by the controller. This is required " +
            "if running in KRaft mode. When communicating with the controller quorum, the broker will always use the first listener in this list.\n " +
            "Note: The ZooKeeper-based controller should not set this configuration.";

    public static final String SASL_MECHANISM_CONTROLLER_PROTOCOL_CONFIG = "sasl.mechanism.controller.protocol";
    public static final String SASL_MECHANISM_CONTROLLER_PROTOCOL_DOC = "SASL mechanism used for communication with controllers. Default is GSSAPI.";

    public static final String METADATA_LOG_SEGMENT_MIN_BYTES_CONFIG = "metadata.log.segment.min.bytes";
    public static final String METADATA_LOG_SEGMENT_MIN_BYTES_DOC = "Override the minimum size for a single metadata log file. This should be used for testing only.";

    public static final String METADATA_LOG_SEGMENT_BYTES_CONFIG = "metadata.log.segment.bytes";
    public static final String METADATA_LOG_SEGMENT_BYTES_DOC = "The maximum size of a single metadata log file.";

    public static final String METADATA_LOG_SEGMENT_MILLIS_CONFIG = "metadata.log.segment.ms";
    public static final String METADATA_LOG_SEGMENT_MILLIS_DOC = "The maximum time before a new metadata log file is rolled out (in milliseconds).";

    public static final String METADATA_MAX_RETENTION_BYTES_CONFIG = "metadata.max.retention.bytes";
    public static final int METADATA_MAX_RETENTION_BYTES_DEFAULT = 100 * 1024 * 1024;
    public static final String METADATA_MAX_RETENTION_BYTES_DOC = "The maximum combined size of the metadata log and snapshots before deleting old " +
            "snapshots and log files. Since at least one snapshot must exist before any logs can be deleted, this is a soft limit.";

    public static final String METADATA_MAX_RETENTION_MILLIS_CONFIG = "metadata.max.retention.ms";
    public static final String METADATA_MAX_RETENTION_MILLIS_DOC = "The number of milliseconds to keep a metadata log file or snapshot before " +
            "deleting it. Since at least one snapshot must exist before any logs can be deleted, this is a soft limit.";

    public static final String METADATA_MAX_IDLE_INTERVAL_MS_CONFIG = "metadata.max.idle.interval.ms";
    public static final int METADATA_MAX_IDLE_INTERVAL_MS_DEFAULT = 500;
    public static final String METADATA_MAX_IDLE_INTERVAL_MS_DOC = "This configuration controls how often the active " +
            "controller should write no-op records to the metadata partition. If the value is 0, no-op records " +
            "are not appended to the metadata partition. The default value is " + METADATA_MAX_IDLE_INTERVAL_MS_DEFAULT;

    public static final String SERVER_MAX_STARTUP_TIME_MS_CONFIG = "server.max.startup.time.ms";
    public static final long SERVER_MAX_STARTUP_TIME_MS_DEFAULT = Long.MAX_VALUE;
    public static final String SERVER_MAX_STARTUP_TIME_MS_DOC = "The maximum number of milliseconds we will wait for the server to come up. " +
            "By default there is no limit. This should be used for testing only.";

    public static final ConfigDef CONFIG_DEF =  new ConfigDef()
            .define(METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG, LONG, METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES, atLeast(1), HIGH, METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_DOC)
            .define(METADATA_SNAPSHOT_MAX_INTERVAL_MS_CONFIG, LONG, METADATA_SNAPSHOT_MAX_INTERVAL_MS_DEFAULT, atLeast(0), HIGH, METADATA_SNAPSHOT_MAX_INTERVAL_MS_DOC)
            .define(PROCESS_ROLES_CONFIG, LIST, Collections.emptyList(), ConfigDef.ValidList.in("broker", "controller"), HIGH, PROCESS_ROLES_DOC)
            .define(NODE_ID_CONFIG, INT, EMPTY_NODE_ID, null, HIGH, NODE_ID_DOC)
            .define(INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_CONFIG, INT, INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DEFAULT, null, MEDIUM, INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DOC)
            .define(BROKER_HEARTBEAT_INTERVAL_MS_CONFIG, INT, BROKER_HEARTBEAT_INTERVAL_MS_DEFAULT, null, MEDIUM, BROKER_HEARTBEAT_INTERVAL_MS_DOC)
            .define(BROKER_SESSION_TIMEOUT_MS_CONFIG, INT, BROKER_SESSION_TIMEOUT_MS_DEFAULT, null, MEDIUM, BROKER_SESSION_TIMEOUT_MS_DOC)
            .define(CONTROLLER_LISTENER_NAMES_CONFIG, STRING, null, null, HIGH, CONTROLLER_LISTENER_NAMES_DOC)
            .define(SASL_MECHANISM_CONTROLLER_PROTOCOL_CONFIG, STRING, SaslConfigs.DEFAULT_SASL_MECHANISM, null, HIGH, SASL_MECHANISM_CONTROLLER_PROTOCOL_DOC)
            .define(METADATA_LOG_DIR_CONFIG, STRING, null, null, HIGH, METADATA_LOG_DIR_DOC)
            .define(METADATA_LOG_SEGMENT_BYTES_CONFIG, INT, LogConfig.DEFAULT_SEGMENT_BYTES, atLeast(Records.LOG_OVERHEAD), HIGH, METADATA_LOG_SEGMENT_BYTES_DOC)
            .defineInternal(METADATA_LOG_SEGMENT_MIN_BYTES_CONFIG, INT, 8 * 1024 * 1024, atLeast(Records.LOG_OVERHEAD), HIGH, METADATA_LOG_SEGMENT_MIN_BYTES_DOC)
            .define(METADATA_LOG_SEGMENT_MILLIS_CONFIG, LONG, LogConfig.DEFAULT_SEGMENT_MS, null, HIGH, METADATA_LOG_SEGMENT_MILLIS_DOC)
            .define(METADATA_MAX_RETENTION_BYTES_CONFIG, LONG, METADATA_MAX_RETENTION_BYTES_DEFAULT, null, HIGH, METADATA_MAX_RETENTION_BYTES_DOC)
            .define(METADATA_MAX_RETENTION_MILLIS_CONFIG, LONG, LogConfig.DEFAULT_RETENTION_MS, null, HIGH, METADATA_MAX_RETENTION_MILLIS_DOC)
            .define(METADATA_MAX_IDLE_INTERVAL_MS_CONFIG, INT, METADATA_MAX_IDLE_INTERVAL_MS_DEFAULT, atLeast(0), LOW, METADATA_MAX_IDLE_INTERVAL_MS_DOC)
            .defineInternal(SERVER_MAX_STARTUP_TIME_MS_CONFIG, LONG, SERVER_MAX_STARTUP_TIME_MS_DEFAULT, atLeast(0), MEDIUM, SERVER_MAX_STARTUP_TIME_MS_DOC);
}
