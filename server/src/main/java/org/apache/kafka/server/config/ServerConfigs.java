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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.storage.internals.log.LogConfig;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class ServerConfigs {
    /** ********* General Configuration ***********/
    public static final String BROKER_ID_CONFIG = "broker.id";
    public static final int BROKER_ID_DEFAULT = -1;
    public static final String BROKER_ID_DOC = "The broker id for this server.";

    public static final String MESSAGE_MAX_BYTES_CONFIG = "message.max.bytes";
    public static final String MESSAGE_MAX_BYTES_DOC = TopicConfig.MAX_MESSAGE_BYTES_DOC +
            "This can be set per topic with the topic level <code>" + TopicConfig.MAX_MESSAGE_BYTES_CONFIG + "</code> config.";

    public static final String NUM_IO_THREADS_CONFIG = "num.io.threads";
    public static final int NUM_IO_THREADS_DEFAULT = 8;
    public static final String NUM_IO_THREADS_DOC = "The number of threads that the server uses for processing requests, which may include disk I/O";

    public static final String BACKGROUND_THREADS_CONFIG = "background.threads";
    public static final int BACKGROUND_THREADS_DEFAULT = 10;
    public static final String BACKGROUND_THREADS_DOC = "The number of threads to use for various background processing tasks";

    public static final String NUM_REPLICA_ALTER_LOG_DIRS_THREADS_CONFIG = "num.replica.alter.log.dirs.threads";
    public static final String NUM_REPLICA_ALTER_LOG_DIRS_THREADS_DOC = "The number of threads that can move replicas between log directories, which may include disk I/O";

    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    public static final int REQUEST_TIMEOUT_MS_DEFAULT = 30000;
    public static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;

    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;
    public static final long DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS;
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC;

    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;
    public static final long SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS;
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC;

    public static final String DELETE_TOPIC_ENABLE_CONFIG = "delete.topic.enable";
    public static final boolean DELETE_TOPIC_ENABLE_DEFAULT = true;
    public static final String DELETE_TOPIC_ENABLE_DOC = "Enables delete topic. Delete topic through the admin tool will have no effect if this config is turned off";

    public static final String COMPRESSION_TYPE_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.COMPRESSION_TYPE_CONFIG);
    public static final String COMPRESSION_TYPE_DOC = "Specify the final compression type for a given topic. This configuration accepts the standard compression codecs " +
            "('gzip', 'snappy', 'lz4', 'zstd'). It additionally accepts 'uncompressed' which is equivalent to no compression; and " +
            "'producer' which means retain the original compression codec set by the producer.";

    public static final String COMPRESSION_GZIP_LEVEL_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.COMPRESSION_GZIP_LEVEL_CONFIG);
    public static final String COMPRESSION_GZIP_LEVEL_DOC = "The compression level to use if " + COMPRESSION_TYPE_CONFIG + " is set to 'gzip'.";
    public static final String COMPRESSION_LZ4_LEVEL_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.COMPRESSION_LZ4_LEVEL_CONFIG);
    public static final String COMPRESSION_LZ4_LEVEL_DOC = "The compression level to use if " + COMPRESSION_TYPE_CONFIG + " is set to 'lz4'.";
    public static final String COMPRESSION_ZSTD_LEVEL_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.COMPRESSION_ZSTD_LEVEL_CONFIG);
    public static final String COMPRESSION_ZSTD_LEVEL_DOC = "The compression level to use if " + COMPRESSION_TYPE_CONFIG + " is set to 'zstd'.";

    /***************** rack configuration *************/
    public static final String BROKER_RACK_CONFIG = "broker.rack";
    public static final String BROKER_RACK_DOC = "Rack of the broker. This will be used in rack aware replication assignment for fault tolerance. Examples: <code>RACK1</code>, <code>us-east-1d</code>";

    /** ********* Controlled shutdown configuration ***********/
    public static final String CONTROLLED_SHUTDOWN_ENABLE_CONFIG = "controlled.shutdown.enable";
    public static final boolean CONTROLLED_SHUTDOWN_ENABLE_DEFAULT = true;
    public static final String CONTROLLED_SHUTDOWN_ENABLE_DOC = "Enable controlled shutdown of the server.";

    /** ********* Fetch Configuration **************/
    public static final String MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_CONFIG = "max.incremental.fetch.session.cache.slots";
    public static final int MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_DEFAULT = 1000;
    public static final String MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_DOC = "The maximum number of total incremental fetch sessions that we will maintain. FetchSessionCache is sharded into 8 shards and the limit is equally divided among all shards. Sessions are allocated to each shard in round-robin. Only entries within a shard are considered eligible for eviction.";

    public static final String FETCH_MAX_BYTES_CONFIG = "fetch.max.bytes";
    public static final int FETCH_MAX_BYTES_DEFAULT = 55 * 1024 * 1024;
    public static final String FETCH_MAX_BYTES_DOC = "The maximum number of bytes we will return for a fetch request. Must be at least 1024.";

    /** ********* Request Limit Configuration **************/
    public static final String MAX_REQUEST_PARTITION_SIZE_LIMIT_CONFIG = "max.request.partition.size.limit";
    public static final int MAX_REQUEST_PARTITION_SIZE_LIMIT_DEFAULT = 2000;
    public static final String MAX_REQUEST_PARTITION_SIZE_LIMIT_DOC = "The maximum number of partitions can be served in one request.";

    /** Internal Configurations **/
    public static final String UNSTABLE_API_VERSIONS_ENABLE_CONFIG = "unstable.api.versions.enable";
    public static final String UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG = "unstable.feature.versions.enable";

    /************* Authorizer Configuration ***********/
    public static final String AUTHORIZER_CLASS_NAME_CONFIG = "authorizer.class.name";
    public static final String AUTHORIZER_CLASS_NAME_DEFAULT = "";
    public static final String AUTHORIZER_CLASS_NAME_DOC = "The fully qualified name of a class that implements <code>" +
           Authorizer.class.getName() + "</code> interface, which is used by the broker for authorization.";
    public static final String EARLY_START_LISTENERS_CONFIG = "early.start.listeners";
    public static final String EARLY_START_LISTENERS_DOC = "A comma-separated list of listener names which may be started before the authorizer has finished " +
            "initialization. This is useful when the authorizer is dependent on the cluster itself for bootstrapping, as is the case for " +
            "the StandardAuthorizer (which stores ACLs in the metadata log.) By default, all listeners included in controller.listener.names " +
            "will also be early start listeners. A listener should not appear in this list if it accepts external traffic.";
    public static final ConfigDef CONFIG_DEF =  new ConfigDef()
            .define(BROKER_ID_CONFIG, INT, BROKER_ID_DEFAULT, HIGH, BROKER_ID_DOC)
            .define(MESSAGE_MAX_BYTES_CONFIG, INT, LogConfig.DEFAULT_MAX_MESSAGE_BYTES, atLeast(0), HIGH, MESSAGE_MAX_BYTES_DOC)
            .define(NUM_IO_THREADS_CONFIG, INT, NUM_IO_THREADS_DEFAULT, atLeast(1), HIGH, NUM_IO_THREADS_DOC)
            .define(NUM_REPLICA_ALTER_LOG_DIRS_THREADS_CONFIG, INT, null, HIGH, NUM_REPLICA_ALTER_LOG_DIRS_THREADS_DOC)
            .define(BACKGROUND_THREADS_CONFIG, INT, BACKGROUND_THREADS_DEFAULT, atLeast(1), HIGH, BACKGROUND_THREADS_DOC)
            .define(REQUEST_TIMEOUT_MS_CONFIG, INT, REQUEST_TIMEOUT_MS_DEFAULT, HIGH, REQUEST_TIMEOUT_MS_DOC)
            .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, LONG, DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS, MEDIUM, SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
            .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, LONG, SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS, MEDIUM, SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
            /************* Authorizer Configuration ***********/
            .define(AUTHORIZER_CLASS_NAME_CONFIG, STRING, AUTHORIZER_CLASS_NAME_DEFAULT, new ConfigDef.NonNullValidator(), LOW, AUTHORIZER_CLASS_NAME_DOC)
            .define(EARLY_START_LISTENERS_CONFIG, STRING, null,  HIGH, EARLY_START_LISTENERS_DOC)
            /************ Rack Configuration ******************/
            .define(BROKER_RACK_CONFIG, STRING, null, MEDIUM, BROKER_RACK_DOC)
            /** ********* Controlled shutdown configuration ***********/
            .define(CONTROLLED_SHUTDOWN_ENABLE_CONFIG, BOOLEAN, CONTROLLED_SHUTDOWN_ENABLE_DEFAULT, MEDIUM, CONTROLLED_SHUTDOWN_ENABLE_DOC)
            .define(DELETE_TOPIC_ENABLE_CONFIG, BOOLEAN, DELETE_TOPIC_ENABLE_DEFAULT, HIGH, DELETE_TOPIC_ENABLE_DOC)
            .define(COMPRESSION_TYPE_CONFIG, STRING, LogConfig.DEFAULT_COMPRESSION_TYPE, ConfigDef.ValidString.in(BrokerCompressionType.names().toArray(new String[0])), HIGH, COMPRESSION_TYPE_DOC)
            .define(COMPRESSION_GZIP_LEVEL_CONFIG, INT, CompressionType.GZIP.defaultLevel(), CompressionType.GZIP.levelValidator(), MEDIUM, COMPRESSION_GZIP_LEVEL_DOC)
            .define(COMPRESSION_LZ4_LEVEL_CONFIG, INT, CompressionType.LZ4.defaultLevel(), CompressionType.LZ4.levelValidator(), MEDIUM, COMPRESSION_LZ4_LEVEL_DOC)
            .define(COMPRESSION_ZSTD_LEVEL_CONFIG, INT, CompressionType.ZSTD.defaultLevel(), CompressionType.ZSTD.levelValidator(), MEDIUM, COMPRESSION_ZSTD_LEVEL_DOC)
            /** ********* Fetch Configuration **************/
            .define(MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_CONFIG, INT, MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_DEFAULT, atLeast(0), MEDIUM, MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_DOC)
            .define(FETCH_MAX_BYTES_CONFIG, INT, FETCH_MAX_BYTES_DEFAULT, atLeast(1024), MEDIUM, FETCH_MAX_BYTES_DOC)

            /** ********* Request Limit Configuration ***********/
            .define(MAX_REQUEST_PARTITION_SIZE_LIMIT_CONFIG, INT, MAX_REQUEST_PARTITION_SIZE_LIMIT_DEFAULT, atLeast(1), MEDIUM, MAX_REQUEST_PARTITION_SIZE_LIMIT_DOC)
            /** Internal Configurations **/
            // This indicates whether unreleased APIs should be advertised by this node.
            .defineInternal(UNSTABLE_API_VERSIONS_ENABLE_CONFIG, BOOLEAN, false, HIGH)
            // This indicates whether unreleased MetadataVersions should be enabled on this node.
            .defineInternal(UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, BOOLEAN, false, HIGH);
}
