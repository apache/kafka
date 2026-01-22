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
package org.apache.kafka.jmh.util;

import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.test.TestUtils;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class BenchmarkConfigUtils {

    public static Properties createDummyBrokerConfig() {
        Properties props = new Properties();

        props.put(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true");
        props.put(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true");
        props.setProperty(KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG, String.valueOf(TimeUnit.MINUTES.toMillis(10)));
        props.put(KRaftConfigs.NODE_ID_CONFIG, "0");
        props.put(ServerConfigs.BROKER_ID_CONFIG, "0");

        props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "PLAINTEXT://localhost:9092");
        props.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:9092");
        props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER");
        props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT");

        File dir = TestUtils.tempDirectory();
        props.put(ServerLogConfigs.LOG_DIR_CONFIG, dir.getAbsolutePath());

        props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker");
        props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, "0@localhost:0");

        props.put(ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_CONFIG, "1500");
        props.put(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG, "1500");

        props.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "true");
        props.put(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, "true");

        props.put(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG, "1000");
        props.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, "2097152");
        props.put(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, "100");
        props.put(ServerLogConfigs.NUM_PARTITIONS_CONFIG, "1");
        props.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "1");

        props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "5");
        props.put(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0");

        props.put(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG, "2");
        props.put(ServerConfigs.BACKGROUND_THREADS_CONFIG, "2");

        return props;
    }
}
