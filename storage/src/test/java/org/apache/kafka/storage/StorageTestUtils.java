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
package org.apache.kafka.storage;

import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.internals.log.CleanerConfig;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Test utilities for the storage module.
 *
 * <p>This class intentionally duplicates broker config helpers found in other modules
 * (e.g., {@code kafka.utils.TestUtils} in core, {@code BenchmarkConfigUtils} in jmh-benchmarks).
 * Per the TestUtils consolidation policy (KAFKA-20350), each module should own its test
 * infrastructure rather than pulling from a shared test-common module, to avoid bloating
 * test-common and to keep the dependency graph clean.
 *
 * @see <a href="https://github.com/apache/kafka/pull/21679#issuecomment-4113577448">KAFKA-20350 rationale</a>
 */
public class StorageTestUtils {

    /**
     * Creates a minimal {@link Properties} suitable for constructing a {@code KafkaConfig}
     * in tests that require a valid broker configuration but do not need full integration setup.
     */
    public static Properties createDummyBrokerConfig() {
        Properties props = new Properties();

        props.put(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true");
        props.put(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true");
        // TODO: I wonder if we are okay with having some of the configuration hardcoded just for sake of few tests...
        // Use string literals for KRaft/Quorum/GroupCoordinator configs to avoid
        // pulling in raft and group-coordinator module dependencies.
        props.setProperty("server.max.startup.time.ms", String.valueOf(TimeUnit.MINUTES.toMillis(10)));
        props.put("node.id", "0");
        props.put(ServerConfigs.BROKER_ID_CONFIG, "0");

        props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "PLAINTEXT://localhost:9092");
        props.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093");
        props.put("controller.listener.names", "CONTROLLER");
        props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT");

        File dir = org.apache.kafka.test.TestUtils.tempDirectory();
        props.put(ServerLogConfigs.LOG_DIR_CONFIG, dir.getAbsolutePath());

        props.put("process.roles", "broker,controller");
        props.put("controller.quorum.voters", "0@localhost:0");

        props.put(ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_CONFIG, "1500");
        props.put(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG, "1500");

        props.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "true");
        props.put(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, "true");

        props.put(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG, "1000");
        props.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, "2097152");
        props.put(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, "100");
        props.put(ServerLogConfigs.NUM_PARTITIONS_CONFIG, "1");
        props.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "1");

        props.put("offsets.topic.replication.factor", "1");
        props.put("offsets.topic.num.partitions", "5");
        props.put("group.initial.rebalance.delay.ms", "0");

        props.put(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG, "2");
        props.put(ServerConfigs.BACKGROUND_THREADS_CONFIG, "2");

        return props;
    }
}