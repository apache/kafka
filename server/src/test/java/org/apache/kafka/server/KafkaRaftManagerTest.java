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
package org.apache.kafka.server;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.SslChannelBuilder;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.Endpoints;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.raft.MetadataLogConfig;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.raft.internals.StringSerde;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.storage.internals.log.LogManager;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class KafkaRaftManagerTest {

    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("__raft_id_test", 0);

    private static Properties createProps(
        Set<ProcessRole> processRoles,
        int nodeId,
        List<Path> logDirs,
        Path metadataDir
    ) {
        Properties props = new Properties();
        if (!logDirs.isEmpty()) {
            props.setProperty(ServerLogConfigs.LOG_DIR_CONFIG, logDirs.get(0).toString());
        }
        if (metadataDir != null) {
            props.setProperty(MetadataLogConfig.METADATA_LOG_DIR_CONFIG, metadataDir.toString());
        }
        props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG,
            processRoles.stream().map(ProcessRole::toString).collect(Collectors.joining(",")));
        props.setProperty(KRaftConfigs.NODE_ID_CONFIG, String.valueOf(nodeId));
        props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL");

        if (processRoles.contains(ProcessRole.BrokerRole)) {
            props.setProperty(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "PLAINTEXT");
            if (processRoles.contains(ProcessRole.ControllerRole)) {
                props.setProperty(SocketServerConfigs.LISTENERS_CONFIG,
                    "PLAINTEXT://localhost:9092,SSL://localhost:9093");
                props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, nodeId + "@localhost:9093");
            } else {
                int voterId = nodeId + 1;
                props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, voterId + "@localhost:9093");
            }
        } else if (processRoles.contains(ProcessRole.ControllerRole)) {
            props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "SSL://localhost:9093");
            props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, nodeId + "@localhost:9093");
        }

        return props;
    }

    private static AbstractKafkaConfig createConfig(
        Set<ProcessRole> processRoles,
        int nodeId,
        List<Path> logDirs,
        Path metadataDir
    ) {
        return new AbstractKafkaConfig(
            AbstractKafkaConfig.CONFIG_DEF, createProps(processRoles, nodeId, logDirs, metadataDir), Map.of(), false
        ) { };
    }

    private static KafkaRaftManager<String> createRaftManager(
        AbstractKafkaConfig config
    ) throws IOException {
        Endpoints endpoints = config.processRoles().contains(ProcessRole.ControllerRole)
            ? Endpoints.fromInetSocketAddresses(Map.of(
                new ListenerName("SSL"), InetSocketAddress.createUnresolved("localhost", 9093)))
            : Endpoints.empty();

        QuorumConfig quorumConfig = new QuorumConfig(config);

        return new KafkaRaftManager<>(
            Uuid.randomUuid().toString(),
            config,
            Uuid.randomUuid(),
            new StringSerde(),
            TOPIC_PARTITION,
            new Uuid(0L, 2L),
            Time.SYSTEM,
            new Metrics(Time.SYSTEM),
            ignored -> { },
            Optional.empty(),
            CompletableFuture.completedFuture(QuorumConfig.parseVoterConnections(quorumConfig.voters())),
            QuorumConfig.parseBootstrapServers(quorumConfig.bootstrapServers()),
            endpoints,
            mock(FaultHandler.class)
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"broker", "controller", "broker,controller"})
    void testNodeIdPresent(String processRoles) throws IOException {
        Set<ProcessRole> roles = parseRoles(processRoles);
        File logDir = TestUtils.tempDirectory();
        int nodeId = 1;
        try {
            AbstractKafkaConfig config = createConfig(roles, nodeId, List.of(logDir.toPath()), null);
            KafkaRaftManager<String> raftManager = createRaftManager(config);
            try {
                assertEquals(nodeId, raftManager.client().nodeId().getAsInt());
            } finally {
                raftManager.shutdown();
            }
        } finally {
            Utils.delete(logDir);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"metadata-only", "log-only", "both"})
    void testLogDirLockWhenControllerOnly(String dirType) throws IOException {
        List<Path> logDirs = dirType.equals("metadata-only")
            ? List.of()
            : List.of(TestUtils.tempDirectory().toPath());

        Path metadataDir = dirType.equals("log-only")
            ? null
            : TestUtils.tempDirectory().toPath();

        int nodeId = 1;
        try {
            AbstractKafkaConfig config = createConfig(Set.of(ProcessRole.ControllerRole), nodeId, logDirs, metadataDir);
            KafkaRaftManager<String> raftManager = createRaftManager(config);
            Path lockDir = metadataDir != null ? metadataDir : logDirs.get(0);
            Path lockPath = lockDir.resolve(LogManager.LOCK_FILE_NAME);
            try {
                assertTrue(fileLocked(lockPath));
            } finally {
                raftManager.shutdown();
            }
            assertFalse(fileLocked(lockPath));
        } finally {
            for (Path p : logDirs) {
                Utils.delete(p.toFile());
            }
            if (metadataDir != null) {
                Utils.delete(metadataDir.toFile());
            }
        }
    }

    @Test
    void testLogDirLockWhenBrokerOnlyWithSeparateMetadataDir() throws IOException {
        Path logDir = TestUtils.tempDirectory().toPath();
        Path metadataDir = TestUtils.tempDirectory().toPath();
        int nodeId = 1;
        try {
            AbstractKafkaConfig config = createConfig(Set.of(ProcessRole.BrokerRole), nodeId, List.of(logDir), metadataDir);
            KafkaRaftManager<String> raftManager = createRaftManager(config);
            Path lockPath = metadataDir.resolve(LogManager.LOCK_FILE_NAME);
            try {
                assertTrue(fileLocked(lockPath));
            } finally {
                raftManager.shutdown();
            }
            assertFalse(fileLocked(lockPath));
        } finally {
            Utils.delete(logDir.toFile());
            Utils.delete(metadataDir.toFile());
        }
    }

    @Test
    void testRaftManagerRegistersSslChannelBuilderForReconfiguration() throws IOException {
        Path logDir = TestUtils.tempDirectory().toPath();
        Path metadataDir = TestUtils.tempDirectory().toPath();
        try {
            List<Reconfigurable> registered = new ArrayList<>();
            Properties props = createProps(Set.of(ProcessRole.ControllerRole), 1, List.of(logDir), metadataDir);
            AbstractKafkaConfig config = new AbstractKafkaConfig(AbstractKafkaConfig.CONFIG_DEF, props, Map.of(), false) {
                @Override
                public void addReconfigurable(Reconfigurable reconfigurable) {
                    registered.add(reconfigurable);
                }
            };

            KafkaRaftManager<String> raftManager = createRaftManager(config);
            try {
                assertTrue(
                    registered.stream().anyMatch(r -> r instanceof SslChannelBuilder),
                    "RaftManager should register its SSL channel builder as a Reconfigurable on the config"
                );
            } finally {
                raftManager.shutdown();
            }
        } finally {
            Utils.delete(logDir.toFile());
            Utils.delete(metadataDir.toFile());
        }
    }

    @Test
    void testFailedConstructionKeepsExistingLock() throws IOException {
        Path metadataDir = TestUtils.tempDirectory().toPath();
        try {
            AbstractKafkaConfig config = createConfig(
                Set.of(ProcessRole.ControllerRole), 1, List.of(), metadataDir);
            KafkaRaftManager<String> first = createRaftManager(config);
            Path lockPath = metadataDir.resolve(LogManager.LOCK_FILE_NAME);
            try {
                assertThrows(KafkaException.class, () -> createRaftManager(config));
                assertTrue(fileLocked(lockPath));
            } finally {
                first.shutdown();
            }
            assertFalse(fileLocked(lockPath));
        } finally {
            Utils.delete(metadataDir.toFile());
        }
    }

    @Test
    void testConstructorReleasesLockOnFailure() throws IOException {
        Path metadataDir = TestUtils.tempDirectory().toPath();
        try {
            Properties props = createProps(
                Set.of(ProcessRole.ControllerRole), 1, List.of(), metadataDir);
            AbstractKafkaConfig config = new AbstractKafkaConfig(
                AbstractKafkaConfig.CONFIG_DEF, props, Map.of(), false
            ) {
                @Override
                public void addReconfigurable(Reconfigurable reconfigurable) {
                    throw new IllegalStateException("injected failure");
                }
            };
            assertThrows(IllegalStateException.class, () -> createRaftManager(config));
            assertFalse(fileLocked(metadataDir.resolve(LogManager.LOCK_FILE_NAME)));
        } finally {
            Utils.delete(metadataDir.toFile());
        }
    }

    private static Set<ProcessRole> parseRoles(String processRoles) {
        Set<ProcessRole> roles = new HashSet<>();
        if (processRoles.contains("broker")) {
            roles.add(ProcessRole.BrokerRole);
        }
        if (processRoles.contains("controller")) {
            roles.add(ProcessRole.ControllerRole);
        }
        return roles;
    }

    private static boolean fileLocked(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            try {
                FileLock lock = channel.tryLock();
                if (lock != null) {
                    lock.close();
                }
                return false;
            } catch (OverlappingFileLockException e) {
                return true;
            }
        }
    }
}
