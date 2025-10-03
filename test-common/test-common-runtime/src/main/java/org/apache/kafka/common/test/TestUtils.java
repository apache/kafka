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
package org.apache.kafka.common.test;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;

public class TestUtils {
    private static final Logger log = LoggerFactory.getLogger(TestUtils.class);

    public static final Random SEEDED_RANDOM = new Random(192348092834L);
    
    public static final String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    public static final String DIGITS = "0123456789";
    public static final String LETTERS_AND_DIGITS = LETTERS + DIGITS;

    private static final long DEFAULT_POLL_INTERVAL_MS = 100;
    private static final long DEFAULT_MAX_WAIT_MS = 15_000;
    private static final Random RANDOM = new Random();

    public static File tempFile() throws IOException {
        final File file = Files.createTempFile("kafka", ".tmp").toFile();
        file.deleteOnExit();
        return file;
    }

    public static String randomString(final int len) {
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i < len; i++)
            b.append(LETTERS_AND_DIGITS.charAt(SEEDED_RANDOM.nextInt(LETTERS_AND_DIGITS.length())));
        return b.toString();
    }

    public static File tempDirectory() {
        final File file;
        String prefix = "kafka-";
        try {
            file = Files.createTempDirectory(prefix).toFile();
        } catch (final IOException ex) {
            throw new RuntimeException("Failed to create a temp dir", ex);
        }

        Exit.addShutdownHook("delete-temp-file-shutdown-hook", () -> {
            try {
                Utils.delete(file);
            } catch (IOException e) {
                log.error("Error deleting {}", file.getAbsolutePath(), e);
            }
        });

        return file;
    }

    public static File tempRelativeDir(String parent) {
        File file = new File(parent, "kafka-" + SEEDED_RANDOM.nextInt(1000000));
        file.mkdirs();
        file.deleteOnExit();
        return file;
    }

    public static void waitForCondition(final Supplier<Boolean> testCondition, final String conditionDetails) throws InterruptedException {
        waitForCondition(testCondition, DEFAULT_MAX_WAIT_MS, () -> conditionDetails);
    }

    public static void waitForCondition(final Supplier<Boolean> testCondition,
                                        final long maxWaitMs,
                                        final Supplier<String> conditionDetails) throws InterruptedException {
        final long expectedEnd = System.currentTimeMillis() + maxWaitMs;

        while (true) {
            try {
                if (testCondition.get()) {
                    return;
                }
                String conditionDetail = conditionDetails.get() == null ? "" : conditionDetails.get();
                throw new TimeoutException("Condition not met: " + conditionDetail);
            } catch (final AssertionError t) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw t;
                }
            } catch (final Exception e) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw new AssertionError(format("Assertion failed with an exception after %s ms", maxWaitMs), e);
                }
            }
            Thread.sleep(Math.min(DEFAULT_POLL_INTERVAL_MS, maxWaitMs));
        }
    }

    public static void waitForCondition(final Supplier<Boolean> testCondition,
                                        final long maxWaitMs,
                                        String conditionDetails) throws InterruptedException {
        waitForCondition(testCondition, maxWaitMs, () -> conditionDetails);
    }

    public static File randomPartitionLogDir(File parentDir) {
        int attempts = 1000;
        while (attempts > 0) {
            File f = new File(parentDir, "kafka-" + RANDOM.nextInt(1000000));
            if (f.mkdir()) {
                f.deleteOnExit();
                return f;
            }
            attempts--;
        }
        throw new RuntimeException("Failed to create directory after 1000 attempts");
    }

    public static Properties createBrokerConfig(int nodeId, int port) {
        return new BrokerConfigBuilder(nodeId).withPort(port).build();
    }

    public static MemoryRecords singletonRecords(byte[] value, byte[] key) {
        return singletonRecords(value, key, Compression.NONE, RecordBatch.NO_TIMESTAMP, RecordBatch.CURRENT_MAGIC_VALUE);
    }

    public static MemoryRecords singletonRecords(byte[] value, long timestamp) {
        return singletonRecords(value, null, Compression.NONE, timestamp, RecordBatch.CURRENT_MAGIC_VALUE);
    }

    public static MemoryRecords singletonRecords(
            byte[] value
    ) {
        return records(List.of(new SimpleRecord(RecordBatch.NO_TIMESTAMP, null, value)),
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                0,
                RecordBatch.NO_PARTITION_LEADER_EPOCH
        );
    }

    public static MemoryRecords singletonRecords(
            byte[] value,
            byte[] key,
            Compression codec,
            long timestamp,
            byte magicValue
            ) {
        return records(List.of(new SimpleRecord(timestamp, key, value)),
                magicValue, codec,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                0,
                RecordBatch.NO_PARTITION_LEADER_EPOCH
        );
    }

    public static MemoryRecords singletonRecords(byte[] value, byte[] key, long timestamp) {
        return singletonRecords(value, key, Compression.NONE, timestamp, RecordBatch.CURRENT_MAGIC_VALUE);
    }

    public static MemoryRecords records(List<SimpleRecord> records) {
        return records(records, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, 0L, RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    public static MemoryRecords records(List<SimpleRecord> records, long baseOffset) {
        return records(records, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, baseOffset, RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    public static MemoryRecords records(List<SimpleRecord> records, long baseOffset, int partitionLeaderEpoch) {
        return records(records, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, baseOffset, partitionLeaderEpoch);
    }

    public static MemoryRecords records(List<SimpleRecord> records, byte magicValue, Compression compression) {
        return records(records, magicValue, compression, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, 0L, RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    public static MemoryRecords records(List<SimpleRecord> records,
                                        byte magicValue,
                                        Compression compression,
                                        long producerId,
                                        short producerEpoch,
                                        int sequence,
                                        long baseOffset,
                                        int partitionLeaderEpoch) {
        ByteBuffer buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records));
        MemoryRecordsBuilder builder = MemoryRecords.builder(buf, magicValue, compression, TimestampType.CREATE_TIME, baseOffset,
            System.currentTimeMillis(), producerId, producerEpoch, sequence, false, partitionLeaderEpoch);
        for (SimpleRecord record : records) {
            builder.append(record);
        }
        return builder.build();
    }

    public static class BrokerConfigBuilder {
        private final int nodeId;
        private boolean enableControlledShutdown = true;
        private boolean enableDeleteTopic = true;
        private int port = -1;
        private Optional<SecurityProtocol> interBrokerSecurityProtocol = Optional.empty();
        private Optional<File> trustStoreFile = Optional.empty();
        private Optional<Properties> saslProperties = Optional.empty();
        private boolean enablePlaintext = true;
        private boolean enableSaslPlaintext = false;
        private int saslPlaintextPort = -1;
        private boolean enableSsl = false;
        private int sslPort = -1;
        private boolean enableSaslSsl = false;
        private int saslSslPort = -1;
        private Optional<String> rack = Optional.empty();
        private int logDirCount = 1;
        private int numPartitions = 1;
        private short defaultReplicationFactor = 1;
        private boolean enableFetchFromFollower = false;

        public BrokerConfigBuilder(int nodeId) {
            this.nodeId = nodeId;
        }

        public BrokerConfigBuilder withPort(int port) {
            this.enablePlaintext = true;
            this.port = port;
            return this;
        }

        public BrokerConfigBuilder withSsl(int port, File trustStoreFile) {
            this.enableSsl = true;
            this.sslPort = port;
            this.trustStoreFile = Optional.of(trustStoreFile);
            return this;
        }

        public Properties build() {
            List<Map.Entry<SecurityProtocol, Integer>> protocolAndPorts = new ArrayList<>();

            if (enablePlaintext || (interBrokerSecurityProtocol.isPresent() && interBrokerSecurityProtocol.get() == SecurityProtocol.PLAINTEXT))
                protocolAndPorts.add(new AbstractMap.SimpleEntry<>(SecurityProtocol.PLAINTEXT, port));
            if (enableSsl || (interBrokerSecurityProtocol.isPresent() && interBrokerSecurityProtocol.get() == SecurityProtocol.SSL))
                protocolAndPorts.add(new AbstractMap.SimpleEntry<>(SecurityProtocol.SSL, sslPort));
            if (enableSaslPlaintext || (interBrokerSecurityProtocol.isPresent() && interBrokerSecurityProtocol.get() == SecurityProtocol.SASL_PLAINTEXT))
                protocolAndPorts.add(new AbstractMap.SimpleEntry<>(SecurityProtocol.SASL_PLAINTEXT, saslPlaintextPort));
            if (enableSaslSsl || (interBrokerSecurityProtocol.isPresent() && interBrokerSecurityProtocol.get() == SecurityProtocol.SASL_SSL))
                protocolAndPorts.add(new AbstractMap.SimpleEntry<>(SecurityProtocol.SASL_SSL, saslSslPort));

            String listeners = protocolAndPorts.stream()
                    .map(entry -> String.format("%s://localhost:%d", entry.getKey().name(), entry.getValue()))
                    .collect(Collectors.joining(","));

            Properties props = new Properties();
            props.put(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true");
            props.put(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true");
            props.put(KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG, String.valueOf(TimeUnit.MINUTES.toMillis(10)));
            props.put(KRaftConfigs.NODE_ID_CONFIG, String.valueOf(nodeId));
            props.put(ServerConfigs.BROKER_ID_CONFIG, String.valueOf(nodeId));
            props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, listeners);
            props.put(SocketServerConfigs.LISTENERS_CONFIG, listeners);
            props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER");

            String securityProtocolMap = protocolAndPorts.stream()
                    .map(entry -> String.format("%s:%s", entry.getKey().name(), entry.getKey().name()))
                    .collect(Collectors.joining(",")) + ",CONTROLLER:PLAINTEXT";
            props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, securityProtocolMap);

            if (logDirCount > 1) {
                String logDirs = IntStream.range(0, logDirCount)
                        .mapToObj(i -> tempDirectory().getAbsolutePath())
                        .collect(Collectors.joining(","));
                props.put(ServerLogConfigs.LOG_DIRS_CONFIG, logDirs);
            } else {
                props.put(ServerLogConfigs.LOG_DIR_CONFIG, tempDirectory().getAbsolutePath());
            }

            props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker");
            props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, "1000@localhost:0");
            props.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, String.valueOf(enableControlledShutdown));
            props.put(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, String.valueOf(enableDeleteTopic));

            rack.ifPresent(r -> props.put(ServerConfigs.BROKER_RACK_CONFIG, r));

            try {
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            interBrokerSecurityProtocol.ifPresent(protocol ->
                    props.put(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG, protocol.name()));

            props.put(ServerLogConfigs.NUM_PARTITIONS_CONFIG, String.valueOf(numPartitions));
            props.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, String.valueOf(defaultReplicationFactor));

            if (enableFetchFromFollower) {
                props.put(ServerConfigs.BROKER_RACK_CONFIG, String.valueOf(nodeId));
                props.put(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG, "org.apache.kafka.common.replica.RackAwareReplicaSelector");
            }

            return props;
        }
    }
}
