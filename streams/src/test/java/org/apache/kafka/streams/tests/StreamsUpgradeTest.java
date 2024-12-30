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
package org.apache.kafka.streams.tests;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor;
import org.apache.kafka.streams.processor.internals.TaskManager;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration;
import org.apache.kafka.streams.processor.internals.assignment.LegacySubscriptionInfoSerde;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.apache.kafka.streams.tests.SmokeTestUtil.intSerde;
import static org.apache.kafka.streams.tests.SmokeTestUtil.stringSerde;

public class StreamsUpgradeTest {

    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("StreamsUpgradeTest requires one argument (properties-file) but no provided: ");
        }
        final String propFileName = args.length > 0 ? args[0] : null;

        final Properties streamsProperties = Utils.loadProps(propFileName);

        System.out.println("StreamsTest instance started (StreamsUpgradeTest trunk)");
        System.out.println("props=" + streamsProperties);

        final KafkaStreams streams = buildStreams(streamsProperties);
        streams.start();

        Exit.addShutdownHook("streams-shutdown-hook", () -> {
            System.out.println("closing Kafka Streams instance");
            System.out.flush();
            streams.close();
            System.out.println("UPGRADE-TEST-CLIENT-CLOSED");
            System.out.flush();
        });
    }

    public static KafkaStreams buildStreams(final Properties streamsProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, Integer> dataTable = builder.table(
            "data", Consumed.with(stringSerde, intSerde));
        final KStream<String, Integer> dataStream = dataTable.toStream();
        dataStream.process(SmokeTestUtil.printProcessorSupplier("data"));
        dataStream.to("echo");

        final boolean runFkJoin = Boolean.parseBoolean(streamsProperties.getProperty(
            "test.run_fk_join",
            "false"));
        if (runFkJoin) {
            try {
                final KTable<Integer, String> fkTable = builder.table(
                    "fk", Consumed.with(intSerde, stringSerde));
                buildFKTable(dataStream, fkTable);
            } catch (final Exception e) {
                System.err.println("Caught " + e.getMessage());
            }
        }

        final Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "StreamsUpgradeTest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());

        final KafkaClientSupplier kafkaClientSupplier;
        if (streamsProperties.containsKey("test.future.metadata")) {
            kafkaClientSupplier = new FutureKafkaClientSupplier();
        } else {
            kafkaClientSupplier = new DefaultKafkaClientSupplier();
        }
        config.putAll(streamsProperties);

        return new KafkaStreams(builder.build(), config, kafkaClientSupplier);
    }

    private static void buildFKTable(final KStream<String, Integer> primaryTable,
                                     final KTable<Integer, String> otherTable) {
        final KStream<String, String> kStream = primaryTable.toTable()
            .join(otherTable, v -> v, (k0, v0) -> v0)
            .toStream();
        kStream.process(SmokeTestUtil.printProcessorSupplier("fk"));
        kStream.to("fk-result", Produced.with(stringSerde, stringSerde));
    }

    private static class FutureKafkaClientSupplier extends DefaultKafkaClientSupplier {
        @Override
        public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
            config.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, FutureStreamsPartitionAssignor.class.getName());
            return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        }
    }

    public static class FutureStreamsPartitionAssignor extends StreamsPartitionAssignor {
        private static final Map<String, String> CLIENT_TAGS = mkMap(mkEntry("t1", "v1"), mkEntry("t2", "v2"));
        private static final Logger log = LoggerFactory.getLogger(FutureStreamsPartitionAssignor.class);

        private AtomicInteger usedSubscriptionMetadataVersionPeek;
        private AtomicLong nextScheduledRebalanceMs;

        public FutureStreamsPartitionAssignor() {
            usedSubscriptionMetadataVersion = LATEST_SUPPORTED_VERSION + 1;
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            final Object o = configs.get("test.future.metadata");
            if (o instanceof AtomicInteger) {
                usedSubscriptionMetadataVersionPeek = (AtomicInteger) o;
            } else {
                // will not be used, just adding a dummy container for simpler code paths
                usedSubscriptionMetadataVersionPeek = new AtomicInteger();
            }
            configs.remove("test.future.metadata");
            nextScheduledRebalanceMs = new AssignorConfiguration(configs).referenceContainer().nextScheduledRebalanceMs;

            super.configure(configs);
        }

        @Override
        public ByteBuffer subscriptionUserData(final Set<String> topics) {
            // Adds the following information to subscription
            // 1. Client UUID (a unique id assigned to an instance of KafkaStreams)
            // 2. Task ids of previously running tasks
            // 3. Task ids of valid local states on the client's state directory.
            final TaskManager taskManager = taskManager();
            handleRebalanceStart(topics);
            byte uniqueField = 0;
            if (usedSubscriptionMetadataVersion <= LATEST_SUPPORTED_VERSION) {
                uniqueField++;
                return new SubscriptionInfo(
                    usedSubscriptionMetadataVersion,
                    LATEST_SUPPORTED_VERSION + 1,
                    taskManager.processId(),
                    userEndPoint(),
                    taskManager.taskOffsetSums(),
                    uniqueField,
                    0,
                    CLIENT_TAGS
                ).encode();
            } else {
                return new FutureSubscriptionInfo(
                    usedSubscriptionMetadataVersion,
                    taskManager.processId().id(),
                    SubscriptionInfo.getActiveTasksFromTaskOffsetSumMap(taskManager.taskOffsetSums()),
                    SubscriptionInfo.getStandbyTasksFromTaskOffsetSumMap(taskManager.taskOffsetSums()),
                    userEndPoint())
                    .encode();
            }
        }

        @Override
        public void onAssignment(final ConsumerPartitionAssignor.Assignment assignment,
                                 final ConsumerGroupMetadata metadata) {
            try {
                super.onAssignment(assignment, metadata);
                usedSubscriptionMetadataVersionPeek.set(usedSubscriptionMetadataVersion);
                return;
            } catch (final TaskAssignmentException cannotProcessFutureVersion) {
                // continue
            }

            final ByteBuffer data = assignment.userData();
            data.rewind();

            final int usedVersion;
            try (final DataInputStream in = new DataInputStream(new ByteBufferInputStream(data))) {
                usedVersion = in.readInt();
            } catch (final IOException ex) {
                throw new TaskAssignmentException("Failed to decode AssignmentInfo", ex);
            }

            if (usedVersion > LATEST_SUPPORTED_VERSION + 1) {
                throw new IllegalStateException("Unknown metadata version: " + usedVersion
                                                    + "; latest supported version: " + LATEST_SUPPORTED_VERSION + 1);
            }

            final AssignmentInfo info = AssignmentInfo.decode(
                assignment.userData().putInt(0, LATEST_SUPPORTED_VERSION));

            if (maybeUpdateSubscriptionVersion(usedVersion, info.commonlySupportedVersion())) {
                log.info("Requested to schedule immediate rebalance due to version probing.");
                nextScheduledRebalanceMs.set(0L);
                usedSubscriptionMetadataVersionPeek.set(usedSubscriptionMetadataVersion);
            }

            final List<TopicPartition> partitions = new ArrayList<>(assignment.partitions());
            partitions.sort(PARTITION_COMPARATOR);

            final Map<TaskId, Set<TopicPartition>> activeTasks = getActiveTasks(partitions, info);

            final TaskManager taskManager = taskManager();
            taskManager.handleAssignment(activeTasks, info.standbyTasks());
            usedSubscriptionMetadataVersionPeek.set(usedSubscriptionMetadataVersion);
        }

        @Override
        public GroupAssignment assign(final Cluster metadata, final GroupSubscription groupSubscription) {
            final Map<String, Subscription> subscriptions = groupSubscription.groupSubscription();
            final Set<Integer> supportedVersions = new HashSet<>();
            for (final Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
                final Subscription subscription = entry.getValue();
                final SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());
                supportedVersions.add(info.latestSupportedVersion());
            }
            Map<String, Assignment> assignment = null;

            final Map<String, Subscription> downgradedSubscriptions = new HashMap<>();
            for (final Subscription subscription : subscriptions.values()) {
                final SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());
                if (info.version() < LATEST_SUPPORTED_VERSION + 1) {
                    assignment = super.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
                    break;
                }
            }

            boolean bumpUsedVersion = false;
            final boolean bumpSupportedVersion;
            if (assignment != null) {
                bumpSupportedVersion = supportedVersions.size() == 1 && supportedVersions.iterator().next() == LATEST_SUPPORTED_VERSION + 1;
            } else {
                for (final Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
                    final Subscription subscription = entry.getValue();

                    final SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData()
                        .putInt(0, LATEST_SUPPORTED_VERSION)
                        .putInt(4, LATEST_SUPPORTED_VERSION));

                    downgradedSubscriptions.put(
                        entry.getKey(),
                        new Subscription(
                            subscription.topics(),
                            new SubscriptionInfo(
                                LATEST_SUPPORTED_VERSION,
                                LATEST_SUPPORTED_VERSION,
                                info.processId(),
                                info.userEndPoint(),
                                taskManager().taskOffsetSums(),
                                (byte) 0,
                                0,
                                CLIENT_TAGS
                            ).encode(),
                            subscription.ownedPartitions()
                        ));
                }
                assignment = super.assign(metadata, new GroupSubscription(downgradedSubscriptions)).groupAssignment();
                bumpUsedVersion = true;
                bumpSupportedVersion = true;
            }

            final Map<String, Assignment> newAssignment = new HashMap<>();
            for (final Map.Entry<String, Assignment> entry : assignment.entrySet()) {
                final Assignment singleAssignment = entry.getValue();
                newAssignment.put(
                    entry.getKey(),
                    new Assignment(
                        singleAssignment.partitions(),
                        new FutureAssignmentInfo(
                            bumpUsedVersion,
                            bumpSupportedVersion,
                            singleAssignment.userData())
                            .encode()));
            }

            return new GroupAssignment(newAssignment);
        }
    }

    private static class FutureSubscriptionInfo {
        private final int version;
        private final UUID processId;
        private final Set<TaskId> activeTasks;
        private final Set<TaskId> standbyTasks;
        private final String userEndPoint;

        // for testing only; don't apply version checks
        FutureSubscriptionInfo(final int version,
                               final UUID processId,
                               final Set<TaskId> activeTasks,
                               final Set<TaskId> standbyTasks,
                               final String userEndPoint) {
            this.version = version;
            this.processId = processId;
            this.activeTasks = activeTasks;
            this.standbyTasks = standbyTasks;
            this.userEndPoint = userEndPoint;
            if (version <= LATEST_SUPPORTED_VERSION) {
                throw new IllegalArgumentException("this class can't be used with version " + version);
            }
        }

        private ByteBuffer encode() {
            final byte[] endPointBytes = LegacySubscriptionInfoSerde.prepareUserEndPoint(userEndPoint);

            final ByteBuffer buf = ByteBuffer.allocate(
                4 + // used version
                4 + // latest supported version version
                16 + // client ID
                4 + activeTasks.size() * 8 +   // length + active tasks
                4 + standbyTasks.size() * 8 +  // length + standby tasks
                4 + endPointBytes.length +      // length + endpoint
                4 + //uniqueField
                4 //assignment error code
            );

            buf.putInt(version); // used version
            buf.putInt(version); // supported version
            LegacySubscriptionInfoSerde.encodeClientUUID(buf, processId);
            LegacySubscriptionInfoSerde.encodeTasks(buf, activeTasks, version);
            LegacySubscriptionInfoSerde.encodeTasks(buf, standbyTasks, version);
            LegacySubscriptionInfoSerde.encodeUserEndPoint(buf, endPointBytes);

            buf.rewind();

            return buf;
        }
    }

    private static class FutureAssignmentInfo extends AssignmentInfo {
        private final boolean bumpUsedVersion;
        private final boolean bumpSupportedVersion;
        final ByteBuffer originalUserMetadata;

        private FutureAssignmentInfo(final boolean bumpUsedVersion,
                                     final boolean bumpSupportedVersion,
                                     final ByteBuffer bytes) {
            super(LATEST_SUPPORTED_VERSION, LATEST_SUPPORTED_VERSION);
            this.bumpUsedVersion = bumpUsedVersion;
            this.bumpSupportedVersion = bumpSupportedVersion;
            originalUserMetadata = bytes;
        }

        @Override
        public ByteBuffer encode() {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();

            originalUserMetadata.rewind();

            try (final DataOutputStream out = new DataOutputStream(baos)) {
                if (bumpUsedVersion) {
                    originalUserMetadata.getInt(); // discard original used version
                    out.writeInt(LATEST_SUPPORTED_VERSION + 1);
                } else {
                    out.writeInt(originalUserMetadata.getInt());
                }
                if (bumpSupportedVersion) {
                    originalUserMetadata.getInt(); // discard original supported version
                    out.writeInt(LATEST_SUPPORTED_VERSION + 1);
                }

                try {
                    while (true) {
                        out.write(originalUserMetadata.get());
                    }
                } catch (final BufferUnderflowException expectedWhenAllDataCopied) { }

                out.flush();
                out.close();

                return ByteBuffer.wrap(baos.toByteArray());
            } catch (final IOException ex) {
                throw new TaskAssignmentException("Failed to encode AssignmentInfo", ex);
            }
        }
    }
}
