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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor;
import org.apache.kafka.streams.processor.internals.TaskManager;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.state.HostInfo;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

public class StreamsUpgradeTest {

    @SuppressWarnings("unchecked")
    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("StreamsUpgradeTest requires one argument (properties-file) but no provided: ");
        }
        final String propFileName = args.length > 0 ? args[0] : null;

        final Properties streamsProperties = Utils.loadProps(propFileName);

        System.out.println("StreamsTest instance started (StreamsUpgradeTest trunk)");
        System.out.println("props=" + streamsProperties);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream dataStream = builder.stream("data");
        dataStream.process(SmokeTestUtil.printProcessorSupplier("data"));
        dataStream.to("echo");

        final Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "StreamsUpgradeTest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        final KafkaClientSupplier kafkaClientSupplier;
        if (streamsProperties.containsKey("test.future.metadata")) {
            streamsProperties.remove("test.future.metadata");
            kafkaClientSupplier = new FutureKafkaClientSupplier();
        } else {
            kafkaClientSupplier = new DefaultKafkaClientSupplier();
        }
        config.putAll(streamsProperties);

        final KafkaStreams streams = new KafkaStreams(builder.build(), config, kafkaClientSupplier);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("closing Kafka Streams instance");
            System.out.flush();
            streams.close();
            System.out.println("UPGRADE-TEST-CLIENT-CLOSED");
            System.out.flush();
        }));
    }

    private static class FutureKafkaClientSupplier extends DefaultKafkaClientSupplier {
        @Override
        public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
            config.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, FutureStreamsPartitionAssignor.class.getName());
            return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        }
    }

    public static class FutureStreamsPartitionAssignor extends StreamsPartitionAssignor {

        public FutureStreamsPartitionAssignor() {
            usedSubscriptionMetadataVersion = SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1;
        }

        @Override
        public Subscription subscription(final Set<String> topics) {
            // Adds the following information to subscription
            // 1. Client UUID (a unique id assigned to an instance of KafkaStreams)
            // 2. Task ids of previously running tasks
            // 3. Task ids of valid local states on the client's state directory.

            final TaskManager taskManager = taskManger();
            final Set<TaskId> previousActiveTasks = taskManager.prevActiveTaskIds();
            final Set<TaskId> standbyTasks = taskManager.cachedTasksIds();
            standbyTasks.removeAll(previousActiveTasks);
            final FutureSubscriptionInfo data = new FutureSubscriptionInfo(
                usedSubscriptionMetadataVersion,
                SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1,
                taskManager.processId(),
                previousActiveTasks,
                standbyTasks,
                userEndPoint());

            taskManager.updateSubscriptionsFromMetadata(topics);

            return new Subscription(new ArrayList<>(topics), data.encode());
        }

        @Override
        public void onAssignment(final PartitionAssignor.Assignment assignment) {
            try {
                super.onAssignment(assignment);
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

            if (usedVersion > AssignmentInfo.LATEST_SUPPORTED_VERSION + 1) {
                throw new IllegalStateException("Unknown metadata version: " + usedVersion
                    + "; latest supported version: " + AssignmentInfo.LATEST_SUPPORTED_VERSION + 1);
            }

            final AssignmentInfo info = AssignmentInfo.decode(
                assignment.userData().putInt(0, AssignmentInfo.LATEST_SUPPORTED_VERSION));

            final List<TopicPartition> partitions = new ArrayList<>(assignment.partitions());
            partitions.sort(PARTITION_COMPARATOR);

            // version 1 field
            final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
            // version 2 fields
            final Map<TopicPartition, PartitionInfo> topicToPartitionInfo = new HashMap<>();
            final Map<HostInfo, Set<TopicPartition>> partitionsByHost;

            processLatestVersionAssignment(info, partitions, activeTasks, topicToPartitionInfo);
            partitionsByHost = info.partitionsByHost();

            final TaskManager taskManager = taskManger();
            taskManager.setClusterMetadata(Cluster.empty().withPartitions(topicToPartitionInfo));
            taskManager.setPartitionsByHostState(partitionsByHost);
            taskManager.setAssignmentMetadata(activeTasks, info.standbyTasks());
            taskManager.updateSubscriptionsFromAssignment(partitions);
        }

        @Override
        public Map<String, Assignment> assign(final Cluster metadata,
                                              final Map<String, Subscription> subscriptions) {
            Map<String, Assignment> assignment = null;

            final Map<String, Subscription> downgradedSubscriptions = new HashMap<>();
            for (final Subscription subscription : subscriptions.values()) {
                final SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());
                if (info.version() < SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1) {
                    assignment = super.assign(metadata, subscriptions);
                    break;
                }
            }

            boolean bumpUsedVersion = false;
            final boolean bumpSupportedVersion;
            if (assignment != null) {
                bumpSupportedVersion = supportedVersions.size() == 1 && supportedVersions.iterator().next() == SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1;
            } else {
                for (final Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
                    final Subscription subscription = entry.getValue();

                    final SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData()
                        .putInt(0, SubscriptionInfo.LATEST_SUPPORTED_VERSION)
                        .putInt(4, SubscriptionInfo.LATEST_SUPPORTED_VERSION));

                    downgradedSubscriptions.put(
                        entry.getKey(),
                        new Subscription(
                            subscription.topics(),
                            new SubscriptionInfo(
                                info.processId(),
                                info.prevTasks(),
                                info.standbyTasks(),
                                info.userEndPoint())
                                .encode()));
                }
                assignment = super.assign(metadata, downgradedSubscriptions);
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

            return newAssignment;
        }
    }

    private static class FutureSubscriptionInfo extends SubscriptionInfo {
        // for testing only; don't apply version checks
        FutureSubscriptionInfo(final int version,
                               final int latestSupportedVersion,
                               final UUID processId,
                               final Set<TaskId> prevTasks,
                               final Set<TaskId> standbyTasks,
                               final String userEndPoint) {
            super(version, latestSupportedVersion, processId, prevTasks, standbyTasks, userEndPoint);
        }

        public ByteBuffer encode() {
            if (version() <= SubscriptionInfo.LATEST_SUPPORTED_VERSION) {
                final ByteBuffer buf = super.encode();
                // super.encode() always encodes `LATEST_SUPPORTED_VERSION` as "latest supported version"
                // need to update to future version
                buf.putInt(4, latestSupportedVersion());
                return buf;
            }

            final ByteBuffer buf = encodeFutureVersion();
            buf.rewind();
            return buf;
        }

        private ByteBuffer encodeFutureVersion() {
            final byte[] endPointBytes = prepareUserEndPoint();

            final ByteBuffer buf = ByteBuffer.allocate(getVersionThreeAndFourByteLength(endPointBytes));

            buf.putInt(LATEST_SUPPORTED_VERSION + 1); // used version
            buf.putInt(LATEST_SUPPORTED_VERSION + 1); // supported version
            encodeClientUUID(buf);
            encodeTasks(buf, prevTasks());
            encodeTasks(buf, standbyTasks());
            encodeUserEndPoint(buf, endPointBytes);

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
                    out.writeInt(AssignmentInfo.LATEST_SUPPORTED_VERSION + 1);
                } else {
                    out.writeInt(originalUserMetadata.getInt());
                }
                if (bumpSupportedVersion) {
                    originalUserMetadata.getInt(); // discard original supported version
                    out.writeInt(AssignmentInfo.LATEST_SUPPORTED_VERSION + 1);
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
