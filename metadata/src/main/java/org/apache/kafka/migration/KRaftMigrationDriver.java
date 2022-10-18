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
package org.apache.kafka.migration;

import org.apache.kafka.common.message.UpdateMetadataResponseData;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class orchestrates and manages the state related to a ZK to KRaft migration. An event thread is used to
 * serialize events coming from various threads and listeners.
 */
public class KRaftMigrationDriver {

    class MetadataLogListener implements KRaftMetadataListener {
        MetadataImage image = MetadataImage.EMPTY;
        MetadataDelta delta = new MetadataDelta(image);

        @Override
        public void handleLeaderChange(boolean isActive, int epoch) {
            eventQueue.append(new KRaftLeaderEvent(isActive, nodeId, epoch));
        }

        @Override
        public void handleRecord(long offset, int epoch, ApiMessage record) {
            if (record.apiKey() == MetadataRecordType.NO_OP_RECORD.id()) {
                return;
            }

            eventQueue.append(new EventQueue.Event() {
                @Override
                public void run() throws Exception {
                    if (delta == null) {
                        delta = new MetadataDelta(image);
                    }
                    delta.replay(offset, epoch, record);
                }

                @Override
                public void handleException(Throwable e) {
                    log.error("Had an exception in " + this.getClass().getSimpleName(), e);
                }
            });
        }

        ZkWriteEvent syncMetadataToZkEvent() {
            return new ZkWriteEvent() {
                @Override
                public void run() throws Exception {
                    if (delta == null) {
                        return;
                    }

                    log.info("Writing metadata changes to ZK");
                    try {
                        apply("Sync to ZK", __ -> migrationState(delta.highestOffset(), delta.highestEpoch()));
                        if (delta.topicsDelta() != null) {
                            delta.topicsDelta().changedTopics().forEach((topicId, topicDelta) -> {
                                // Ensure the topic exists
                                if (image.topics().getTopic(topicId) == null) {
                                    apply("Create topic " + topicDelta.name(), migrationState -> client.createTopic(topicDelta.name(), topicId, topicDelta.partitionChanges(), migrationState));
                                } else {
                                    apply("Updating topic " + topicDelta.name(), migrationState -> client.updateTopicPartitions(Collections.singletonMap(topicDelta.name(), topicDelta.partitionChanges()), migrationState));
                                }
                            });
                        }

                        if (delta.clusterDelta() != null) {
                            delta.clusterDelta().changedBrokers().forEach((brokerId, brokerRegistrationOpt) -> {
                                if (brokerRegistrationOpt.isPresent() && image.cluster().broker(brokerId) == null) {
                                    apply("Create Broker " + brokerId, migrationState -> client.createKRaftBroker(brokerId, brokerRegistrationOpt.get(), migrationState));
                                } else if (brokerRegistrationOpt.isPresent()) {
                                    apply("Update Broker " + brokerId, migrationState -> client.updateKRaftBroker(brokerId, brokerRegistrationOpt.get(), migrationState));
                                } else {
                                    apply("Remove Broker " + brokerId, migrationState -> client.removeKRaftBroker(brokerId, migrationState));
                                }
                            });
                        }
                    } finally {
                        image = delta.apply();
                        delta = null;
                    }
                }
            };
        }
    }

    class ZkBrokerListener implements MigrationClient.BrokerRegistrationListener {
        @Override
        public void onBrokerChange(Integer brokerId) {
            eventQueue.append(new BrokerIdChangeEvent(brokerId));
        }

        @Override
        public void onBrokersChange() {
            eventQueue.append(new BrokersChangeEvent());
        }
    }

    abstract class RPCResponseEvent<T extends ApiMessage> implements EventQueue.Event {
        private final int brokerId;
        private final T data;

        RPCResponseEvent(int brokerId, T data) {
            this.brokerId = brokerId;
            this.data = data;
        }

        int brokerId() {
            return brokerId;
        }
        T data() {
            return data;
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    abstract class ZkWriteEvent implements EventQueue.Event {
        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class UpdateMetadataResponseEvent extends RPCResponseEvent<UpdateMetadataResponseData> {
        UpdateMetadataResponseEvent(int brokerId, UpdateMetadataResponseData data) {
            super(brokerId, data);
        }

        @Override
        public void run() throws Exception {
            // TODO handle UMR response
        }
    }

    class PollEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            switch (migrationState) {
                case UNINITIALIZED:
                    log.info("Recovering migration state");
                    apply("Recovery", client::getOrCreateMigrationRecoveryState);
                    client.watchZkBrokerRegistrations(new ZkBrokerListener());
                    String maybeDone = recoveryState.zkMigrationComplete() ? "done" : "not done";
                    log.info("Recovered migration state {}. ZK migration is {}.", recoveryState, maybeDone);
                    transitionTo(MigrationState.INACTIVE);
                    break;
                case INACTIVE:
                    break;
                case NEW_LEADER:
                    // This probably means we are retrying
                    eventQueue.append(new BecomeZkLeaderEvent());
                    break;
                case NOT_READY:
                    break;
                case ZK_MIGRATION:
                    eventQueue.append(new MigrateMetadataEvent());
                    break;
                case DUAL_WRITE:
                    eventQueue.append(listener.syncMetadataToZkEvent());
                    break;
            }

            // Poll again after some time
            long deadline = time.nanoseconds() + NANOSECONDS.convert(10, SECONDS);
            eventQueue.scheduleDeferred(
                "poll",
                new EventQueue.DeadlineFunction(deadline),
                new PollEvent());
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class MigrateMetadataEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            if (migrationState != MigrationState.ZK_MIGRATION) {
                log.warn("Skipping ZK migration, already done");
                return;
            }

            Set<Integer> brokersWithAssignments = new HashSet<>();
            log.info("Begin migration from ZK");
            consumer.beginMigration();
            try {
                // TODO use a KIP-868 metadata transaction here
                List<CompletableFuture<?>> futures = new ArrayList<>();
                client.readAllMetadata(batch -> futures.add(consumer.acceptBatch(batch)), brokersWithAssignments::add);
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})).get();

                Set<Integer> brokersWithRegistrations = new HashSet<>(zkBrokerRegistrations.keySet());
                brokersWithAssignments.removeAll(brokersWithRegistrations);
                if (!brokersWithAssignments.isEmpty()) {
                    //throw new IllegalStateException("Cannot migrate data with offline brokers: " + brokersWithAssignments);
                    log.error("Offline ZK brokers detected: {}", brokersWithAssignments);
                }

                // Update the migration state
                OffsetAndEpoch offsetAndEpoch = consumer.completeMigration();
                apply("Migrating ZK to KRaft", __ -> migrationState(offsetAndEpoch.offset(), offsetAndEpoch.epoch()));
            } catch (Throwable t) {
                log.error("Migration failed", t);
                consumer.abortMigration();
            } finally {
                // TODO Just skip to dual write for now
                apply("Persist recovery state", client::setMigrationRecoveryState);
                transitionTo(MigrationState.DUAL_WRITE);
            }
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class BrokersChangeEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            Set<Integer> updatedBrokerIds = client.readBrokerIds();
            Set<Integer> added = new HashSet<>(updatedBrokerIds);
            added.removeAll(zkBrokerRegistrations.keySet());

            Set<Integer> removed = new HashSet<>(zkBrokerRegistrations.keySet());
            removed.removeAll(updatedBrokerIds);

            log.debug("ZK Brokers added: " + added + ", removed: " + removed);
            added.forEach(brokerId -> {
                Optional<ZkBrokerRegistration> broker = client.readBrokerRegistration(brokerId);
                if (broker.isPresent()) {
                    client.addZkBroker(brokerId);
                    zkBrokerRegistrations.put(brokerId, broker.get());
                } else {
                    throw new IllegalStateException("Saw broker " + brokerId + " added, but registration data is missing");
                }
            });
            removed.forEach(brokerId -> {
                client.removeZkBroker(brokerId);
                zkBrokerRegistrations.remove(brokerId);
            });

            // TODO actually verify the IBP and clusterID
            boolean brokersReady = zkBrokerRegistrations.values().stream().allMatch(broker ->
                broker.isMigrationReady() && broker.ibp().isPresent() && broker.clusterId().isPresent());
            // TODO add some state to track if brokers are ready
            if (brokersReady) {
                log.debug("All ZK Brokers are ready for migration.");
                //transitionTo(MigrationState.READY);
            } else {
                log.debug("Some ZK Brokers still not ready for migration.");
                //transitionTo(MigrationState.INELIGIBLE);
            }
            // TODO integrate with ClusterControlManager
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class BrokerIdChangeEvent implements EventQueue.Event {
        private final int brokerId;

        BrokerIdChangeEvent(int brokerId) {
            this.brokerId = brokerId;
        }

        @Override
        public void run() throws Exception {
            // TODO not sure this is expected. Can registration data change at runtime?
            log.debug("Broker {} changed. New registration: {}", brokerId, client.readBrokerRegistration(brokerId));
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class KRaftLeaderEvent implements EventQueue.Event {
        private final boolean isActive;
        private final int kraftControllerId;
        private final int kraftControllerEpoch;

        KRaftLeaderEvent(boolean isActive, int kraftControllerId, int kraftControllerEpoch) {
            this.isActive = isActive;
            this.kraftControllerId = kraftControllerId;
            this.kraftControllerEpoch = kraftControllerEpoch;
        }

        @Override
        public void run() throws Exception {
            if (migrationState == MigrationState.UNINITIALIZED) {
                // If we get notified about being the active controller before we have initialized, we need
                // to reschedule this event.
                eventQueue.append(new PollEvent());
                eventQueue.append(this);
                return;
            }

            if (!isActive) {
                apply("KRaftLeaderEvent is active", state -> state.mergeWithControllerState(ZkControllerState.EMPTY));
                transitionTo(MigrationState.INACTIVE);
            } else {
                // Apply the new KRaft state
                apply("KRaftLeaderEvent not active", state -> state.withNewKRaftController(kraftControllerId, kraftControllerEpoch));
                // Instead of doing the ZK write directly, schedule as an event so that we can easily retry ZK failures
                transitionTo(MigrationState.NEW_LEADER);
                eventQueue.append(new BecomeZkLeaderEvent());
            }
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class BecomeZkLeaderEvent extends ZkWriteEvent {
        @Override
        public void run() throws Exception {
            ZkControllerState zkControllerState = client.claimControllerLeadership(
                recoveryState.kraftControllerId(), recoveryState.kraftControllerEpoch());
            apply("BecomeZkLeaderEvent", state -> state.mergeWithControllerState(zkControllerState));

            if (!recoveryState.zkMigrationComplete()) {
                transitionTo(MigrationState.ZK_MIGRATION);
            } else {
                transitionTo(MigrationState.DUAL_WRITE);
            }
        }
    }

    private final Time time;
    private final Logger log;
    private final int nodeId;
    private final MigrationClient client;
    private final KafkaEventQueue eventQueue;
    private volatile MigrationState migrationState;
    private volatile MigrationRecoveryState recoveryState;
    private final Map<Integer, ZkBrokerRegistration> zkBrokerRegistrations = new HashMap<>();
    private final MetadataLogListener listener = new MetadataLogListener();
    private ZkMetadataConsumer consumer;

    public KRaftMigrationDriver(int nodeId, MigrationClient client) {
        this.nodeId = nodeId;
        this.time = Time.SYSTEM;
        this.log = LoggerFactory.getLogger(KRaftMigrationDriver.class); // TODO use LogContext
        this.migrationState = MigrationState.UNINITIALIZED;
        this.recoveryState = MigrationRecoveryState.EMPTY;
        this.client = client;
        this.eventQueue = new KafkaEventQueue(Time.SYSTEM, new LogContext("KRaftMigrationDriver"), "kraft-migration");
    }

    public void setMigrationCallback(ZkMetadataConsumer consumer) {
        this.consumer = consumer;
    }

    private MigrationRecoveryState migrationState(long metadataOffset, long metadataEpoch) {
        return new MigrationRecoveryState(recoveryState.kraftControllerId(), recoveryState.kraftControllerEpoch(),
                metadataOffset, metadataEpoch, time.milliseconds(), recoveryState.migrationZkVersion(), recoveryState.controllerZkVersion());
    }

    public void start() {
        eventQueue.prepend(new PollEvent());
    }

    public KRaftMetadataListener listener() {
        return listener;
    }

    private void apply(String name, Function<MigrationRecoveryState, MigrationRecoveryState> stateMutator) {
        MigrationRecoveryState beforeState = KRaftMigrationDriver.this.recoveryState;
        MigrationRecoveryState afterState = stateMutator.apply(beforeState);
        log.debug("{} transitioned from {} to {}", name, beforeState, afterState);
        KRaftMigrationDriver.this.recoveryState = afterState;
    }

    private void transitionTo(MigrationState newState) {
        // TODO enforce a state machine
        migrationState = newState;
        eventQueue.append(new PollEvent());
    }
}
