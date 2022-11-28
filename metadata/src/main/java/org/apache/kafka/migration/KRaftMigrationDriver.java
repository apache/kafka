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
import org.apache.kafka.metadata.BrokerRegistration;
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
                public void run() {
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

        // Should only be called from the event queue
        void checkZkBrokerRegistrations() {
            if (delta == null || delta.clusterDelta() == null) {
                return;
            }
            try {
                delta.clusterDelta().changedBrokers().forEach((brokerId, registrationOpt) -> {
                    if (registrationOpt.isPresent()) {
                        // Added
                        if (registrationOpt.get().zkBroker()) {
                            log.debug("ZK Broker {} registered with KRaft controller", brokerId);
                            zkBrokerRegistrations.put(brokerId, registrationOpt.get());
                            client.addZkBroker(brokerId);
                        }
                    } else {
                        // Removed
                        if (zkBrokerRegistrations.remove(brokerId) != null) {
                            log.debug("ZK Broker {} unregistered from KRaft controller", brokerId);
                            client.removeZkBroker(brokerId);
                        }
                    }
                });

                if (zkBrokerRegistrations.keySet().containsAll(knownZkBrokers)) {
                    log.info("All ZK brokers have registered, ZK data migration will now begin.");
                    transitionTo(MigrationState.ZK_MIGRATION);
                } else {
                    Set<Integer> waitingBrokers = new HashSet<>(knownZkBrokers);
                    waitingBrokers.removeAll(zkBrokerRegistrations.keySet());
                    log.info("Not all ZK brokers have registered. Still waiting on brokers {}", waitingBrokers);
                }
            } finally {
                image = delta.apply();
                delta = null;
            }
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
                    } finally {
                        image = delta.apply();
                        delta = null;
                    }
                }
            };
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
                    String maybeDone = recoveryState.zkMigrationComplete() ? "done" : "not done";
                    log.info("Recovered migration state {}. ZK migration is {}.", recoveryState, maybeDone);
                    transitionTo(MigrationState.INACTIVE);
                    break;
                case INACTIVE:
                    // Not the KRaft controller, nothing to do
                    break;
                case WAIT_FOR_CONTROLLER_QUORUM:
                    break;
                case BECOME_CONTROLLER:
                    eventQueue.append(new BecomeZkLeaderEvent());
                    break;
                case WAIT_FOR_BROKERS:
                    log.info("Checking on ZK broker registrations");
                    listener.checkZkBrokerRegistrations();
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
                apply("KRaftLeaderEvent inactive", state -> state.mergeWithControllerState(ZkControllerState.EMPTY));
                transitionTo(MigrationState.INACTIVE);
            } else {
                // Apply the new KRaft state
                apply("KRaftLeaderEvent active", state -> state.withNewKRaftController(kraftControllerId, kraftControllerEpoch));
                // Instead of doing the ZK write directly, schedule as an event so that we can easily retry ZK failures
                transitionTo(MigrationState.BECOME_CONTROLLER);
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
            knownZkBrokers.addAll(client.readBrokerIds());
            knownZkBrokers.addAll(client.readBrokerIdsFromTopicAssignments());
            log.info("Claimed controller leadership in ZK {}. Now waiting for brokers {}", zkControllerState, knownZkBrokers);
            if (!recoveryState.zkMigrationComplete()) {
                transitionTo(MigrationState.ZK_MIGRATION);
            } else {
                transitionTo(MigrationState.WAIT_FOR_BROKERS);
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
    private final Map<Integer, BrokerRegistration> zkBrokerRegistrations = new HashMap<>();
    private final Set<Integer> knownZkBrokers = new HashSet<>();

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
