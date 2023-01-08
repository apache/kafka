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
package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.image.loader.SnapshotManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.fault.FaultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class orchestrates and manages the state related to a ZK to KRaft migration. An event thread is used to
 * serialize events coming from various threads and listeners.
 */
public class KRaftMigrationDriver implements MetadataPublisher {
    private final Time time;
    private final Logger log;
    private final int nodeId;
    private final MigrationClient zkMigrationClient;
    private final LegacyPropagator propagator;
    private final ZkRecordConsumer zkRecordConsumer;
    private final KafkaEventQueue eventQueue;
    private final FaultHandler faultHandler;
    /**
     * A callback for when the migration state has been recovered from ZK. This is used to delay the installation of this
     * MetadataPublisher with MetadataLoader.
     */
    private final Consumer<KRaftMigrationDriver> initialZkLoadHandler;
    private volatile LeaderAndEpoch leaderAndEpoch;
    private volatile MigrationState migrationState;
    private volatile ZkMigrationLeadershipState migrationLeadershipState;
    private volatile MetadataImage image;

    public KRaftMigrationDriver(
        int nodeId,
        ZkRecordConsumer zkRecordConsumer,
        MigrationClient zkMigrationClient,
        LegacyPropagator propagator,
        Consumer<KRaftMigrationDriver> initialZkLoadHandler,
        FaultHandler faultHandler
    ) {
        this.nodeId = nodeId;
        this.zkRecordConsumer = zkRecordConsumer;
        this.zkMigrationClient = zkMigrationClient;
        this.propagator = propagator;
        this.time = Time.SYSTEM;
        this.log = LoggerFactory.getLogger(KRaftMigrationDriver.class);
        this.migrationState = MigrationState.UNINITIALIZED;
        this.migrationLeadershipState = ZkMigrationLeadershipState.EMPTY;
        this.eventQueue = new KafkaEventQueue(Time.SYSTEM, new LogContext("KRaftMigrationDriver"), "kraft-migration");
        this.image = MetadataImage.EMPTY;
        this.leaderAndEpoch = LeaderAndEpoch.UNKNOWN;
        this.initialZkLoadHandler = initialZkLoadHandler;
        this.faultHandler = faultHandler;
    }

    public void start() {
        eventQueue.prepend(new PollEvent());
    }

    public void shutdown() throws InterruptedException {
        eventQueue.beginShutdown("KRaftMigrationDriver#shutdown");
        log.debug("Shutting down KRaftMigrationDriver");
        eventQueue.close();
    }

    private void initializeMigrationState() {
        log.info("Recovering migration state");
        apply("Recovery", zkMigrationClient::getOrCreateMigrationRecoveryState);
        String maybeDone = migrationLeadershipState.zkMigrationComplete() ? "done" : "not done";
        log.info("Recovered migration state {}. ZK migration is {}.", migrationLeadershipState, maybeDone);
        initialZkLoadHandler.accept(this);
        // Let's transition to INACTIVE state and wait for leadership events.
        transitionTo(MigrationState.INACTIVE);
    }

    private boolean isControllerQuorumReadyForMigration() {
        // TODO implement this
        return true;
    }

    private boolean areZkBrokersReadyForMigration() {
        if (image == MetadataImage.EMPTY) {
            // TODO maybe add WAIT_FOR_INITIAL_METADATA_PUBLISH state to avoid this kind of check?
            log.info("Waiting for initial metadata publish before checking if Zk brokers are registered.");
            return false;
        }
        Set<Integer> kraftRegisteredZkBrokers = image.cluster().zkBrokers().keySet();
        Set<Integer> zkRegisteredZkBrokers = zkMigrationClient.readBrokerIdsFromTopicAssignments();
        zkRegisteredZkBrokers.removeAll(kraftRegisteredZkBrokers);
        if (zkRegisteredZkBrokers.isEmpty()) {
            return true;
        } else {
            log.info("Still waiting for ZK brokers {} to register with KRaft.", zkRegisteredZkBrokers);
            return false;
        }
    }

    private void apply(String name, Function<ZkMigrationLeadershipState, ZkMigrationLeadershipState> stateMutator) {
        ZkMigrationLeadershipState beforeState = this.migrationLeadershipState;
        ZkMigrationLeadershipState afterState = stateMutator.apply(beforeState);
        log.trace("{} transitioned from {} to {}", name, beforeState, afterState);
        this.migrationLeadershipState = afterState;
    }

    private boolean isValidStateChange(MigrationState newState) {
        if (migrationState == newState)
            return true;
        switch (migrationState) {
            case UNINITIALIZED:
            case DUAL_WRITE:
                return newState == MigrationState.INACTIVE;
            case INACTIVE:
                return newState == MigrationState.WAIT_FOR_CONTROLLER_QUORUM;
            case WAIT_FOR_CONTROLLER_QUORUM:
                return
                    newState == MigrationState.INACTIVE ||
                    newState == MigrationState.WAIT_FOR_BROKERS;
            case WAIT_FOR_BROKERS:
                return
                    newState == MigrationState.INACTIVE ||
                    newState == MigrationState.BECOME_CONTROLLER;
            case BECOME_CONTROLLER:
                return
                    newState == MigrationState.INACTIVE ||
                    newState == MigrationState.ZK_MIGRATION ||
                    newState == MigrationState.KRAFT_CONTROLLER_TO_BROKER_COMM;
            case ZK_MIGRATION:
                return
                    newState == MigrationState.INACTIVE ||
                    newState == MigrationState.KRAFT_CONTROLLER_TO_BROKER_COMM;
            case KRAFT_CONTROLLER_TO_BROKER_COMM:
                return
                    newState == MigrationState.INACTIVE ||
                    newState == MigrationState.DUAL_WRITE;
            default:
                log.error("Migration driver trying to transition from an unknown state {}", migrationState);
                return false;
        }
    }

    private void transitionTo(MigrationState newState) {
        if (!isValidStateChange(newState)) {
            log.error("Error transition in migration driver from {} to {}", migrationState, newState);
            return;
        }
        if (newState != migrationState) {
            log.debug("{} transitioning from {} to {} state", nodeId, migrationState, newState);
        } else {
            log.trace("{} transitioning from {} to {} state", nodeId, migrationState, newState);
        }
        switch (newState) {
            case UNINITIALIZED:
                // No state can transition to UNITIALIZED.
                throw new IllegalStateException("Illegal transition from " + migrationState + " to " + newState + " " +
                "state in Zk to KRaft migration");
            case INACTIVE:
                // Any state can go to INACTIVE.
                break;
        }
        migrationState = newState;
    }

    @Override
    public String name() {
        return "KRaftMigrationDriver";
    }

    @Override
    public void publishSnapshot(MetadataDelta delta, MetadataImage newImage, SnapshotManifest manifest) {
        eventQueue.append(new MetadataChangeEvent(delta, newImage, manifest.provenance(), true));
    }

    @Override
    public void publishLogDelta(MetadataDelta delta, MetadataImage newImage, LogDeltaManifest manifest) {
        if (!leaderAndEpoch.equals(manifest.leaderAndEpoch())) {
            eventQueue.append(new KRaftLeaderEvent(manifest.leaderAndEpoch()));
        }
        eventQueue.append(new MetadataChangeEvent(delta, newImage, manifest.provenance(), false));
    }


    @Override
    public void close() throws Exception {
        eventQueue.close();
    }

    // Events handled by Migration Driver.
    abstract class MigrationEvent implements EventQueue.Event {
        @Override
        public void handleException(Throwable e) {
            KRaftMigrationDriver.this.faultHandler.handleFault("Error during ZK migration", e);
        }
    }

    class PollEvent extends MigrationEvent {
        @Override
        public void run() throws Exception {
            switch (migrationState) {
                case UNINITIALIZED:
                    initializeMigrationState();
                    break;
                case INACTIVE:
                    // Nothing to do when the driver is inactive. We need to wait on the
                    // controller node's state to move forward.
                    break;
                case WAIT_FOR_CONTROLLER_QUORUM:
                    eventQueue.append(new WaitForControllerQuorumEvent());
                    break;
                case BECOME_CONTROLLER:
                    eventQueue.append(new BecomeZkControllerEvent());
                    break;
                case WAIT_FOR_BROKERS:
                    eventQueue.append(new WaitForZkBrokersEvent());
                    break;
                case ZK_MIGRATION:
                    eventQueue.append(new MigrateMetadataEvent());
                    break;
                case KRAFT_CONTROLLER_TO_BROKER_COMM:
                    eventQueue.append(new SendRPCsToBrokersEvent());
                    break;
                case DUAL_WRITE:
                    // Nothing to do in the PollEvent. If there's metadata change, we use
                    // MetadataChange event to drive the writes to Zookeeper.
                    break;
            }

            // Poll again after some time
            long deadline = time.nanoseconds() + NANOSECONDS.convert(1, SECONDS);
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

    class KRaftLeaderEvent extends MigrationEvent {
        private final LeaderAndEpoch leaderAndEpoch;

        KRaftLeaderEvent(LeaderAndEpoch leaderAndEpoch) {
            this.leaderAndEpoch = leaderAndEpoch;
        }

        @Override
        public void run() throws Exception {
            // We can either be the active controller or just resigned from being the controller.
            KRaftMigrationDriver.this.leaderAndEpoch = leaderAndEpoch;
            boolean isActive = leaderAndEpoch.isLeader(KRaftMigrationDriver.this.nodeId);
            switch (migrationState) {
                case UNINITIALIZED:
                    // Poll and retry after initialization
                    long deadline = time.nanoseconds() + NANOSECONDS.convert(10, SECONDS);
                    eventQueue.scheduleDeferred(
                        "poll",
                        new EventQueue.DeadlineFunction(deadline),
                        this);
                    break;
                default:
                    if (!isActive) {
                        apply("KRaftLeaderEvent is not active", state -> ZkMigrationLeadershipState.EMPTY);
                        transitionTo(MigrationState.INACTIVE);
                    } else {
                        // Apply the new KRaft state
                        apply("KRaftLeaderEvent is active", state -> state.withNewKRaftController(nodeId, leaderAndEpoch.epoch()));
                        // Before becoming the controller fo ZkBrokers, we need to make sure the
                        // Controller Quorum can handle migration.
                        transitionTo(MigrationState.WAIT_FOR_CONTROLLER_QUORUM);
                    }
                    break;
            }
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class WaitForControllerQuorumEvent extends MigrationEvent {

        @Override
        public void run() throws Exception {
            switch (migrationState) {
                case WAIT_FOR_CONTROLLER_QUORUM:
                    if (isControllerQuorumReadyForMigration()) {
                        log.debug("Controller Quorum is ready for Zk to KRaft migration");
                        // Note that leadership would not change here. Hence we do not need to
                        // `apply` any leadership state change.
                        transitionTo(MigrationState.WAIT_FOR_BROKERS);
                    }
                    break;
                default:
                    // Ignore the event as we're not trying to become controller anymore.
                    break;
            }
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class BecomeZkControllerEvent extends MigrationEvent {
        @Override
        public void run() throws Exception {
            switch (migrationState) {
                case BECOME_CONTROLLER:
                    // TODO: Handle unhappy path.
                    apply("BecomeZkLeaderEvent", zkMigrationClient::claimControllerLeadership);
                    if (migrationLeadershipState.zkControllerEpochZkVersion() == -1) {
                        // We could not claim leadership, stay in BECOME_CONTROLLER to retry
                    } else {
                        if (!migrationLeadershipState.zkMigrationComplete()) {
                            transitionTo(MigrationState.ZK_MIGRATION);
                        } else {
                            transitionTo(MigrationState.KRAFT_CONTROLLER_TO_BROKER_COMM);
                        }
                    }
                    break;
                default:
                    // Ignore the event as we're not trying to become controller anymore.
                    break;
            }
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class WaitForZkBrokersEvent extends MigrationEvent {
        @Override
        public void run() throws Exception {
            switch (migrationState) {
                case WAIT_FOR_BROKERS:
                    if (areZkBrokersReadyForMigration()) {
                        log.debug("Zk brokers are registered and ready for migration");
                        transitionTo(MigrationState.BECOME_CONTROLLER);
                    }
                    break;
                default:
                    // Ignore the event as we're not in the appropriate state anymore.
                    break;
            }
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class MigrateMetadataEvent extends MigrationEvent {
        @Override
        public void run() throws Exception {
            Set<Integer> brokersInMetadata = new HashSet<>();
            log.info("Starting ZK migration");
            zkRecordConsumer.beginMigration();
            try {
                AtomicInteger count = new AtomicInteger(0);
                zkMigrationClient.readAllMetadata(batch -> {
                    try {
                        log.info("Migrating {} records from ZK: {}", batch.size(), batch);
                        CompletableFuture<?> future = zkRecordConsumer.acceptBatch(batch);
                        count.addAndGet(batch.size());
                        future.get();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e.getCause());
                    }
                }, brokersInMetadata::add);
                OffsetAndEpoch offsetAndEpochAfterMigration = zkRecordConsumer.completeMigration();
                log.info("Completed migration of metadata from Zookeeper to KRaft. A total of {} metadata records were " +
                         "generated. The current metadata offset is now {} with an epoch of {}. Saw {} brokers in the " +
                         "migrated metadata {}.",
                    count.get(),
                    offsetAndEpochAfterMigration.offset(),
                    offsetAndEpochAfterMigration.epoch(),
                    brokersInMetadata.size(),
                    brokersInMetadata);
                ZkMigrationLeadershipState newState = migrationLeadershipState.withKRaftMetadataOffsetAndEpoch(
                    offsetAndEpochAfterMigration.offset(),
                    offsetAndEpochAfterMigration.epoch());
                apply("Migrate metadata from Zk", state -> zkMigrationClient.setMigrationRecoveryState(newState));
                transitionTo(MigrationState.KRAFT_CONTROLLER_TO_BROKER_COMM);
            } catch (Throwable t) {
                zkRecordConsumer.abortMigration();
                // TODO ???
            }
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class SendRPCsToBrokersEvent extends MigrationEvent {

        @Override
        public void run() throws Exception {
            // Ignore sending RPCs to the brokers since we're no longer in the state.
            if (migrationState == MigrationState.KRAFT_CONTROLLER_TO_BROKER_COMM) {
                if (image.highestOffsetAndEpoch().compareTo(migrationLeadershipState.offsetAndEpoch()) >= 0) {
                    propagator.sendRPCsToBrokersFromMetadataImage(image, migrationLeadershipState.zkControllerEpoch());
                    // Migration leadership state doesn't change since we're not doing any Zk writes.
                    transitionTo(MigrationState.DUAL_WRITE);
                }
            }
        }
    }

    class MetadataChangeEvent extends MigrationEvent {
        private final MetadataDelta delta;
        private final MetadataImage image;
        private final MetadataProvenance provenance;
        private final boolean isSnapshot;

        MetadataChangeEvent(MetadataDelta delta, MetadataImage image, MetadataProvenance provenance, boolean isSnapshot) {
            this.delta = delta;
            this.image = image;
            this.provenance = provenance;
            this.isSnapshot = isSnapshot;
        }

        @Override
        public void run() throws Exception {
            KRaftMigrationDriver.this.image = image;

            if (migrationState != MigrationState.DUAL_WRITE) {
                log.trace("Received metadata change, but the controller is not in dual-write " +
                        "mode. Ignoring the change to be replicated to Zookeeper");
                return;
            }
            if (delta.featuresDelta() != null) {
                propagator.setMetadataVersion(image.features().metadataVersion());
            }

            if (image.highestOffsetAndEpoch().compareTo(migrationLeadershipState.offsetAndEpoch()) >= 0) {
                if (delta.topicsDelta() != null) {
                    delta.topicsDelta().changedTopics().forEach((topicId, topicDelta) -> {
                        if (delta.topicsDelta().createdTopicIds().contains(topicId)) {
                            apply("Create topic " + topicDelta.name(), migrationState ->
                                zkMigrationClient.createTopic(
                                    topicDelta.name(),
                                    topicId,
                                    topicDelta.partitionChanges(),
                                    migrationState));
                        } else {
                            apply("Updating topic " + topicDelta.name(), migrationState ->
                                zkMigrationClient.updateTopicPartitions(
                                    Collections.singletonMap(topicDelta.name(), topicDelta.partitionChanges()),
                                    migrationState));
                        }
                    });
                }


                apply("Write MetadataDelta to Zk", state -> zkMigrationClient.writeMetadataDeltaToZookeeper(delta, image, state));
                // TODO: Unhappy path: Probably relinquish leadership and let new controller
                //  retry the write?
                propagator.sendRPCsToBrokersFromMetadataDelta(delta, image,
                        migrationLeadershipState.zkControllerEpoch());
            } else {
                String metadataType = isSnapshot ? "snapshot" : "delta";
                log.info("Ignoring {} {} which contains metadata that has already been written to ZK.", metadataType, provenance);
            }
        }
    }
}
