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
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.image.loader.SnapshotManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;


import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    private final BrokersRpcClient rpcClient;
    private final KafkaEventQueue eventQueue;
    private volatile MigrationState migrationState;
    private volatile ZkMigrationLeadershipState migrationLeadershipState;
    private volatile MetadataDelta delta;
    private volatile MetadataImage image;

    public KRaftMigrationDriver(int nodeId, MigrationClient zkMigrationClient, BrokersRpcClient rpcClient) {
        this.nodeId = nodeId;
        this.time = Time.SYSTEM;
        this.log = LoggerFactory.getLogger(KRaftMigrationDriver.class);
        this.migrationState = MigrationState.UNINITIALIZED;
        this.migrationLeadershipState = ZkMigrationLeadershipState.EMPTY;
        this.zkMigrationClient = zkMigrationClient;
        this.rpcClient = rpcClient;
        this.eventQueue = new KafkaEventQueue(Time.SYSTEM, new LogContext("KRaftMigrationDriver"), "kraft-migration");
        this.delta = null;
        this.image = MetadataImage.EMPTY;
    }

    public void start() {
        eventQueue.prepend(new PollEvent());
    }

    private void initializeMigrationState() {
        log.info("Recovering migration state");
        apply("Recovery", zkMigrationClient::getOrCreateMigrationRecoveryState);
        String maybeDone = migrationLeadershipState.zkMigrationComplete() ? "done" : "not done";
        log.info("Recovered migration state {}. ZK migration is {}.", migrationLeadershipState, maybeDone);
        // Let's transition to INACTIVE state and wait for leadership events.
        transitionTo(MigrationState.INACTIVE);
    }

    private boolean isControllerQuorumReadyForMigration() {
        return true;
    }

    private boolean areZkBrokersReadyForMigration() {
        Set<Integer> kraftRegisteredZkBrokers = image.cluster().brokers().values()
            .stream()
            .filter(BrokerRegistration::isMigratingZkBroker)
            .map(BrokerRegistration::id)
            .collect(Collectors.toSet());
        Set<Integer> zkRegisteredZkBrokers = zkMigrationClient.readBrokerIds();
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
        log.debug("{} transitioned from {} to {}", name, beforeState, afterState);
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
                    newState == MigrationState.BECOME_CONTROLLER;
            case BECOME_CONTROLLER:
                return
                    newState == MigrationState.INACTIVE ||
                    newState == MigrationState.WAIT_FOR_BROKERS ||
                    newState == MigrationState.KRAFT_CONTROLLER_TO_BROKER_COMM;
            case WAIT_FOR_BROKERS:
                return
                    newState == MigrationState.INACTIVE ||
                    newState == MigrationState.ZK_MIGRATION;
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
        log.debug("{} transitioning from {} to {} state", nodeId, migrationState, newState);
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
        eventQueue.append(new MetadataChangeEvent(delta, newImage));
    }

    @Override
    public void publishLogDelta(MetadataDelta delta, MetadataImage newImage, LogDeltaManifest manifest) {
        eventQueue.append(new MetadataChangeEvent(delta, newImage));
    }

    @Override
    public void publishLeaderAndEpoch(LeaderAndEpoch leaderAndEpoch) {
        eventQueue.append(new KRaftLeaderEvent(leaderAndEpoch.isLeader(nodeId), leaderAndEpoch.epoch()));
    }

    @Override
    public void close() throws Exception {
        eventQueue.close();
    }

    // Events handled by Migration Driver.

    class PollEvent implements EventQueue.Event {
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

    class KRaftLeaderEvent implements EventQueue.Event {
        private final boolean isActive;
        private final int kraftControllerEpoch;

        KRaftLeaderEvent(boolean isActive, int kraftControllerEpoch) {
            this.isActive = isActive;
            this.kraftControllerEpoch = kraftControllerEpoch;
        }
        @Override
        public void run() throws Exception {
            // We can either the the active controller or just resigned being the controller.
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
                        apply("KRaftLeaderEvent is active",
                            state -> state.withControllerZkVersion(ZkMigrationLeadershipState.EMPTY.controllerZkVersion()));
                        transitionTo(MigrationState.INACTIVE);
                    } else {
                        // Apply the new KRaft state
                        apply("KRaftLeaderEvent not active", state -> state.withNewKRaftController(nodeId, kraftControllerEpoch));
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

    class WaitForControllerQuorumEvent implements EventQueue.Event {

        @Override
        public void run() throws Exception {
            switch (migrationState) {
                case WAIT_FOR_CONTROLLER_QUORUM:
                    if (isControllerQuorumReadyForMigration()) {
                        log.debug("Controller Quorum is ready for Zk to KRaft migration");
                        // Note that leadership would not change here. Hence we do not need to
                        // `apply` any leadership state change.
                        transitionTo(MigrationState.BECOME_CONTROLLER);
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

    class BecomeZkControllerEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            switch (migrationState) {
                case BECOME_CONTROLLER:
                    // TODO: Handle unhappy path.
                    apply("BecomeZkLeaderEvent", zkMigrationClient::claimControllerLeadership);
                    if (!migrationLeadershipState.zkMigrationComplete()) {
                        transitionTo(MigrationState.WAIT_FOR_BROKERS);
                    } else {
                        transitionTo(MigrationState.KRAFT_CONTROLLER_TO_BROKER_COMM);
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

    class WaitForZkBrokersEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            switch (migrationState) {
                case WAIT_FOR_BROKERS:
                    if (areZkBrokersReadyForMigration()) {
                        log.debug("Zk brokers are registered and ready for migration");
                        transitionTo(MigrationState.ZK_MIGRATION);
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

    class MigrateMetadataEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            // TODO: Do actual zk write.
            OffsetAndEpoch offsetAndEpochAfterMigration = new OffsetAndEpoch(-1, -1);
            log.debug("Completed migrating metadata from Zookeeper. Current offset is {} and " +
                    "epoch is {}", offsetAndEpochAfterMigration.offset(),
                offsetAndEpochAfterMigration.epoch());
            ZkMigrationLeadershipState newState = migrationLeadershipState.withKRaftMetadataOffsetAndEpoch(
                offsetAndEpochAfterMigration.offset(),
                offsetAndEpochAfterMigration.epoch());
            apply("Migrate metadata from Zk", state -> zkMigrationClient.setMigrationRecoveryState(newState));
            transitionTo(MigrationState.KRAFT_CONTROLLER_TO_BROKER_COMM);
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class SendRPCsToBrokersEvent implements EventQueue.Event {

        @Override
        public void run() throws Exception {
            switch (migrationState) {
                case KRAFT_CONTROLLER_TO_BROKER_COMM:
                    if (image.highestOffsetAndEpoch().compareTo(migrationLeadershipState.offsetAndEpoch()) >= 0) {
                        rpcClient.sendRPCsToBrokersFromMetadataImage(image,
                            migrationLeadershipState.kraftControllerEpoch());
                        // Migration leadership state doesn't change since we're not doing any Zk
                        // writes.
                        transitionTo(MigrationState.DUAL_WRITE);
                    }
                    break;
                default:
                    // Ignore sending RPCs to the brokers since we're no longer in the state.
                    break;
            }
        }
    }

    class MetadataChangeEvent implements EventQueue.Event {
        private final MetadataDelta delta;
        private final MetadataImage image;
        MetadataChangeEvent(MetadataDelta delta, MetadataImage image) {
            this.delta = delta;
            this.image = image;
        }

        @Override
        public void run() throws Exception {
            KRaftMigrationDriver.this.image = image;
            KRaftMigrationDriver.this.delta = delta;

            switch (migrationState) {
                case DUAL_WRITE:
                    if (image.highestOffsetAndEpoch().compareTo(migrationLeadershipState.offsetAndEpoch()) >= 0) {
                        apply("Write MetadataDelta to Zk",
                            state -> zkMigrationClient.writeMetadataDeltaToZookeeper(delta, image, state));
                        // TODO: Unhappy path: Probably relinquish leadership and let new controller
                        //  retry the write?
                        rpcClient.sendRPCsToBrokersFromMetadataDelta(delta, image,
                            migrationLeadershipState.kraftControllerEpoch());
                    }
                    break;
                default:
                    log.debug("Received metadata change, but the controller is not in dual-write " +
                        "mode. Ignoring the change to be replicated to Zookeeper");
                    break;
            }
        }
    }
}
