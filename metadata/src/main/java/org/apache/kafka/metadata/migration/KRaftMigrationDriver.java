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

import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.QuorumFeatures;
import org.apache.kafka.controller.metrics.QuorumControllerMetrics;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.loader.LoaderManifestType;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.util.RecordRedactor;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.server.util.Deadline;
import org.apache.kafka.server.util.FutureUtils;

import org.slf4j.Logger;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * This class orchestrates and manages the state related to a ZK to KRaft migration. A single event thread is used to
 * serialize events coming from various threads and listeners.
 */
public class KRaftMigrationDriver implements MetadataPublisher {

    private static class PollTimeSupplier {
        private final ExponentialBackoff pollBackoff;
        private long pollCount;

        PollTimeSupplier() {
            this.pollCount = 0;
            this.pollBackoff = new ExponentialBackoff(100, 2, 60000, 0.02);
        }

        void reset() {
            this.pollCount = 0;
        }

        public long nextPollTimeMs() {
            long next = pollBackoff.backoff(pollCount);
            pollCount++;
            return next;
        }
    }

    private static final Consumer<Throwable> NO_OP_HANDLER = ex -> { };

    /**
     * When waiting for the metadata layer to commit batches, we block the migration driver thread for this
     * amount of time. A large value is selected to avoid timeouts in the common case, but prevent us from
     * blocking indefinitely.
     */
    static final int METADATA_COMMIT_MAX_WAIT_MS = 300_000;

    private final Time time;
    private final Logger log;
    private final int nodeId;
    private final MigrationClient zkMigrationClient;
    private final KRaftMigrationZkWriter zkMetadataWriter;
    private final LegacyPropagator propagator;
    private final ZkRecordConsumer zkRecordConsumer;
    private final KafkaEventQueue eventQueue;
    private final PollTimeSupplier pollTimeSupplier;
    private final QuorumControllerMetrics controllerMetrics;
    private final FaultHandler faultHandler;
    private final QuorumFeatures quorumFeatures;
    private final RecordRedactor recordRedactor;
    /**
     * A callback for when the migration state has been recovered from ZK. This is used to delay the installation of this
     * MetadataPublisher with MetadataLoader.
     */
    private final Consumer<MetadataPublisher> initialZkLoadHandler;
    private final int minBatchSize;
    private volatile MigrationDriverState migrationState;
    private volatile ZkMigrationLeadershipState migrationLeadershipState;
    private volatile MetadataImage image;
    private volatile boolean firstPublish;

    // This is updated by the MetadataPublisher thread. When processing events in the migration driver thread,
    // we should check if a newer leader has been seen by examining this variable.
    private volatile LeaderAndEpoch curLeaderAndEpoch;

    KRaftMigrationDriver(
        int nodeId,
        ZkRecordConsumer zkRecordConsumer,
        MigrationClient zkMigrationClient,
        LegacyPropagator propagator,
        Consumer<MetadataPublisher> initialZkLoadHandler,
        FaultHandler faultHandler,
        QuorumFeatures quorumFeatures,
        KafkaConfigSchema configSchema,
        QuorumControllerMetrics controllerMetrics,
        int minBatchSize,
        Time time
    ) {
        this.nodeId = nodeId;
        this.zkRecordConsumer = zkRecordConsumer;
        this.zkMigrationClient = zkMigrationClient;
        this.propagator = propagator;
        this.time = time;
        LogContext logContext = new LogContext("[KRaftMigrationDriver id=" + nodeId + "] ");
        this.controllerMetrics = controllerMetrics;
        Logger log = logContext.logger(KRaftMigrationDriver.class);
        this.log = log;
        this.migrationState = MigrationDriverState.UNINITIALIZED;
        this.migrationLeadershipState = ZkMigrationLeadershipState.EMPTY;
        this.eventQueue = new KafkaEventQueue(Time.SYSTEM, logContext, "controller-" + nodeId + "-migration-driver-");
        this.pollTimeSupplier = new PollTimeSupplier();
        this.image = MetadataImage.EMPTY;
        this.firstPublish = false;
        this.initialZkLoadHandler = initialZkLoadHandler;
        this.faultHandler = faultHandler;
        this.quorumFeatures = quorumFeatures;
        this.zkMetadataWriter = new KRaftMigrationZkWriter(zkMigrationClient, log::error);
        this.recordRedactor = new RecordRedactor(configSchema);
        this.minBatchSize = minBatchSize;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public void start() {
        eventQueue.prepend(new PollEvent());
    }

    // Visible for testing
    public CompletableFuture<MigrationDriverState> migrationState() {
        CompletableFuture<MigrationDriverState> stateFuture = new CompletableFuture<>();
        eventQueue.append(() -> stateFuture.complete(migrationState));
        return stateFuture;
    }

    private boolean isControllerQuorumReadyForMigration() {
        Optional<String> notReadyMsg = this.quorumFeatures.reasonAllControllersZkMigrationNotReady(
                image.features().metadataVersion(), image.cluster().controllers());
        if (notReadyMsg.isPresent()) {
            log.warn("Still waiting for all controller nodes ready to begin the migration. Not ready due to:" + notReadyMsg.get());
            return false;
        }
        return true;
    }

    private boolean imageDoesNotContainAllBrokers(MetadataImage image, Set<Integer> brokerIds) {
        for (BrokerRegistration broker : image.cluster().brokers().values()) {
            if (broker.isMigratingZkBroker()) {
                brokerIds.remove(broker.id());
            }
        }
        return !brokerIds.isEmpty();
    }

    private boolean areZkBrokersReadyForMigration() {
        if (!firstPublish) {
            log.info("Waiting for initial metadata publish before checking if Zk brokers are registered.");
            return false;
        }

        if (image.cluster().isEmpty()) {
            // This primarily happens in system tests when we are starting a new ZK cluster and KRaft quorum
            // around the same time.
            log.info("No brokers are known to KRaft, waiting for brokers to register.");
            return false;
        }

        Set<Integer> zkBrokerRegistrations = zkMigrationClient.readBrokerIds();
        if (zkBrokerRegistrations.isEmpty()) {
            // Similar to the above empty check
            log.info("No brokers are registered in ZK, waiting for brokers to register.");
            return false;
        }

        if (imageDoesNotContainAllBrokers(image, zkBrokerRegistrations)) {
            log.info("Still waiting for ZK brokers {} to register with KRaft.", zkBrokerRegistrations);
            return false;
        }

        // Once all of those are found, check the topic assignments. This is much more expensive than listing /brokers
        Set<Integer> zkBrokersWithAssignments = new HashSet<>();
        zkMigrationClient.topicClient().iterateTopics(
            EnumSet.of(TopicMigrationClient.TopicVisitorInterest.TOPICS),
            (topicName, topicId, assignments) -> assignments.values().forEach(zkBrokersWithAssignments::addAll)
        );

        if (imageDoesNotContainAllBrokers(image, zkBrokersWithAssignments)) {
            log.info("Still waiting for ZK brokers {} found in metadata to register with KRaft.", zkBrokersWithAssignments);
            return false;
        }

        return true;
    }

    /**
     * Apply a function which transforms our internal migration state.
     *
     * @param name         A descriptive name of the function that is being applied
     * @param migrationOp  A function which performs some migration operations and possibly transforms our internal state
     */

    private void applyMigrationOperation(String name, KRaftMigrationOperation migrationOp) {
        applyMigrationOperation(name, migrationOp, false);
    }

    private void applyMigrationOperation(String name, KRaftMigrationOperation migrationOp, boolean alwaysLog) {
        ZkMigrationLeadershipState beforeState = this.migrationLeadershipState;
        long startTimeNs = time.nanoseconds();
        ZkMigrationLeadershipState afterState = migrationOp.apply(beforeState);
        long durationNs = time.nanoseconds() - startTimeNs;
        if (afterState.loggableChangeSinceState(beforeState) || alwaysLog) {
            log.info("{} in {} ns. Transitioned migration state from {} to {}",
                name, durationNs, beforeState, afterState);
        } else if (afterState.equals(beforeState)) {
            log.trace("{} in {} ns. Kept migration state as {}", name, durationNs, afterState);
        } else {
            log.trace("{} in {} ns. Transitioned migration state from {} to {}",
                name, durationNs, beforeState, afterState);
        }
        this.migrationLeadershipState = afterState;
    }

    private boolean isValidStateChange(MigrationDriverState newState) {
        if (migrationState == newState)
            return true;

        if (newState == MigrationDriverState.UNINITIALIZED) {
            return false;
        }

        switch (migrationState) {
            case UNINITIALIZED:
            case DUAL_WRITE:
                return newState == MigrationDriverState.INACTIVE;
            case INACTIVE:
                return newState == MigrationDriverState.WAIT_FOR_CONTROLLER_QUORUM;
            case WAIT_FOR_CONTROLLER_QUORUM:
                return
                    newState == MigrationDriverState.INACTIVE ||
                    newState == MigrationDriverState.BECOME_CONTROLLER ||
                    newState == MigrationDriverState.WAIT_FOR_BROKERS;
            case WAIT_FOR_BROKERS:
                return
                    newState == MigrationDriverState.INACTIVE ||
                    newState == MigrationDriverState.BECOME_CONTROLLER;
            case BECOME_CONTROLLER:
                return
                    newState == MigrationDriverState.INACTIVE ||
                    newState == MigrationDriverState.ZK_MIGRATION ||
                    newState == MigrationDriverState.SYNC_KRAFT_TO_ZK;
            case ZK_MIGRATION:
                return
                    newState == MigrationDriverState.INACTIVE ||
                    newState == MigrationDriverState.SYNC_KRAFT_TO_ZK;
            case SYNC_KRAFT_TO_ZK:
                return
                    newState == MigrationDriverState.INACTIVE ||
                    newState == MigrationDriverState.KRAFT_CONTROLLER_TO_BROKER_COMM;
            case KRAFT_CONTROLLER_TO_BROKER_COMM:
                return
                    newState == MigrationDriverState.INACTIVE ||
                    newState == MigrationDriverState.DUAL_WRITE;
            default:
                log.error("Migration driver trying to transition from an unknown state {}", migrationState);
                return false;
        }
    }

    /**
     * Check that the migration driver is in the correct state for a given event. If the event causes
     * updates (i.e., to ZK or broker RPCs), also check that the event is for the current KRaft controller epoch.
     */
    private boolean checkDriverState(MigrationDriverState expectedState, MigrationEvent migrationEvent) {
        if (migrationEvent instanceof MigrationWriteEvent) {
            LeaderAndEpoch curLeaderAndEpoch = KRaftMigrationDriver.this.curLeaderAndEpoch;
            LeaderAndEpoch eventLeaderAndEpoch = ((MigrationWriteEvent) migrationEvent).eventLeaderAndEpoch();
            if (!eventLeaderAndEpoch.equals(curLeaderAndEpoch)) {
                log.info("Current leader epoch is {}, but event was created with epoch {}. Not running this event {}.",
                        curLeaderAndEpoch, eventLeaderAndEpoch, migrationEvent);
                return false;
            }
        }

        if (!migrationState.equals(expectedState)) {
            log.info("Expected driver state {} but found {}. Not running this event {}.",
                expectedState, migrationState, migrationEvent);
            return false;
        }

        return true;
    }

    // Visible for testing
    void transitionTo(MigrationDriverState newState) {
        if (!isValidStateChange(newState)) {
            throw new IllegalStateException(
                String.format("Invalid transition in migration driver from %s to %s", migrationState, newState));
        }

        if (newState != migrationState) {
            log.info("{} transitioning from {} to {} state", nodeId, migrationState, newState);
            pollTimeSupplier.reset();
            wakeup();
        } else {
            log.trace("{} transitioning from {} to {} state", nodeId, migrationState, newState);
        }

        migrationState = newState;
    }

    private void wakeup() {
        eventQueue.append(new PollEvent());
    }

    // MetadataPublisher methods

    @Override
    public String name() {
        return "KRaftMigrationDriver";
    }

    @Override
    public void onControllerChange(LeaderAndEpoch newLeaderAndEpoch) {
        curLeaderAndEpoch = newLeaderAndEpoch;
        eventQueue.append(new KRaftLeaderEvent(newLeaderAndEpoch));
    }

    @Override
    public void onMetadataUpdate(
        MetadataDelta delta,
        MetadataImage newImage,
        LoaderManifest manifest
    ) {
        enqueueMetadataChangeEvent(delta,
            newImage,
            manifest.provenance(),
            manifest.type() == LoaderManifestType.SNAPSHOT,
            NO_OP_HANDLER);
    }

    @Override
    public void close() throws InterruptedException {
        eventQueue.beginShutdown("KRaftMigrationDriver#shutdown");
        log.debug("Shutting down KRaftMigrationDriver");
        eventQueue.close();
    }

    /**
     * Construct and enqueue a {@link MetadataChangeEvent} with a given completion handler. In production use cases,
     * this handler is a no-op. This method exists so that we can add additional logic in our unit tests to wait for the
     * enqueued event to finish executing.
     */
    void enqueueMetadataChangeEvent(
        MetadataDelta delta,
        MetadataImage newImage,
        MetadataProvenance provenance,
        boolean isSnapshot,
        Consumer<Throwable> completionHandler
    ) {
        LeaderAndEpoch eventLeaderAndEpoch = KRaftMigrationDriver.this.curLeaderAndEpoch;
        MetadataChangeEvent metadataChangeEvent = new MetadataChangeEvent(
            delta,
            newImage,
            provenance,
            isSnapshot,
            completionHandler,
            eventLeaderAndEpoch
        );
        eventQueue.append(metadataChangeEvent);
    }

    // Events handled by Migration Driver.
    abstract class MigrationEvent implements EventQueue.Event {
        @SuppressWarnings("ThrowableNotThrown")
        @Override
        public void handleException(Throwable e) {
            if (e instanceof MigrationClientAuthException) {
                KRaftMigrationDriver.this.faultHandler.handleFault("Encountered ZooKeeper authentication in " + this, e);
            } else if (e instanceof MigrationClientException) {
                log.info(String.format("Encountered ZooKeeper error during event %s. Will retry.", this), e.getCause());
            } else if (e instanceof RejectedExecutionException) {
                log.debug("Not processing {} because the event queue is closed.", this);
            } else {
                KRaftMigrationDriver.this.faultHandler.handleFault("Unhandled error in " + this, e);
            }
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    /**
     * An event that has some side effects like updating ZK or sending RPCs to brokers.
     * These event should only run if they are for the current KRaft controller epoch.
     * See {@link #checkDriverState(MigrationDriverState, MigrationEvent)}
     */
    interface MigrationWriteEvent {
        /**
         * @return The LeaderAndEpoch as seen at the time of event creation.
         */
        LeaderAndEpoch eventLeaderAndEpoch();
    }

    /**
     * An event generated by a call to {@link MetadataPublisher#onControllerChange}. This will not be called until
     * this class is registered with {@link org.apache.kafka.image.loader.MetadataLoader}. The registration happens
     * after the migration state is loaded from ZooKeeper in {@link RecoverMigrationStateFromZKEvent}.
     */
    class KRaftLeaderEvent extends MigrationEvent {
        private final LeaderAndEpoch leaderAndEpoch;

        KRaftLeaderEvent(LeaderAndEpoch leaderAndEpoch) {
            this.leaderAndEpoch = leaderAndEpoch;
        }

        @Override
        public void run() throws Exception {
            // We can either be the active controller or just resigned from being the controller.
            boolean isActive = leaderAndEpoch.isLeader(KRaftMigrationDriver.this.nodeId);

            if (!isActive) {
                applyMigrationOperation("Became inactive migration driver", state ->
                    state.withNewKRaftController(
                        leaderAndEpoch.leaderId().orElse(ZkMigrationLeadershipState.EMPTY.kraftControllerId()),
                        leaderAndEpoch.epoch()
                    ).withUnknownZkController()
                );
                transitionTo(MigrationDriverState.INACTIVE);
            } else {
                // Load the existing migration state and apply the new KRaft state
                applyMigrationOperation("Became active migration driver", state -> {
                    ZkMigrationLeadershipState recoveredState = zkMigrationClient.getOrCreateMigrationRecoveryState(state);
                    return recoveredState.withNewKRaftController(nodeId, leaderAndEpoch.epoch()).withUnknownZkController();
                });

                // Before becoming the controller fo ZkBrokers, we need to make sure the
                // Controller Quorum can handle migration.
                transitionTo(MigrationDriverState.WAIT_FOR_CONTROLLER_QUORUM);
            }
        }
    }

    class MetadataChangeEvent extends MigrationEvent {
        private final MetadataDelta delta;
        private final MetadataImage image;
        private final MetadataProvenance provenance;
        private final boolean isSnapshot;
        private final Consumer<Throwable> completionHandler;
        private final LeaderAndEpoch leaderAndEpoch;

        MetadataChangeEvent(
                MetadataDelta delta,
                MetadataImage image,
                MetadataProvenance provenance,
                boolean isSnapshot,
                Consumer<Throwable> completionHandler,
                LeaderAndEpoch leaderAndEpoch
        ) {
            this.delta = delta;
            this.image = image;
            this.provenance = provenance;
            this.isSnapshot = isSnapshot;
            this.completionHandler = completionHandler;
            this.leaderAndEpoch = leaderAndEpoch;
        }

        @Override
        public void run() throws Exception {
            if (!firstPublish && image.isEmpty()) {
                // KAFKA-15389 When first loading from an empty log, MetadataLoader can publish an empty image
                log.debug("Encountered an empty MetadataImage while waiting for the first image to be published. " +
                        "Ignoring this image since it either does not include bootstrap records or it is a valid " +
                        "image for an older unsupported metadata version.");
                completionHandler.accept(null);
                return;
            }
            LeaderAndEpoch curLeaderAndEpoch = KRaftMigrationDriver.this.curLeaderAndEpoch;
            KRaftMigrationDriver.this.firstPublish = true;
            MetadataImage prevImage = KRaftMigrationDriver.this.image;
            KRaftMigrationDriver.this.image = image;
            String metadataType = isSnapshot ? "snapshot" : "delta";

            if (migrationState.equals(MigrationDriverState.INACTIVE)) {
                // No need to log anything if this node is not the active controller
                completionHandler.accept(null);
                return;
            }

            if (!migrationState.allowDualWrite()) {
                log.trace("Received metadata {}, but the controller is not in dual-write " +
                    "mode. Ignoring this metadata update.", metadataType);
                completionHandler.accept(null);
                // If the driver is active and dual-write is not yet enabled, then the migration has not yet begun.
                // Only wake up the thread if the broker registrations have changed
                if (delta.clusterDelta() != null) {
                    wakeup();
                }
                return;
            }

            if (!curLeaderAndEpoch.equals(leaderAndEpoch)) {
                log.trace("Received metadata {} with {}, but the current leader and epoch is {}." +
                    "Ignoring this metadata update.", metadataType, leaderAndEpoch, curLeaderAndEpoch);
                completionHandler.accept(null);
                return;
            }

            // Until the metadata has been migrated, the migrationLeadershipState offset is -1. We need to ignore
            // metadata images until we see that the migration has happened and the image exceeds the offset of the
            // migration
            if (!migrationLeadershipState.initialZkMigrationComplete()) {
                log.info("Ignoring {} {} since the migration has not finished.", metadataType, provenance);
                completionHandler.accept(null);
                return;
            }

            // If the migration has finished, the migrationLeadershipState offset will be positive. Ignore any images
            // which are older than the offset that has been written to ZK.
            if (image.highestOffsetAndEpoch().compareTo(migrationLeadershipState.offsetAndEpoch()) < 0) {
                log.info("Ignoring {} {} which contains metadata that has already been written to ZK.", metadataType, provenance);
                completionHandler.accept(null);
                return;
            }

            Map<String, Integer> dualWriteCounts = new TreeMap<>();
            long startTime = time.nanoseconds();
            final long zkWriteTimeMs;
            if (isSnapshot) {
                zkMetadataWriter.handleSnapshot(image, countingOperationConsumer(
                    dualWriteCounts, KRaftMigrationDriver.this::applyMigrationOperation));
                zkWriteTimeMs = NANOSECONDS.toMillis(time.nanoseconds() - startTime);
                controllerMetrics.updateZkWriteSnapshotTimeMs(zkWriteTimeMs);
            } else {
                if (zkMetadataWriter.handleDelta(prevImage, image, delta, countingOperationConsumer(
                      dualWriteCounts, KRaftMigrationDriver.this::applyMigrationOperation))) {
                    // Only record delta write time if we changed something. Otherwise, no-op records will skew timings.
                    zkWriteTimeMs = NANOSECONDS.toMillis(time.nanoseconds() - startTime);
                    controllerMetrics.updateZkWriteDeltaTimeMs(zkWriteTimeMs);
                } else {
                    zkWriteTimeMs = 0;
                }
            }
            if (dualWriteCounts.isEmpty()) {
                log.trace("Did not make any ZK writes when handling KRaft {}", isSnapshot ? "snapshot" : "delta");
            } else {
                log.debug("Made the following ZK writes in {} ms when handling KRaft {}: {}",
                    zkWriteTimeMs, isSnapshot ? "snapshot" : "delta", dualWriteCounts);
            }

            // Persist the offset of the metadata that was written to ZK
            ZkMigrationLeadershipState zkStateAfterDualWrite = migrationLeadershipState.withKRaftMetadataOffsetAndEpoch(
                    image.highestOffsetAndEpoch().offset(), image.highestOffsetAndEpoch().epoch());
            //update the dual write offset metric
            controllerMetrics.updateDualWriteOffset(image.highestOffsetAndEpoch().offset());

            applyMigrationOperation("Updated ZK migration state after " + metadataType,
                state -> zkMigrationClient.setMigrationRecoveryState(zkStateAfterDualWrite));

            if (isSnapshot) {
                // When we load a snapshot, need to send full metadata updates to the brokers
                log.debug("Sending full metadata RPCs to brokers for snapshot.");
                propagator.sendRPCsToBrokersFromMetadataImage(image, migrationLeadershipState.zkControllerEpoch());
            } else {
                // delta
                if (delta.topicsDelta() != null || delta.clusterDelta() != null) {
                    log.trace("Sending incremental metadata RPCs to brokers for delta.");
                    propagator.sendRPCsToBrokersFromMetadataDelta(delta, image, migrationLeadershipState.zkControllerEpoch());
                } else {
                    log.trace("Not sending RPCs to brokers for metadata {} since no relevant metadata has changed", metadataType);
                }
            }

            completionHandler.accept(null);
        }

        @Override
        public void handleException(Throwable e) {
            completionHandler.accept(e);
            super.handleException(e);
        }

        @Override
        public String toString() {
            return "MetadataChangeEvent{" +
                "provenance=" + provenance +
                ", isSnapshot=" + isSnapshot +
                '}';
        }
    }

    class WaitForControllerQuorumEvent extends MigrationEvent {

        @Override
        public void run() throws Exception {
            if (checkDriverState(MigrationDriverState.WAIT_FOR_CONTROLLER_QUORUM, this)) {
                if (!firstPublish) {
                    log.trace("Waiting until we have received metadata before proceeding with migration");
                    return;
                }

                ZkMigrationState zkMigrationState = image.features().zkMigrationState();
                switch (zkMigrationState) {
                    case NONE:
                        // This error message is used in zookeeper_migration_test.py::TestMigration.test_pre_migration_mode_3_4
                        log.error("The controller's ZkMigrationState is NONE which means this cluster should not be migrated from ZooKeeper. " +
                            "This controller should not be configured with 'zookeeper.metadata.migration.enable' set to true. " +
                            "Will not proceed with a migration.");
                        transitionTo(MigrationDriverState.INACTIVE);
                        break;
                    case PRE_MIGRATION:
                        if (isControllerQuorumReadyForMigration()) {
                            // Base case when starting the migration
                            log.info("Controller Quorum is ready for Zk to KRaft migration. Now waiting for ZK brokers.");
                            transitionTo(MigrationDriverState.WAIT_FOR_BROKERS);
                        }
                        break;
                    case MIGRATION:
                        if (!migrationLeadershipState.initialZkMigrationComplete()) {
                            log.error("KRaft controller indicates an active migration, but the ZK state does not.");
                            transitionTo(MigrationDriverState.INACTIVE);
                        } else {
                            // Base case when rebooting a controller during migration
                            log.info("Migration is in already progress, not waiting on ZK brokers.");
                            transitionTo(MigrationDriverState.BECOME_CONTROLLER);
                        }
                        break;
                    case POST_MIGRATION:
                        log.error("KRaft controller indicates a completed migration, but the migration driver is somehow active.");
                        transitionTo(MigrationDriverState.INACTIVE);
                        break;
                    default:
                        throw new IllegalStateException("Unsupported ZkMigrationState " + zkMigrationState);
                }
            }
        }
    }

    class WaitForZkBrokersEvent extends MigrationEvent {
        @Override
        public void run() throws Exception {
            if (checkDriverState(MigrationDriverState.WAIT_FOR_BROKERS, this)) {
                if (areZkBrokersReadyForMigration()) {
                    log.info("Zk brokers are registered and ready for migration");
                    transitionTo(MigrationDriverState.BECOME_CONTROLLER);
                }
            }
        }
    }

    class BecomeZkControllerEvent extends MigrationEvent implements MigrationWriteEvent {
        private final LeaderAndEpoch leaderAndEpoch;

        BecomeZkControllerEvent(LeaderAndEpoch leaderAndEpoch) {
            this.leaderAndEpoch = leaderAndEpoch;
        }

        @Override
        public void run() throws Exception {
            // The leader epoch check in checkDriverState prevents us from getting stuck retrying this event after a
            // new leader has been seen.
            if (checkDriverState(MigrationDriverState.BECOME_CONTROLLER, this)) {
                applyMigrationOperation("Claimed ZK controller leadership", zkMigrationClient::claimControllerLeadership, true);
                if (migrationLeadershipState.zkControllerEpochZkVersion() == ZkMigrationLeadershipState.UNKNOWN_ZK_VERSION) {
                    log.info("Unable to claim leadership, will retry until we learn of a different KRaft leader");
                    return; // Stay in BECOME_CONTROLLER state and retry
                }

                // KAFKA-16171 and KAFKA-16667: Prior writing to /controller and /controller_epoch ZNodes above,
                // the previous controller could have modified the /migration ZNode. Since ZK does grant us linearizability
                // between writes and reads on different ZNodes, we need to write something to the /migration ZNode to
                // ensure we have the latest /migration zkVersion.
                applyMigrationOperation("Updated migration state", state -> {
                    // ZkVersion of -1 causes an unconditional update on /migration via KafkaZkClient#retryRequestsUntilConnected
                    state = state.withMigrationZkVersion(-1);
                    return zkMigrationClient.setMigrationRecoveryState(state);
                });

                if (!migrationLeadershipState.initialZkMigrationComplete()) {
                    transitionTo(MigrationDriverState.ZK_MIGRATION);
                } else {
                    transitionTo(MigrationDriverState.SYNC_KRAFT_TO_ZK);
                }
            }
        }

        @Override
        public LeaderAndEpoch eventLeaderAndEpoch() {
            return leaderAndEpoch;
        }
    }

    private BufferingBatchConsumer<ApiMessageAndVersion> buildMigrationBatchConsumer(
        MigrationManifest.Builder manifestBuilder
    ) {
        return new BufferingBatchConsumer<>(batch -> {
            try {
                if (log.isTraceEnabled()) {
                    batch.forEach(apiMessageAndVersion ->
                        log.trace(recordRedactor.toLoggableString(apiMessageAndVersion.message())));
                }
                CompletableFuture<?> future = zkRecordConsumer.acceptBatch(batch);
                long batchStart = time.nanoseconds();
                FutureUtils.waitWithLogging(KRaftMigrationDriver.this.log, "",
                    "the metadata layer to commit " + batch.size() + " migration records",
                    future, Deadline.fromDelay(time, METADATA_COMMIT_MAX_WAIT_MS, TimeUnit.MILLISECONDS), time);
                long batchEnd = time.nanoseconds();
                manifestBuilder.acceptBatch(batch, batchEnd - batchStart);
            } catch (Throwable e) {
                // This will cause readAllMetadata to throw since this batch consumer is called directly from readAllMetadata
                throw new RuntimeException(e);
            }
        }, minBatchSize);
    }

    class MigrateMetadataEvent extends MigrationEvent implements MigrationWriteEvent {

        private final LeaderAndEpoch leaderAndEpoch;

        MigrateMetadataEvent(LeaderAndEpoch leaderAndEpoch) {
            this.leaderAndEpoch = leaderAndEpoch;
        }

        @Override
        public void run() throws Exception {
            if (!checkDriverState(MigrationDriverState.ZK_MIGRATION, this)) {
                return;
            }
            Set<Integer> brokersInMetadata = new HashSet<>();
            log.info("Starting ZK migration");
            MigrationManifest.Builder manifestBuilder = MigrationManifest.newBuilder(time);
            try {
                FutureUtils.waitWithLogging(KRaftMigrationDriver.this.log, "",
                    "the metadata layer to begin the migration transaction",
                    zkRecordConsumer.beginMigration(),
                    Deadline.fromDelay(time, METADATA_COMMIT_MAX_WAIT_MS, TimeUnit.MILLISECONDS), time);
            } catch (Throwable t) {
                log.error("Could not start the migration", t);
                super.handleException(t);
            }
            try {
                BufferingBatchConsumer<ApiMessageAndVersion> migrationBatchConsumer = buildMigrationBatchConsumer(manifestBuilder);
                zkMigrationClient.readAllMetadata(
                    migrationBatchConsumer,
                    brokersInMetadata::add
                );
                migrationBatchConsumer.flush();
                CompletableFuture<OffsetAndEpoch> completeMigrationFuture = zkRecordConsumer.completeMigration();
                OffsetAndEpoch offsetAndEpochAfterMigration = FutureUtils.waitWithLogging(
                    KRaftMigrationDriver.this.log, "",
                    "the metadata layer to complete the migration",
                    completeMigrationFuture, Deadline.fromDelay(time, METADATA_COMMIT_MAX_WAIT_MS, TimeUnit.MILLISECONDS), time);
                MigrationManifest manifest = manifestBuilder.build();
                log.info("Completed migration of metadata from ZooKeeper to KRaft. {}. " +
                         "The current metadata offset is now {} with an epoch of {}. Saw {} brokers in the " +
                         "migrated metadata {}.",
                    manifest,
                    offsetAndEpochAfterMigration.offset(),
                    offsetAndEpochAfterMigration.epoch(),
                    brokersInMetadata.size(),
                    brokersInMetadata);
                ZkMigrationLeadershipState newState = migrationLeadershipState.withKRaftMetadataOffsetAndEpoch(
                    offsetAndEpochAfterMigration.offset(),
                    offsetAndEpochAfterMigration.epoch());
                applyMigrationOperation(
                    "Finished initial migration of ZK metadata to KRaft",
                    state -> zkMigrationClient.setMigrationRecoveryState(newState),
                    true
                );
                // Even though we just migrated everything, we still pass through the SYNC_KRAFT_TO_ZK state. This
                // accomplishes two things: ensuring we have consistent metadata state between KRaft and ZK, and
                // exercising the snapshot handling code in KRaftMigrationZkWriter.
                transitionTo(MigrationDriverState.SYNC_KRAFT_TO_ZK);
            } catch (Throwable t) {
                MigrationManifest partialManifest = manifestBuilder.build();
                log.error("Aborting the metadata migration from ZooKeeper to KRaft. {}.", partialManifest, t);
                zkRecordConsumer.abortMigration(); // This terminates the controller via fatal fault handler
                super.handleException(t);
            }
        }

        @Override
        public LeaderAndEpoch eventLeaderAndEpoch() {
            return leaderAndEpoch;
        }
    }

    class SyncKRaftMetadataEvent extends MigrationEvent implements MigrationWriteEvent {
        private final LeaderAndEpoch leaderAndEpoch;

        SyncKRaftMetadataEvent(LeaderAndEpoch leaderAndEpoch) {
            this.leaderAndEpoch = leaderAndEpoch;
        }

        @Override
        public void run() throws Exception {
            if (checkDriverState(MigrationDriverState.SYNC_KRAFT_TO_ZK, this)) {
                // The migration offset will be non-negative at this point, so we just need to check that the image
                // we have actually includes the migration metadata.
                if (image.highestOffsetAndEpoch().compareTo(migrationLeadershipState.offsetAndEpoch()) < 0) {
                    log.info("Ignoring image {} which does not contain a superset of the metadata in ZK. Staying in " +
                             "SYNC_KRAFT_TO_ZK until a newer image is loaded", image.provenance());
                    return;
                }
                log.info("Performing a full metadata sync from KRaft to ZK.");
                Map<String, Integer> dualWriteCounts = new TreeMap<>();
                long startTime = time.nanoseconds();
                zkMetadataWriter.handleSnapshot(image, countingOperationConsumer(
                    dualWriteCounts, KRaftMigrationDriver.this::applyMigrationOperation));
                long endTime = time.nanoseconds();
                controllerMetrics.updateZkWriteSnapshotTimeMs(NANOSECONDS.toMillis(startTime - endTime));
                if (dualWriteCounts.isEmpty()) {
                    log.info("Did not make any ZK writes when reconciling with KRaft state.");
                } else {
                    log.info("Made the following ZK writes when reconciling with KRaft state: {}", dualWriteCounts);
                }
                transitionTo(MigrationDriverState.KRAFT_CONTROLLER_TO_BROKER_COMM);
            }
        }

        @Override
        public LeaderAndEpoch eventLeaderAndEpoch() {
            return leaderAndEpoch;
        }
    }

    class SendRPCsToBrokersEvent extends MigrationEvent implements MigrationWriteEvent {

        private final LeaderAndEpoch leaderAndEpoch;

        SendRPCsToBrokersEvent(LeaderAndEpoch leaderAndEpoch) {
            this.leaderAndEpoch = leaderAndEpoch;
        }

        @Override
        public void run() throws Exception {
            // Ignore sending RPCs to the brokers since we're no longer in the state.
            if (checkDriverState(MigrationDriverState.KRAFT_CONTROLLER_TO_BROKER_COMM, this)) {
                if (image.highestOffsetAndEpoch().compareTo(migrationLeadershipState.offsetAndEpoch()) >= 0) {
                    log.info("Sending RPCs to broker before moving to dual-write mode using " +
                            "at offset and epoch {}", image.highestOffsetAndEpoch());
                    propagator.sendRPCsToBrokersFromMetadataImage(image, migrationLeadershipState.zkControllerEpoch());
                    // Migration leadership state doesn't change since we're not doing any Zk writes.
                    transitionTo(MigrationDriverState.DUAL_WRITE);
                } else {
                    log.info("Not sending metadata RPCs with current metadata image since does not contain the offset " +
                        "that was last written to ZK during the migration. Image offset {} is less than migration " +
                        "leadership state offset {}", image.highestOffsetAndEpoch(), migrationLeadershipState.offsetAndEpoch());
                }
            }
        }

        @Override
        public LeaderAndEpoch eventLeaderAndEpoch() {
            return leaderAndEpoch;
        }
    }

    class RecoverMigrationStateFromZKEvent extends MigrationEvent {
        @Override
        public void run() throws Exception {
            if (checkDriverState(MigrationDriverState.UNINITIALIZED, this)) {
                applyMigrationOperation("Recovered migration state from ZK", zkMigrationClient::getOrCreateMigrationRecoveryState);
                String maybeDone = migrationLeadershipState.initialZkMigrationComplete() ? "done" : "not done";
                log.info("Initial migration of ZK metadata is {}.", maybeDone);

                // Once we've recovered the migration state from ZK, install this class as a metadata publisher
                // by calling the initialZkLoadHandler.
                initialZkLoadHandler.accept(KRaftMigrationDriver.this);

                // Transition to INACTIVE state and wait for leadership events.
                transitionTo(MigrationDriverState.INACTIVE);
            }
        }
    }

    class PollEvent extends MigrationEvent {

        @Override
        public void run() throws Exception {
            LeaderAndEpoch eventLeaderAndEpoch = KRaftMigrationDriver.this.curLeaderAndEpoch;
            switch (migrationState) {
                case UNINITIALIZED:
                    eventQueue.append(new RecoverMigrationStateFromZKEvent());
                    break;
                case INACTIVE:
                    // Nothing to do when the driver is inactive. We must wait until a KRaftLeaderEvent
                    // tells informs us that we are the leader.
                    break;
                case WAIT_FOR_CONTROLLER_QUORUM:
                    eventQueue.append(new WaitForControllerQuorumEvent());
                    break;
                case WAIT_FOR_BROKERS:
                    eventQueue.append(new WaitForZkBrokersEvent());
                    break;
                case BECOME_CONTROLLER:
                    eventQueue.append(new BecomeZkControllerEvent(eventLeaderAndEpoch));
                    break;
                case ZK_MIGRATION:
                    eventQueue.append(new MigrateMetadataEvent(eventLeaderAndEpoch));
                    break;
                case SYNC_KRAFT_TO_ZK:
                    eventQueue.append(new SyncKRaftMetadataEvent(eventLeaderAndEpoch));
                    break;
                case KRAFT_CONTROLLER_TO_BROKER_COMM:
                    eventQueue.append(new SendRPCsToBrokersEvent(eventLeaderAndEpoch));
                    break;
                case DUAL_WRITE:
                    // Nothing to do in the PollEvent. If there's metadata change, we use
                    // MetadataChange event to drive the writes to Zookeeper.
                    break;
            }

            // Poll again after some time
            long deadline = time.nanoseconds() + NANOSECONDS.convert(pollTimeSupplier.nextPollTimeMs(), MILLISECONDS);
            eventQueue.scheduleDeferred(
                    "poll",
                    new EventQueue.DeadlineFunction(deadline),
                    new PollEvent());
        }
    }

    static KRaftMigrationOperationConsumer countingOperationConsumer(
        Map<String, Integer> dualWriteCounts,
        BiConsumer<String, KRaftMigrationOperation> operationConsumer
    ) {
        return (opType, logMsg, operation) -> {
            dualWriteCounts.compute(opType, (key, value) -> {
                if (value == null) {
                    return 1;
                } else {
                    return value + 1;
                }
            });
            operationConsumer.accept(logMsg, operation);
        };
    }

    public static class Builder {
        private Integer nodeId;
        private ZkRecordConsumer zkRecordConsumer;
        private MigrationClient zkMigrationClient;
        private LegacyPropagator propagator;
        private Consumer<MetadataPublisher> initialZkLoadHandler;
        private FaultHandler faultHandler;
        private QuorumFeatures quorumFeatures;
        private KafkaConfigSchema configSchema;
        private QuorumControllerMetrics controllerMetrics;
        private Integer minBatchSize;
        private Time time;

        public Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder setZkRecordConsumer(ZkRecordConsumer zkRecordConsumer) {
            this.zkRecordConsumer = zkRecordConsumer;
            return this;
        }

        public Builder setZkMigrationClient(MigrationClient zkMigrationClient) {
            this.zkMigrationClient = zkMigrationClient;
            return this;
        }

        public Builder setPropagator(LegacyPropagator propagator) {
            this.propagator = propagator;
            return this;
        }

        public Builder setInitialZkLoadHandler(Consumer<MetadataPublisher> initialZkLoadHandler) {
            this.initialZkLoadHandler = initialZkLoadHandler;
            return this;
        }

        public Builder setFaultHandler(FaultHandler faultHandler) {
            this.faultHandler = faultHandler;
            return this;
        }

        public Builder setQuorumFeatures(QuorumFeatures quorumFeatures) {
            this.quorumFeatures = quorumFeatures;
            return this;
        }

        public Builder setConfigSchema(KafkaConfigSchema configSchema) {
            this.configSchema = configSchema;
            return this;
        }

        public Builder setControllerMetrics(QuorumControllerMetrics controllerMetrics) {
            this.controllerMetrics = controllerMetrics;
            return this;
        }

        public Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder setMinMigrationBatchSize(int minBatchSize) {
            this.minBatchSize = minBatchSize;
            return this;
        }

        public KRaftMigrationDriver build() {
            if (nodeId == null) {
                throw new IllegalStateException("You must specify the node ID of this controller.");
            }
            if (zkRecordConsumer == null) {
                throw new IllegalStateException("You must specify the ZkRecordConsumer.");
            }
            if (zkMigrationClient == null) {
                throw new IllegalStateException("You must specify the MigrationClient.");
            }
            if (propagator == null) {
                throw new IllegalStateException("You must specify the MetadataPropagator.");
            }
            if (initialZkLoadHandler == null) {
                throw new IllegalStateException("You must specify the initial ZK load callback.");
            }
            if (faultHandler == null) {
                throw new IllegalStateException("You must specify the FaultHandler.");
            }
            if (configSchema == null) {
                throw new IllegalStateException("You must specify the KafkaConfigSchema.");
            }
            if (controllerMetrics == null) {
                throw new IllegalStateException("You must specify the QuorumControllerMetrics.");
            }
            if (time == null) {
                throw new IllegalStateException("You must specify the Time.");
            }
            if (minBatchSize == null) {
                minBatchSize = 200;
            }
            return new KRaftMigrationDriver(
                nodeId,
                zkRecordConsumer,
                zkMigrationClient,
                propagator,
                initialZkLoadHandler,
                faultHandler,
                quorumFeatures,
                configSchema,
                controllerMetrics,
                minBatchSize,
                time
            );
        }
    }
}
