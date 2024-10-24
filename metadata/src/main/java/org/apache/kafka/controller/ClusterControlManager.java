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

package org.apache.kafka.controller;

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.BrokerIdNotRegisteredException;
import org.apache.kafka.common.errors.DuplicateBrokerRegistrationException;
import org.apache.kafka.common.errors.InconsistentClusterIdException;
import org.apache.kafka.common.errors.InvalidRegistrationException;
import org.apache.kafka.common.errors.StaleBrokerEpochException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.ControllerRegistrationRequestData;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerFeature;
import org.apache.kafka.common.metadata.RegisterControllerRecord;
import org.apache.kafka.common.metadata.RegisterControllerRecord.ControllerFeatureCollection;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metadata.ControllerRegistration;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.ListenerInfo;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.metadata.placement.ReplicaPlacer;
import org.apache.kafka.metadata.placement.StripedReplicaPlacer;
import org.apache.kafka.metadata.placement.UsableBroker;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.slf4j.Logger;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.NANOSECONDS;


/**
 * The ClusterControlManager manages all the hard state associated with the Kafka cluster.
 * Hard state is state which appears in the metadata log, such as broker registrations,
 * brokers being fenced or unfenced, and broker feature versions.
 */
public class ClusterControlManager {
    static final long DEFAULT_SESSION_TIMEOUT_NS = NANOSECONDS.convert(9, TimeUnit.SECONDS);

    static class Builder {
        private LogContext logContext = null;
        private String clusterId = null;
        private Time time = Time.SYSTEM;
        private SnapshotRegistry snapshotRegistry = null;
        private long sessionTimeoutNs = DEFAULT_SESSION_TIMEOUT_NS;
        private ReplicaPlacer replicaPlacer = null;
        private FeatureControlManager featureControl = null;
        private BrokerUncleanShutdownHandler brokerUncleanShutdownHandler = null;
        private String interBrokerListenerName = "PLAINTEXT";

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setClusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder setSessionTimeoutNs(long sessionTimeoutNs) {
            this.sessionTimeoutNs = sessionTimeoutNs;
            return this;
        }

        Builder setReplicaPlacer(ReplicaPlacer replicaPlacer) {
            this.replicaPlacer = replicaPlacer;
            return this;
        }

        Builder setFeatureControlManager(FeatureControlManager featureControl) {
            this.featureControl = featureControl;
            return this;
        }

        Builder setBrokerUncleanShutdownHandler(BrokerUncleanShutdownHandler brokerUncleanShutdownHandler) {
            this.brokerUncleanShutdownHandler = brokerUncleanShutdownHandler;
            return this;
        }

        Builder setInterBrokerListenerName(String interBrokerListenerName) {
            this.interBrokerListenerName = interBrokerListenerName;
            return this;
        }

        ClusterControlManager build() {
            if (logContext == null) {
                logContext = new LogContext();
            }
            if (clusterId == null) {
                clusterId = Uuid.randomUuid().toString();
            }
            if (snapshotRegistry == null) {
                snapshotRegistry = new SnapshotRegistry(logContext);
            }
            if (replicaPlacer == null) {
                replicaPlacer = new StripedReplicaPlacer(new Random());
            }
            if (featureControl == null) {
                throw new RuntimeException("You must specify FeatureControlManager");
            }
            if (brokerUncleanShutdownHandler == null) {
                throw new RuntimeException("You must specify BrokerUncleanShutdownHandler");
            }
            return new ClusterControlManager(logContext,
                clusterId,
                time,
                snapshotRegistry,
                sessionTimeoutNs,
                replicaPlacer,
                featureControl,
                brokerUncleanShutdownHandler,
                interBrokerListenerName
            );
        }
    }

    class ReadyBrokersFuture {
        private final CompletableFuture<Void> future;
        private final int minBrokers;

        ReadyBrokersFuture(CompletableFuture<Void> future, int minBrokers) {
            this.future = future;
            this.minBrokers = minBrokers;
        }

        boolean check() {
            int numUnfenced = 0;
            for (BrokerRegistration registration : brokerRegistrations.values()) {
                if (!registration.fenced()) {
                    numUnfenced++;
                }
                if (numUnfenced >= minBrokers) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * The SLF4J log context.
     */
    private final LogContext logContext;

    /**
     * The ID of this cluster.
     */
    private final String clusterId;

    /**
     * The SLF4J log object.
     */
    private final Logger log;

    /**
     * The Kafka clock object to use.
     */
    private final Time time;

    /**
     * How long sessions should last, in nanoseconds.
     */
    private final long sessionTimeoutNs;

    /**
     * The replica placer to use.
     */
    private final ReplicaPlacer replicaPlacer;

    /**
     * Maps broker IDs to broker registrations.
     */
    private final TimelineHashMap<Integer, BrokerRegistration> brokerRegistrations;

    /**
     * Save the offset of each broker registration record, we will only unfence a
     * broker when its high watermark has reached its broker registration record,
     * this is not necessarily the exact offset of each broker registration record
     * but should not be smaller than it.
     */
    private final TimelineHashMap<Integer, Long> registerBrokerRecordOffsets;

    /**
     * The broker heartbeat manager, or null if this controller is on standby.
     */
    private volatile BrokerHeartbeatManager heartbeatManager;

    /**
     * A future which is completed as soon as we have the given number of brokers
     * ready.
     */
    private Optional<ReadyBrokersFuture> readyBrokersFuture;

    /**
     * The feature control manager.
     */
    private final FeatureControlManager featureControl;

    private final BrokerUncleanShutdownHandler brokerUncleanShutdownHandler;

    /**
     * The statically configured inter-broker listener name.
     */
    private final String interBrokerListenerName;

    /**
     * Maps controller IDs to controller registrations.
     */
    private final TimelineHashMap<Integer, ControllerRegistration> controllerRegistrations;

    /**
     * Maps directories to their respective brokers.
     */
    private final TimelineHashMap<Uuid, Integer> directoryToBroker;

    private ClusterControlManager(
        LogContext logContext,
        String clusterId,
        Time time,
        SnapshotRegistry snapshotRegistry,
        long sessionTimeoutNs,
        ReplicaPlacer replicaPlacer,
        FeatureControlManager featureControl,
        BrokerUncleanShutdownHandler brokerUncleanShutdownHandler,
        String interBrokerListenerName
    ) {
        this.logContext = logContext;
        this.clusterId = clusterId;
        this.log = logContext.logger(ClusterControlManager.class);
        this.time = time;
        this.sessionTimeoutNs = sessionTimeoutNs;
        this.replicaPlacer = replicaPlacer;
        this.brokerRegistrations = new TimelineHashMap<>(snapshotRegistry, 0);
        this.registerBrokerRecordOffsets = new TimelineHashMap<>(snapshotRegistry, 0);
        this.heartbeatManager = null;
        this.readyBrokersFuture = Optional.empty();
        this.featureControl = featureControl;
        this.controllerRegistrations = new TimelineHashMap<>(snapshotRegistry, 0);
        this.directoryToBroker = new TimelineHashMap<>(snapshotRegistry, 0);
        this.brokerUncleanShutdownHandler = brokerUncleanShutdownHandler;
        this.interBrokerListenerName = interBrokerListenerName;
    }

    ReplicaPlacer replicaPlacer() {
        return replicaPlacer;
    }

    /**
     * Transition this ClusterControlManager to active.
     */
    public void activate() {
        heartbeatManager = new BrokerHeartbeatManager(logContext, time, sessionTimeoutNs);
        for (BrokerRegistration registration : brokerRegistrations.values()) {
            heartbeatManager.register(registration.id(), registration.fenced());
        }
    }

    String clusterId() { // Visible for testing
        return clusterId;
    }

    /**
     * Transition this ClusterControlManager to standby.
     */
    public void deactivate() {
        heartbeatManager = null;
    }

    Map<Integer, BrokerRegistration> brokerRegistrations() {
        return brokerRegistrations;
    }

    Set<Integer> fencedBrokerIds() {
        return brokerRegistrations.values()
            .stream()
            .filter(BrokerRegistration::fenced)
            .map(BrokerRegistration::id)
            .collect(Collectors.toSet());
    }

    /**
     * Process an incoming broker registration request.
     */
    public ControllerResult<BrokerRegistrationReply> registerBroker(
        BrokerRegistrationRequestData request,
        long newBrokerEpoch,
        FinalizedControllerFeatures finalizedFeatures
    ) {
        if (heartbeatManager == null) {
            throw new RuntimeException("ClusterControlManager is not active.");
        }
        if (!clusterId.equals(request.clusterId())) {
            throw new InconsistentClusterIdException("Expected cluster ID " + clusterId +
                ", but got cluster ID " + request.clusterId());
        }
        int brokerId = request.brokerId();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        BrokerRegistration existing = brokerRegistrations.get(brokerId);
        Uuid prevIncarnationId = null;
        if (existing != null) {
            prevIncarnationId = existing.incarnationId();
            if (heartbeatManager.hasValidSession(brokerId, existing.epoch())) {
                if (!request.incarnationId().equals(prevIncarnationId)) {
                    throw new DuplicateBrokerRegistrationException("Another broker is " +
                        "registered with that broker id.");
                }
            }
        }

        if (request.isMigratingZkBroker()) {
            throw new BrokerIdNotRegisteredException("Controller does not support registering ZK brokers.");
        }

        if (featureControl.metadataVersion().isDirectoryAssignmentSupported()) {
            if (request.logDirs().isEmpty()) {
                throw new InvalidRegistrationException("No directories specified in request");
            }
            if (request.logDirs().stream().anyMatch(DirectoryId::reserved)) {
                throw new InvalidRegistrationException("Reserved directory ID in request");
            }
            Set<Uuid> set = new HashSet<>(request.logDirs());
            if (set.size() != request.logDirs().size()) {
                throw new InvalidRegistrationException("Duplicate directory ID in request");
            }
            for (Uuid directory : request.logDirs()) {
                Integer dirBrokerId = directoryToBroker.get(directory);
                if (dirBrokerId != null && dirBrokerId != brokerId) {
                    throw new InvalidRegistrationException("Broker " + dirBrokerId +
                            " is already registered with directory " + directory);
                }
            }
        }

        ListenerInfo listenerInfo = ListenerInfo.fromBrokerRegistrationRequest(request.listeners());
        RegisterBrokerRecord record = new RegisterBrokerRecord().
            setBrokerId(brokerId).
            setIsMigratingZkBroker(request.isMigratingZkBroker()).
            setIncarnationId(request.incarnationId()).
            setRack(request.rack()).
            setEndPoints(listenerInfo.toBrokerRegistrationRecord());

        // Track which finalized features we have not yet verified are supported by the broker.
        Map<String, Short> unverifiedFeatures = new HashMap<>(finalizedFeatures.featureMap());

        // Check every broker feature version range includes the finalized version.
        for (BrokerRegistrationRequestData.Feature feature : request.features()) {
            record.features().add(processRegistrationFeature(brokerId, finalizedFeatures, feature));
            unverifiedFeatures.remove(feature.name());
        }
        // Brokers that don't send a supported metadata.version range are assumed to only
        // support the original metadata.version.
        if (request.features().find(MetadataVersion.FEATURE_NAME) == null) {
            record.features().add(processRegistrationFeature(brokerId, finalizedFeatures,
                new BrokerRegistrationRequestData.Feature().
                    setName(MetadataVersion.FEATURE_NAME).
                    setMinSupportedVersion(MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel()).
                    setMaxSupportedVersion(MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel())));
            unverifiedFeatures.remove(MetadataVersion.FEATURE_NAME);
        }
        // We also need to check every controller feature is supported by the broker.
        unverifiedFeatures.forEach((featureName, finalizedVersion) -> {
            if (finalizedVersion != 0 && request.features().findAll(featureName).isEmpty()) {
                processRegistrationFeature(brokerId, finalizedFeatures,
                    new BrokerRegistrationRequestData.Feature().
                        setName(featureName).
                        setMinSupportedVersion((short) 0).
                        setMaxSupportedVersion((short) 0));
            }
        });
        if (featureControl.metadataVersion().isDirectoryAssignmentSupported()) {
            record.setLogDirs(request.logDirs());
        }

        if (!request.incarnationId().equals(prevIncarnationId)) {
            int prevNumRecords = records.size();
            brokerUncleanShutdownHandler.addRecordsForShutdown(request.brokerId(), records);
            int numRecordsAdded = records.size() - prevNumRecords;
            if (existing == null) {
                log.info("No previous registration found for broker {}. New incarnation ID is " +
                        "{}.  Generated {} record(s) to clean up previous incarnations. New broker " +
                        "epoch is {}.", brokerId, request.incarnationId(), numRecordsAdded, newBrokerEpoch);
            } else {
                log.info("Registering a new incarnation of broker {}. Previous incarnation ID " +
                        "was {}; new incarnation ID is {}. Generated {} record(s) to clean up " +
                        "previous incarnations. Broker epoch will become {}.", brokerId,
                        existing.incarnationId(), request.incarnationId(), numRecordsAdded,
                        newBrokerEpoch);
            }
            record.setBrokerEpoch(newBrokerEpoch);
        } else {
            log.info("Amending registration of broker {}, incarnation ID {}. Broker epoch remains {}.",
                    request.brokerId(), request.incarnationId(), existing.epoch());
            record.setFenced(existing.fenced());
            record.setInControlledShutdown(existing.inControlledShutdown());
            record.setBrokerEpoch(existing.epoch());
        }
        records.add(new ApiMessageAndVersion(record, featureControl.metadataVersion().
            registerBrokerRecordVersion()));

        if (!request.incarnationId().equals(prevIncarnationId)) {
            // Remove any existing session for the old broker incarnation.
            heartbeatManager.remove(brokerId);
        }
        heartbeatManager.register(brokerId, record.fenced());

        return ControllerResult.atomicOf(records, new BrokerRegistrationReply(record.brokerEpoch()));
    }

    ControllerResult<Void> registerController(ControllerRegistrationRequestData request) {
        if (!featureControl.metadataVersion().isControllerRegistrationSupported()) {
            throw new UnsupportedVersionException("The current MetadataVersion is too old to " +
                    "support controller registrations.");
        }
        ListenerInfo listenerInfo = ListenerInfo.fromControllerRegistrationRequest(request.listeners());
        ControllerFeatureCollection features = new ControllerFeatureCollection();
        request.features().forEach(feature ->
            features.add(new RegisterControllerRecord.ControllerFeature().
                setName(feature.name()).
                setMaxSupportedVersion(feature.maxSupportedVersion()).
                setMinSupportedVersion(feature.minSupportedVersion()))
        );
        List<ApiMessageAndVersion> records = new ArrayList<>();
        records.add(new ApiMessageAndVersion(new RegisterControllerRecord().
            setControllerId(request.controllerId()).
            setIncarnationId(request.incarnationId()).
            setZkMigrationReady(false).
            setEndPoints(listenerInfo.toControllerRegistrationRecord()).
            setFeatures(features),
                (short) 0));
        return ControllerResult.atomicOf(records, null);
    }

    BrokerFeature processRegistrationFeature(
        int brokerId,
        FinalizedControllerFeatures finalizedFeatures,
        BrokerRegistrationRequestData.Feature feature
    ) {
        int defaultVersion = feature.name().equals(MetadataVersion.FEATURE_NAME) ? 1 : 0; // The default value for MetadataVersion is 1 not 0.
        short finalized = finalizedFeatures.versionOrDefault(feature.name(), (short) defaultVersion);
        if (!VersionRange.of(feature.minSupportedVersion(), feature.maxSupportedVersion()).contains(finalized)) {
            throw new UnsupportedVersionException("Unable to register because the broker " +
                "does not support finalized version " + finalized + " of " + feature.name() +
                    ". The broker wants a version between " + feature.minSupportedVersion() + " and " +
                    feature.maxSupportedVersion() + ", inclusive.");
        }
        // A feature is not found in the finalizedFeature map if it is unknown to the controller or set to 0 (feature not enabled).
        if (!finalizedFeatures.featureNames().contains(feature.name()))
            log.debug("Broker {} registered with version range ({}, {}] of feature {} which controller does not know " +
                    "or has finalized version of 0.",
                    brokerId, feature.minSupportedVersion(), feature.maxSupportedVersion(), feature.name());
        return new BrokerFeature().
                setName(feature.name()).
                setMinSupportedVersion(feature.minSupportedVersion()).
                setMaxSupportedVersion(feature.maxSupportedVersion());
    }

    /**
     * Track an incoming broker heartbeat. Unlike most functions, this one is not called from the main
     * controller thread, so it can only access local, volatile and atomic data.
     *
     * @param brokerId      The broker id to track.
     * @param brokerEpoch   The broker epoch to track.
     *
     * @returns             True only if the ClusterControlManager is active.
     */
    boolean trackBrokerHeartbeat(int brokerId, long brokerEpoch) {
        BrokerHeartbeatManager manager = heartbeatManager;
        if (manager == null) return false;
        manager.tracker().updateContactTime(new BrokerIdAndEpoch(brokerId, brokerEpoch));
        return true;
    }

    public OptionalLong registerBrokerRecordOffset(int brokerId) {
        Long registrationOffset = registerBrokerRecordOffsets.get(brokerId);
        if (registrationOffset != null) {
            return OptionalLong.of(registrationOffset);
        }
        return OptionalLong.empty();
    }

    public void replay(RegisterBrokerRecord record, long offset) {
        registerBrokerRecordOffsets.put(record.brokerId(), offset);
        int brokerId = record.brokerId();
        ListenerInfo listenerInfo = ListenerInfo.fromBrokerRegistrationRecord(record.endPoints());
        Map<String, VersionRange> features = new HashMap<>();
        for (BrokerFeature feature : record.features()) {
            features.put(feature.name(), VersionRange.of(
                feature.minSupportedVersion(), feature.maxSupportedVersion()));
        }
        // Update broker registrations.
        BrokerRegistration prevRegistration = brokerRegistrations.put(brokerId,
            new BrokerRegistration.Builder().
                setId(brokerId).
                setEpoch(record.brokerEpoch()).
                setIncarnationId(record.incarnationId()).
                setListeners(listenerInfo.listeners()).
                setSupportedFeatures(features).
                setRack(Optional.ofNullable(record.rack())).
                setFenced(record.fenced()).
                setInControlledShutdown(record.inControlledShutdown()).
                setIsMigratingZkBroker(record.isMigratingZkBroker()).
                setDirectories(record.logDirs()).
                    build());
        updateDirectories(brokerId, prevRegistration == null ? null : prevRegistration.directories(), record.logDirs());
        if (heartbeatManager != null) {
            if (prevRegistration != null) heartbeatManager.remove(brokerId);
            heartbeatManager.register(brokerId, record.fenced());
        }
        if (prevRegistration == null) {
            log.info("Replayed initial RegisterBrokerRecord for broker {}: {}", record.brokerId(), record);
        } else if (prevRegistration.incarnationId().equals(record.incarnationId())) {
            log.info("Replayed RegisterBrokerRecord modifying the registration for broker {}: {}",
                record.brokerId(), record);
        } else {
            log.info("Replayed RegisterBrokerRecord establishing a new incarnation of broker {}: {}",
                record.brokerId(), record);
        }
    }

    public void replay(UnregisterBrokerRecord record) {
        registerBrokerRecordOffsets.remove(record.brokerId());
        int brokerId = record.brokerId();
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration found for that id", record));
        } else if (registration.epoch() != record.brokerEpoch()) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration with that epoch found", record));
        } else {
            if (heartbeatManager != null) heartbeatManager.remove(brokerId);
            updateDirectories(brokerId, registration.directories(), null);
            brokerRegistrations.remove(brokerId);
            log.info("Replayed {}", record);
        }
    }

    public void replay(FenceBrokerRecord record) {
        replayRegistrationChange(
            record,
            record.id(),
            record.epoch(),
            BrokerRegistrationFencingChange.FENCE.asBoolean(),
            BrokerRegistrationInControlledShutdownChange.NONE.asBoolean(),
            Optional.empty()
        );
    }

    public void replay(UnfenceBrokerRecord record) {
        replayRegistrationChange(
            record,
            record.id(),
            record.epoch(),
            BrokerRegistrationFencingChange.UNFENCE.asBoolean(),
            BrokerRegistrationInControlledShutdownChange.NONE.asBoolean(),
            Optional.empty()
        );
    }

    public void replay(BrokerRegistrationChangeRecord record) {
        BrokerRegistrationFencingChange fencingChange =
            BrokerRegistrationFencingChange.fromValue(record.fenced()).orElseThrow(
                () -> new IllegalStateException(String.format("Unable to replay %s: unknown " +
                    "value for fenced field: %x", record, record.fenced())));
        BrokerRegistrationInControlledShutdownChange inControlledShutdownChange =
            BrokerRegistrationInControlledShutdownChange.fromValue(record.inControlledShutdown()).orElseThrow(
                () -> new IllegalStateException(String.format("Unable to replay %s: unknown " +
                    "value for inControlledShutdown field: %x", record, record.inControlledShutdown())));
        Optional<List<Uuid>> directoriesChange = Optional.ofNullable(record.logDirs()).filter(list -> !list.isEmpty());
        replayRegistrationChange(
            record,
            record.brokerId(),
            record.brokerEpoch(),
            fencingChange.asBoolean(),
            inControlledShutdownChange.asBoolean(),
            directoriesChange
        );
    }

    private void replayRegistrationChange(
        ApiMessage record,
        int brokerId,
        long brokerEpoch,
        Optional<Boolean> fencingChange,
        Optional<Boolean> inControlledShutdownChange,
        Optional<List<Uuid>> directoriesChange
    ) {
        BrokerRegistration curRegistration = brokerRegistrations.get(brokerId);
        if (curRegistration == null) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration found for that id", record.toString()));
        } else if (curRegistration.epoch() != brokerEpoch) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration with that epoch found", record.toString()));
        } else {
            BrokerRegistration nextRegistration = curRegistration.cloneWith(
                fencingChange,
                inControlledShutdownChange,
                directoriesChange
            );
            if (!curRegistration.equals(nextRegistration)) {
                log.info("Replayed {} modifying the registration for broker {}: {}",
                        record.getClass().getSimpleName(), brokerId, record);
                brokerRegistrations.put(brokerId, nextRegistration);
            } else {
                log.info("Ignoring no-op registration change for {}", curRegistration);
            }
            updateDirectories(brokerId, curRegistration.directories(), nextRegistration.directories());
            if (heartbeatManager != null) heartbeatManager.register(brokerId, nextRegistration.fenced());
            if (readyBrokersFuture.isPresent()) {
                if (readyBrokersFuture.get().check()) {
                    readyBrokersFuture.get().future.complete(null);
                    readyBrokersFuture = Optional.empty();
                }
            }
        }
    }

    public void replay(RegisterControllerRecord record) {
        ControllerRegistration newRegistration = new ControllerRegistration.Builder(record).build();
        ControllerRegistration prevRegistration =
            controllerRegistrations.put(record.controllerId(), newRegistration);
        log.info("Replayed RegisterControllerRecord contaning {}.{}", newRegistration,
            prevRegistration == null ? "" :
                " Previous incarnation was " + prevRegistration.incarnationId());
    }

    Iterator<UsableBroker> usableBrokers() {
        if (heartbeatManager == null) {
            throw new RuntimeException("ClusterControlManager is not active.");
        }
        return heartbeatManager.usableBrokers(
            id -> brokerRegistrations.get(id).rack());
    }

    /**
     * Returns true if the broker is unfenced; Returns false if it is
     * not or if it does not exist.
     */
    public boolean isUnfenced(int brokerId) {
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) return false;
        return !registration.fenced();
    }

    /**
     * Get a broker registration if it exists.
     *
     * @param brokerId The brokerId to get the registration for
     * @return The current registration or null if the broker is not registered
     */
    public BrokerRegistration registration(int brokerId) {
        return brokerRegistrations.get(brokerId);
    }

    /**
     * Returns true if the broker is in controlled shutdown state; Returns false
     * if it is not or if it does not exist.
     */
    public boolean inControlledShutdown(int brokerId) {
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) return false;
        return registration.inControlledShutdown();
    }

    /**
     * Returns true if the broker is active. Active means not fenced nor in controlled
     * shutdown; Returns false if it is not active or if it does not exist.
     */
    public boolean isActive(int brokerId) {
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) return false;
        return !registration.inControlledShutdown() && !registration.fenced();
    }

    BrokerHeartbeatManager heartbeatManager() {
        // We throw RuntimeException here rather than NotControllerException because all the callers
        // have already verified that we are active. For example, ControllerWriteEvent.run verifies
        // that we are the current active controller before running any event-specific code.
        if (heartbeatManager == null) {
            throw new RuntimeException("ClusterControlManager is not active.");
        }
        return heartbeatManager;
    }

    public void checkBrokerEpoch(int brokerId, long brokerEpoch) {
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) {
            throw new StaleBrokerEpochException("No broker registration found for " +
                "broker id " + brokerId);
        }
        if (registration.epoch() != brokerEpoch) {
            throw new StaleBrokerEpochException("Expected broker epoch " +
                registration.epoch() + ", but got broker epoch " + brokerEpoch);
        }
    }

    public void addReadyBrokersFuture(CompletableFuture<Void> future, int minBrokers) {
        readyBrokersFuture = Optional.of(new ReadyBrokersFuture(future, minBrokers));
        if (readyBrokersFuture.get().check()) {
            readyBrokersFuture.get().future.complete(null);
            readyBrokersFuture = Optional.empty();
        }
    }

    /**
     * Determines whether the specified directory is online in the specified broker.
     * Returns false whether the directory is not online or the broker is not registered.
     */
    public boolean hasOnlineDir(int brokerId, Uuid directoryId) {
        BrokerRegistration registration = registration(brokerId);
        return registration != null && registration.hasOnlineDir(directoryId);
    }

    /**
     * Determines the default directory for new partitions placed on a given broker.
     */
    public Uuid defaultDir(int brokerId) {
        BrokerRegistration registration = registration(brokerId);
        if (registration == null) {
            // throwing an exception here can break the expected error from an
            // Admin call, so instead, we return UNASSIGNED, and let the fact
            // that the broker is not registered be handled elsewhere.
            return DirectoryId.UNASSIGNED;
        }
        List<Uuid> directories = registration.directories();
        if (directories.isEmpty()) {
            return DirectoryId.MIGRATING;
        }
        if (directories.size() == 1) {
            return directories.get(0);
        }
        return DirectoryId.UNASSIGNED;
    }

    void updateDirectories(int brokerId, List<Uuid> dirsToRemove, List<Uuid> dirsToAdd) {
        if (dirsToRemove != null) {
            for (Uuid directory : dirsToRemove) {
                if (!directoryToBroker.remove(directory, brokerId)) {
                    throw new IllegalStateException("BUG: directory " + directory + " not assigned to broker " + brokerId);
                }
            }
        }
        if (dirsToAdd != null) {
            for (Uuid directory : dirsToAdd) {
                Integer existingId = directoryToBroker.putIfAbsent(directory, brokerId);
                if (existingId != null && existingId != brokerId) {
                    throw new IllegalStateException("BUG: directory " + directory + " already assigned to broker " + existingId);
                }
            }
        }
    }

    Iterator<Entry<Integer, Map<String, VersionRange>>> brokerSupportedFeatures() {
        return new Iterator<Entry<Integer, Map<String, VersionRange>>>() {
            private final Iterator<BrokerRegistration> iter = brokerRegistrations.values().iterator();

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public Entry<Integer, Map<String, VersionRange>> next() {
                BrokerRegistration registration = iter.next();
                return new AbstractMap.SimpleImmutableEntry<>(registration.id(),
                        registration.supportedFeatures());
            }
        };
    }

    Iterator<Entry<Integer, Map<String, VersionRange>>> controllerSupportedFeatures() {
        if (!featureControl.metadataVersion().isControllerRegistrationSupported()) {
            throw new UnsupportedVersionException("The current MetadataVersion is too old to " +
                    "support controller registrations.");
        }
        return new Iterator<Entry<Integer, Map<String, VersionRange>>>() {
            private final Iterator<ControllerRegistration> iter = controllerRegistrations.values().iterator();

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public Entry<Integer, Map<String, VersionRange>> next() {
                ControllerRegistration registration = iter.next();
                return new AbstractMap.SimpleImmutableEntry<>(registration.id(),
                        registration.supportedFeatures());
            }
        };
    }

    @FunctionalInterface
    interface BrokerUncleanShutdownHandler {
        void addRecordsForShutdown(int brokerId, List<ApiMessageAndVersion> records);
    }
}
