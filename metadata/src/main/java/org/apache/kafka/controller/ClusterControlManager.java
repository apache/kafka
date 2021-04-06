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

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.errors.DuplicateBrokerRegistrationException;
import org.apache.kafka.common.errors.StaleBrokerEpochException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpointCollection;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerFeature;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerFeatureCollection;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metadata.FeatureMapAndEpoch;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;


/**
 * The ClusterControlManager manages all the hard state associated with the Kafka cluster.
 * Hard state is state which appears in the metadata log, such as broker registrations,
 * brokers being fenced or unfenced, and broker feature versions.
 */
public class ClusterControlManager {
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
     * The replica placement policy to use.
     */
    private final ReplicaPlacementPolicy placementPolicy;

    /**
     * Maps broker IDs to broker registrations.
     */
    private final TimelineHashMap<Integer, BrokerRegistration> brokerRegistrations;

    /**
     * The broker heartbeat manager, or null if this controller is on standby.
     */
    private BrokerHeartbeatManager heartbeatManager;

    /**
     * A future which is completed as soon as we have the given number of brokers
     * ready.
     */
    private Optional<ReadyBrokersFuture> readyBrokersFuture;

    ClusterControlManager(LogContext logContext,
                          Time time,
                          SnapshotRegistry snapshotRegistry,
                          long sessionTimeoutNs,
                          ReplicaPlacementPolicy placementPolicy) {
        this.logContext = logContext;
        this.log = logContext.logger(ClusterControlManager.class);
        this.time = time;
        this.sessionTimeoutNs = sessionTimeoutNs;
        this.placementPolicy = placementPolicy;
        this.brokerRegistrations = new TimelineHashMap<>(snapshotRegistry, 0);
        this.heartbeatManager = null;
        this.readyBrokersFuture = Optional.empty();
    }

    /**
     * Transition this ClusterControlManager to active.
     */
    public void activate() {
        heartbeatManager = new BrokerHeartbeatManager(logContext, time, sessionTimeoutNs);
        for (BrokerRegistration registration : brokerRegistrations.values()) {
            heartbeatManager.touch(registration.id(), registration.fenced(), -1);
        }
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

    /**
     * Process an incoming broker registration request.
     */
    public ControllerResult<BrokerRegistrationReply> registerBroker(
            BrokerRegistrationRequestData request,
            long brokerEpoch,
            FeatureMapAndEpoch finalizedFeatures) {
        if (heartbeatManager == null) {
            throw new RuntimeException("ClusterControlManager is not active.");
        }
        int brokerId = request.brokerId();
        BrokerRegistration existing = brokerRegistrations.get(brokerId);
        if (existing != null) {
            if (heartbeatManager.hasValidSession(brokerId)) {
                if (!existing.incarnationId().equals(request.incarnationId())) {
                    throw new DuplicateBrokerRegistrationException("Another broker is " +
                        "registered with that broker id.");
                }
            } else {
                if (!existing.incarnationId().equals(request.incarnationId())) {
                    // Remove any existing session for the old broker incarnation.
                    heartbeatManager.remove(brokerId);
                    existing = null;
                }
            }
        }

        RegisterBrokerRecord record = new RegisterBrokerRecord().setBrokerId(brokerId).
            setIncarnationId(request.incarnationId()).
            setBrokerEpoch(brokerEpoch).
            setRack(request.rack());
        for (BrokerRegistrationRequestData.Listener listener : request.listeners()) {
            record.endPoints().add(new BrokerEndpoint().
                setHost(listener.host()).
                setName(listener.name()).
                setPort(listener.port()).
                setSecurityProtocol(listener.securityProtocol()));
        }
        for (BrokerRegistrationRequestData.Feature feature : request.features()) {
            Optional<VersionRange> finalized = finalizedFeatures.map().get(feature.name());
            if (finalized.isPresent()) {
                if (!finalized.get().contains(new VersionRange(feature.minSupportedVersion(),
                        feature.maxSupportedVersion()))) {
                    throw new UnsupportedVersionException("Unable to register because " +
                        "the broker has an unsupported version of " + feature.name());
                }
            }
            record.features().add(new BrokerFeature().
                setName(feature.name()).
                setMinSupportedVersion(feature.minSupportedVersion()).
                setMaxSupportedVersion(feature.maxSupportedVersion()));
        }

        if (existing == null) {
            heartbeatManager.touch(brokerId, true, -1);
        } else {
            heartbeatManager.touch(brokerId, existing.fenced(), -1);
        }

        List<ApiMessageAndVersion> records = new ArrayList<>();
        records.add(new ApiMessageAndVersion(record, (short) 0));
        return ControllerResult.of(records, new BrokerRegistrationReply(brokerEpoch));
    }

    public void replay(RegisterBrokerRecord record) {
        int brokerId = record.brokerId();
        List<Endpoint> listeners = new ArrayList<>();
        for (BrokerEndpoint endpoint : record.endPoints()) {
            listeners.add(new Endpoint(endpoint.name(),
                SecurityProtocol.forId(endpoint.securityProtocol()),
                endpoint.host(), endpoint.port()));
        }
        Map<String, VersionRange> features = new HashMap<>();
        for (BrokerFeature feature : record.features()) {
            features.put(feature.name(), new VersionRange(
                feature.minSupportedVersion(), feature.maxSupportedVersion()));
        }
        // Normally, all newly registered brokers start off in the fenced state.
        // If this registration record is for a broker incarnation that was already
        // registered, though, we preserve the existing fencing state.
        boolean fenced = true;
        BrokerRegistration prevRegistration = brokerRegistrations.get(brokerId);
        if (prevRegistration != null &&
                prevRegistration.incarnationId().equals(record.incarnationId())) {
            fenced = prevRegistration.fenced();
        }
        // Update broker registrations.
        brokerRegistrations.put(brokerId, new BrokerRegistration(brokerId,
            record.brokerEpoch(), record.incarnationId(), listeners, features,
            Optional.ofNullable(record.rack()), fenced));

        if (prevRegistration == null) {
            log.info("Registered new broker: {}", record);
        } else if (prevRegistration.incarnationId().equals(record.incarnationId())) {
            log.info("Re-registered broker incarnation: {}", record);
        } else {
            log.info("Re-registered broker id {}: {}", brokerId, record);
        }
    }

    public void replay(UnregisterBrokerRecord record) {
        int brokerId = record.brokerId();
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration found for that id", record.toString()));
        } else if (registration.epoch() !=  record.brokerEpoch()) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration with that epoch found", record.toString()));
        } else {
            brokerRegistrations.remove(brokerId);
            log.info("Unregistered broker: {}", record);
        }
    }

    public void replay(FenceBrokerRecord record) {
        int brokerId = record.id();
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration found for that id", record.toString()));
        } else if (registration.epoch() !=  record.epoch()) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration with that epoch found", record.toString()));
        } else {
            brokerRegistrations.put(brokerId, registration.cloneWithFencing(true));
            log.info("Fenced broker: {}", record);
        }
    }

    public void replay(UnfenceBrokerRecord record) {
        int brokerId = record.id();
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration found for that id", record.toString()));
        } else if (registration.epoch() !=  record.epoch()) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration with that epoch found", record.toString()));
        } else {
            brokerRegistrations.put(brokerId, registration.cloneWithFencing(false));
            log.info("Unfenced broker: {}", record);
        }
        if (readyBrokersFuture.isPresent()) {
            if (readyBrokersFuture.get().check()) {
                readyBrokersFuture.get().future.complete(null);
                readyBrokersFuture = Optional.empty();
            }
        }
    }

    public List<List<Integer>> placeReplicas(int numPartitions, short numReplicas) {
        if (heartbeatManager == null) {
            throw new RuntimeException("ClusterControlManager is not active.");
        }
        return heartbeatManager.placeReplicas(numPartitions, numReplicas,
            id -> brokerRegistrations.get(id).rack(), placementPolicy);
    }

    public boolean unfenced(int brokerId) {
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) return false;
        return !registration.fenced();
    }

    BrokerHeartbeatManager heartbeatManager() {
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

    class ClusterControlIterator implements Iterator<List<ApiMessageAndVersion>> {
        private final Iterator<Entry<Integer, BrokerRegistration>> iterator;

        ClusterControlIterator(long epoch) {
            this.iterator = brokerRegistrations.entrySet(epoch).iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public List<ApiMessageAndVersion> next() {
            if (!hasNext()) throw new NoSuchElementException();
            Entry<Integer, BrokerRegistration> entry = iterator.next();
            int brokerId = entry.getKey();
            BrokerRegistration registration = entry.getValue();
            BrokerEndpointCollection endpoints = new BrokerEndpointCollection();
            for (Entry<String, Endpoint> endpointEntry : registration.listeners().entrySet()) {
                endpoints.add(new BrokerEndpoint().setName(endpointEntry.getKey()).
                    setHost(endpointEntry.getValue().host()).
                    setPort(endpointEntry.getValue().port()).
                    setSecurityProtocol(endpointEntry.getValue().securityProtocol().id));
            }
            BrokerFeatureCollection features = new BrokerFeatureCollection();
            for (Entry<String, VersionRange> featureEntry : registration.supportedFeatures().entrySet()) {
                features.add(new BrokerFeature().setName(featureEntry.getKey()).
                    setMaxSupportedVersion(featureEntry.getValue().max()).
                    setMinSupportedVersion(featureEntry.getValue().min()));
            }
            List<ApiMessageAndVersion> batch = new ArrayList<>();
            batch.add(new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerId(brokerId).
                setIncarnationId(registration.incarnationId()).
                setBrokerEpoch(registration.epoch()).
                setEndPoints(endpoints).
                setFeatures(features).
                setRack(registration.rack().orElse(null)), (short) 0));
            if (!registration.fenced()) {
                batch.add(new ApiMessageAndVersion(new UnfenceBrokerRecord().
                    setId(brokerId).
                    setEpoch(registration.epoch()), (short) 0));
            }
            return batch;
        }
    }

    ClusterControlIterator iterator(long epoch) {
        return new ClusterControlIterator(epoch);
    }
}
