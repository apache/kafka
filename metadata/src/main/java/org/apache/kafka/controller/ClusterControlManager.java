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
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.FeatureManager;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ClusterControlManager {
    static class BrokerSoftState {
        private long lastContactNs;

        BrokerSoftState(long lastContactNs) {
            this.lastContactNs = lastContactNs;
        }
    }

    public static class RegistrationReply {
        private final long epoch;

        RegistrationReply(long epoch) {
            this.epoch = epoch;
        }

        public long epoch() {
            return epoch;
        }

        @Override
        public int hashCode() {
            return Objects.hash(epoch);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof RegistrationReply)) return false;
            RegistrationReply other = (RegistrationReply) o;
            return other.epoch == epoch;
        }

        @Override
        public String toString() {
            return "RegistrationReply(epoch=" + epoch + ")";
        }
    }

    public static class HeartbeatReply {
        private final boolean isCaughtUp;
        private final boolean isFenced;

        HeartbeatReply(boolean isCaughtUp, boolean isFenced) {
            this.isCaughtUp = isCaughtUp;
            this.isFenced = isFenced;
        }

        public boolean isCaughtUp() {
            return isCaughtUp;
        }

        public boolean isFenced() {
            return isFenced;
        }

        @Override
        public int hashCode() {
            return Objects.hash(isCaughtUp, isFenced);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof HeartbeatReply)) return false;
            HeartbeatReply other = (HeartbeatReply) o;
            return other.isCaughtUp == isCaughtUp &&
                other.isFenced == isFenced;
        }

        @Override
        public String toString() {
            return "HeartbeatReply(isCaughtUp=" + isCaughtUp +
                ", isFenced=" + isFenced + ")";
        }
    }

    private final Logger log;

    /**
     * The Kafka clock object to use.
     */
    private final Time time;

    /**
     * How long leases should last, in milliseconds.
     */
    private final long leaseDurationNs;

    /**
     * How long leases should be exclusive, in milliseconds.
     */
    private final long exclusiveLeaseDurationNs;

    /**
     * Maps broker IDs to broker registrations.
     */
    private final TimelineHashMap<Integer, BrokerRegistration> brokerRegistrations;

    /**
     * A set containing the usable brokers (that are registered and unfenced).
     */
    private final TimelineHashSet<Integer> usable;

    /**
     * Maps broker IDs to soft state.  The soft state is not persisted in the metadata log.
     */
    private final HashMap<Integer, BrokerSoftState> brokerSoftStates;

    ClusterControlManager(LogContext logContext,
                          Time time,
                          SnapshotRegistry snapshotRegistry,
                          long leaseDurationMs,
                          long exclusiveLeaseDurationMs) {
        this.log = logContext.logger(ClusterControlManager.class);
        this.time = time;
        this.leaseDurationNs = NANOSECONDS.convert(leaseDurationMs, MILLISECONDS);
        this.exclusiveLeaseDurationNs = NANOSECONDS.convert(
            Math.max(leaseDurationMs, exclusiveLeaseDurationMs), MILLISECONDS);
        this.brokerRegistrations = new TimelineHashMap<>(snapshotRegistry, 0);
        this.usable = new TimelineHashSet<>(snapshotRegistry, 0);
        this.brokerSoftStates = new HashMap<>();
    }

    /**
     * Process an incoming broker registration request.
     */
    public ControllerResult<RegistrationReply> registerBroker(
            BrokerRegistrationRequestData request, long brokerEpoch,
            FeatureManager.FinalizedFeaturesAndEpoch finalizedFeaturesAndEpoch) {
        int brokerId = request.brokerId();
        BrokerRegistration existing = brokerRegistrations.get(brokerId);
        if (existing != null && !existing.incarnationId().equals(request.incarnationId())) {
            throw new DuplicateBrokerRegistrationException("Another broker has " +
                "registered with that broker id.");
        }
        RegisterBrokerRecord record = new RegisterBrokerRecord().setBrokerId(brokerId).
            setIncarnationId(request.incarnationId()).
            setBrokerEpoch(brokerEpoch);
        for (BrokerRegistrationRequestData.Listener listener : request.listeners()) {
            record.endPoints().add(new RegisterBrokerRecord.BrokerEndpoint().
                setHost(listener.host()).
                setName(listener.name()).
                setPort(listener.port()).
                setSecurityProtocol(listener.securityProtocol()));
        }
        for (BrokerRegistrationRequestData.Feature feature : request.features()) {
            VersionRange supported = finalizedFeaturesAndEpoch.finalizedFeatures().
                getOrDefault(feature.name(), VersionRange.ALL);
            if (!supported.contains(new VersionRange(feature.minSupportedVersion(),
                    feature.maxSupportedVersion()))) {
                throw new UnsupportedVersionException("Unable to register because " +
                    "the broker has an unsupported version of " + feature.name());
            }
            record.features().add(new RegisterBrokerRecord.BrokerFeature().
                setName(feature.name()).
                setMinSupportedVersion(feature.minSupportedVersion()).
                setMaxSupportedVersion(feature.maxSupportedVersion()));
        }
        return new ControllerResult<>(
            Collections.singletonList(new ApiMessageAndVersion(record, (short) 0)),
                new RegistrationReply(brokerEpoch));
    }

    public ControllerResult<HeartbeatReply> processBrokerHeartbeat(BrokerHeartbeatRequestData request,
                                                                   long lastCommittedOffset) {
        int brokerId = request.brokerId();
        verifyHeartbeatAgainstRegistration(request, brokerRegistrations.get(brokerId));
        touchBroker(brokerId);
        boolean isCaughtUp = request.currentMetadataOffset() >= lastCommittedOffset;
        boolean isFenced = !usable.contains(brokerId);
        List<ApiMessageAndVersion> records = new ArrayList<>();
        if (isFenced) {
            if (isCaughtUp && !request.shouldFence()) {
                records.add(new ApiMessageAndVersion(new UnfenceBrokerRecord().
                    setId(brokerId).setEpoch(request.brokerEpoch()), (short) 0));
            }
        } else {
            if (request.shouldFence()) {
                records.add(new ApiMessageAndVersion(new FenceBrokerRecord().
                    setId(brokerId).setEpoch(request.brokerEpoch()), (short) 0));
            }
        }
        return new ControllerResult<>(records, new HeartbeatReply(isCaughtUp, isFenced));
    }

    void touchBroker(int brokerId) {
        long nowNs = time.nanoseconds();
        BrokerSoftState state = brokerSoftStates.get(brokerId);
        if (state == null) {
            state = new BrokerSoftState(nowNs);
            brokerSoftStates.put(brokerId, state);
        } else {
            state.lastContactNs = nowNs;
        }
        log.trace("Set lastContactNs for {} to {}", brokerId, state.lastContactNs);
        // TODO: fence brokers when their time is up, using lastContactNs
    }

    static void verifyHeartbeatAgainstRegistration(BrokerHeartbeatRequestData request,
                                                   BrokerRegistration registration) {
        if (registration == null) {
            throw new StaleBrokerEpochException("No registration found for broker " +
                request.brokerId());
        }
        if (request.brokerEpoch() != registration.epoch()) {
            throw new StaleBrokerEpochException("Expected broker epoch " +
                request.brokerEpoch() + "; got epoch " + registration.epoch());
        }
    }

    private long exclusiveLeaseExpirationNs(long leaseStartNs) {
        return leaseStartNs + exclusiveLeaseDurationNs;
    }

    public void replay(RegisterBrokerRecord record) {
        int brokerId = record.brokerId();
        long nowNs = time.nanoseconds();
        brokerSoftStates.remove(brokerId);
        brokerSoftStates.put(brokerId, new BrokerSoftState(nowNs));
        List<Endpoint> listeners = new ArrayList<>();
        for (RegisterBrokerRecord.BrokerEndpoint endpoint : record.endPoints()) {
            listeners.add(new Endpoint(endpoint.name(),
                SecurityProtocol.forId(endpoint.securityProtocol()),
                endpoint.host(), endpoint.port()));
        }
        Map<String, VersionRange> features = new HashMap<>();
        for (RegisterBrokerRecord.BrokerFeature feature : record.features()) {
            features.put(feature.name(), new VersionRange(
                feature.minSupportedVersion(), feature.maxSupportedVersion()));
        }
        brokerRegistrations.put(brokerId, new BrokerRegistration(brokerId,
            record.brokerEpoch(), record.incarnationId(), listeners, features,
            record.rack()));
        touchBroker(brokerId);
        log.trace("Replayed {}", record);
    }

    public void replay(UnregisterBrokerRecord record) {
        int brokerId = record.brokerId();
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) {
            log.error("Unable to replay {}: no broker registration found for that id", record);
        } else if (registration.epoch() !=  record.brokerEpoch()) {
            log.error("Unable to replay {}: no broker registration with that epoch found", record);
        } else {
            brokerRegistrations.remove(brokerId);
            brokerSoftStates.remove(brokerId);
            usable.remove(brokerId);
            log.trace("Replayed {}", record);
        }
    }

    public void replay(FenceBrokerRecord record) {
        int brokerId = record.id();
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) {
            log.error("Unable to replay {}: no broker registration found for that id", record);
        } else if (registration.epoch() !=  record.epoch()) {
            log.error("Unable to replay {}: no broker registration with that epoch found", record);
        } else {
            usable.remove(record.id());
            log.trace("Replayed {}", record);
        }
    }

    public void replay(UnfenceBrokerRecord record) {
        int brokerId = record.id();
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) {
            log.error("Unable to replay {}: no broker registration found for that id", record);
        } else if (registration.epoch() !=  record.epoch()) {
            log.error("Unable to replay {}: no broker registration with that epoch found", record);
        } else {
            usable.add(record.id());
            log.trace("Replayed {}", record);
        }
    }

    /**
     * Returns true if the given broker id is registered and unfenced.
     */
    public boolean isUsable(int brokerId) {
        return usable.contains(brokerId);
    }

    public List<Integer> chooseRandomUsable(Random random, int numBrokers) {
        if (usable.size() < numBrokers) {
            throw new RuntimeException("there are only " + usable.size() +
                " usable brokers");
        }
        List<Integer> choices = new ArrayList<>();
        // TODO: rack-awareness
        List<Integer> indexes = new ArrayList<>();
        int initialIndex = random.nextInt(usable.size());
        for (int i = 0; i < numBrokers; i++) {
            indexes.add((initialIndex + i) % usable.size());
        }
        indexes.sort(Integer::compareTo);
        Iterator<Integer> iter = usable.iterator();
        for (int i = 0; choices.size() < indexes.size(); i++) {
            int brokerId = iter.next();
            if (indexes.get(choices.size()) == i) {
                choices.add(brokerId);
            }
        }
        Collections.shuffle(choices);
        return choices;
    }
}
