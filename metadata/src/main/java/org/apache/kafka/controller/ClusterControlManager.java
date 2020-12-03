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
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

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
        private long leaseStartNs;

        BrokerSoftState(long leaseStartNs) {
            this.leaseStartNs = leaseStartNs;
        }
    }

    public static class RegistrationReply {
        private final long epoch;
        private final long leaseDurationMs;

        RegistrationReply(long epoch, long leaseDurationMs) {
            this.epoch = epoch;
            this.leaseDurationMs = leaseDurationMs;
        }

        public long epoch() {
            return epoch;
        }

        public long leaseDurationMs() {
            return leaseDurationMs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(epoch, leaseDurationMs);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof RegistrationReply)) return false;
            RegistrationReply other = (RegistrationReply) o;
            return other.epoch == epoch && other.leaseDurationMs == leaseDurationMs;
        }

        @Override
        public String toString() {
            return "RegistrationReply(epoch=" + epoch +
                ", leaseDurationMs=" + leaseDurationMs + ")";
        }
    }

    public static class HeartbeatReply {
        private final BrokerState nextState;
        private final long leaseDurationMs;

        HeartbeatReply(BrokerState nextState, long leaseDurationMs) {
            this.nextState = nextState;
            this.leaseDurationMs = leaseDurationMs;
        }

        public BrokerState nextState() {
            return nextState;
        }

        public long leaseDurationMs() {
            return leaseDurationMs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(nextState, leaseDurationMs);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof HeartbeatReply)) return false;
            HeartbeatReply other = (HeartbeatReply) o;
            return other.nextState == nextState && other.leaseDurationMs == leaseDurationMs;
        }

        @Override
        public String toString() {
            return "HeartbeatReply(nextState=" + nextState +
                ", leaseDurationMs=" + leaseDurationMs + ")";
        }
    }

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
     * Maps broker IDs to soft state.  The soft state is not persisted in the metadata log.
     */
    private final HashMap<Integer, BrokerSoftState> brokerSoftStates;

    ClusterControlManager(Time time,
                          SnapshotRegistry snapshotRegistry,
                          long leaseDurationMs,
                          long exclusiveLeaseDurationMs) {
        this.time = time;
        this.leaseDurationNs = NANOSECONDS.convert(leaseDurationMs, MILLISECONDS);
        this.exclusiveLeaseDurationNs = NANOSECONDS.convert(
            Math.max(leaseDurationMs, exclusiveLeaseDurationMs), MILLISECONDS);
        this.brokerRegistrations = new TimelineHashMap<>(snapshotRegistry, 0);
        this.brokerSoftStates = new HashMap<>();
    }

    /**
     * Process an incoming broker heartbeat.
     * Unlike most read operations, when processing the broker heartbeat, we look
     * at both committed and uncommitted state.
     *
     * We check that the committed state contains the registration because the broker
     * should not send out any heartbeats until its registration has succeeded and
     * been acknowledged.  We check that the uncommitted state contains the
     * registration because the broker lease can't be renewed if we've already
     * removed the broker in the uncommitted state.
     */
    public HeartbeatReply processBrokerHeartbeat(BrokerHeartbeatRequestData request,
                                                 long lastCommittedOffset) {
        int brokerId = request.brokerId();
        verifyHeartbeatAgainstRegistration(request, brokerRegistrations.get(brokerId));
        verifyHeartbeatAgainstRegistration(request,
            brokerRegistrations.get(brokerId, lastCommittedOffset));
        long nowNs = time.nanoseconds();
        BrokerSoftState state = brokerSoftStates.get(brokerId);
        if (state == null) {
            state = new BrokerSoftState(nowNs);
            brokerSoftStates.put(brokerId, state);
        } else {
            state.leaseStartNs = nowNs;
        }
        // TODO: check targetState, currentMetadataOffset.
        // TODO: Send back the correct broker state here, rather than always sending RUNNING
        return new HeartbeatReply(BrokerState.RUNNING, leaseDurationNs);
    }

    private void verifyHeartbeatAgainstRegistration(BrokerHeartbeatRequestData request,
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

    /**
     * Process an incoming broker registration request.
     */
    public ControllerResult<RegistrationReply> registerBroker(
            BrokerRegistrationRequestData request, long writeOffset) {
        long currentNs = time.nanoseconds();
        int brokerId = request.brokerId();
        BrokerRegistration existing = brokerRegistrations.get(brokerId);
        if (existing != null) {
            BrokerSoftState state = brokerSoftStates.get(brokerId);
            if (state != null && currentNs < exclusiveLeaseExpirationNs(state.leaseStartNs)) {
                throw new DuplicateBrokerRegistrationException("Another broker has " +
                    "registered with that broker id.");
            }
        }
        RegisterBrokerRecord record = new RegisterBrokerRecord().setBrokerId(brokerId).
            setIncarnationId(request.incarnationId()).
            setBrokerEpoch(writeOffset);
        for (BrokerRegistrationRequestData.Listener listener : request.listeners()) {
            record.endPoints().add(new RegisterBrokerRecord.BrokerEndpoint().
                setHost(listener.host()).
                setName(listener.name()).
                setPort(listener.port()).
                setSecurityProtocol(listener.securityProtocol()));
        }
        return new ControllerResult<>(
            Collections.singletonList(new ApiMessageAndVersion(record, (short) 0)),
            new RegistrationReply(writeOffset,
                NANOSECONDS.convert(leaseDurationNs, MILLISECONDS)));
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
            record.brokerEpoch(), listeners, features, record.rack()));
    }

    public void replay(FenceBrokerRecord record) {
        int brokerId = record.id();
        BrokerRegistration registration = brokerRegistrations.remove(brokerId);
        if (registration == null || registration.id() != brokerId) {
            throw new RuntimeException("Can't apply " + record + " since no such " +
                "broker registration was found.");
        }
        if (registration.epoch() != record.epoch()) {
            throw new RuntimeException("Can't apply " + record + " since the current " +
                "broker registration for that ID has epoch " + registration.epoch() +
                " instead of the expected values");
        }
        brokerSoftStates.remove(brokerId);
    }

    /**
     * Returns true if the given broker id is registered.
     */
    public boolean isRegistered(int brokerId) {
        return brokerRegistrations.containsKey(brokerId);
    }

    public List<Integer> chooseRandomRegistered(Random random, int numBrokers) {
        if (brokerRegistrations.size() < numBrokers) {
            throw new RuntimeException("there are only " + brokerRegistrations.size() +
                " registered brokers");
        }
        List<Integer> choices = new ArrayList<>();
        // TODO: rack-awareness
        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < numBrokers; i++) {
            indexes.add(random.nextInt(numBrokers - i));
        }
        indexes.sort(Integer::compareTo);
        Iterator<Integer> iter = brokerRegistrations.keySet().iterator();
        for (int i = 0; choices.size() < indexes.size(); i++) {
            int brokerId = iter.next();
            if (indexes.get(choices.size()) + choices.size() == i) {
                choices.add(brokerId);
            }
        }
        Collections.shuffle(choices);
        return choices;
    }
}
