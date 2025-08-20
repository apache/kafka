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

import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.placement.UsableBroker;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.controller.BrokerControlState.CONTROLLED_SHUTDOWN;
import static org.apache.kafka.controller.BrokerControlState.FENCED;
import static org.apache.kafka.controller.BrokerControlState.SHUTDOWN_NOW;
import static org.apache.kafka.controller.BrokerControlState.UNFENCED;


/**
 * The BrokerHeartbeatManager manages some of the soft state associated with broker heartbeats.
 * For example, it stores the last metadata offset which each broker reported. It contains the
 * BrokerHeartbeatTracker, which stores the last time we received a heartbeat from each broker.
 * In addition to storing this soft state, the BrokerHeartbeatManager aggregates some information
 * about brokers (such as whether they're fenced or not) into a single place.
 *
 * Only the active controller has a BrokerHeartbeatManager, since only the active
 * controller handles broker heartbeats.  Standby controllers will create a heartbeat
 * manager as part of the process of activating.  This design minimizes the size of the
 * metadata partition by excluding heartbeats from it.  However, it does mean that after
 * a controller failover, we may take some extra time to fence brokers, since the new
 * active controller does not know when the last heartbeats were received from each.
 */
public class BrokerHeartbeatManager {
    static class BrokerHeartbeatState {
        /**
         * The broker ID.
         */
        private final int id;

        /**
         * True if this broker is fenced.
         */
        private boolean fenced;

        /**
         * The last metadata offset which this broker reported.  When this field is updated,
         * we may also have to update the broker's position in the active set.
         */
        private long metadataOffset;

        /**
         * The offset at which the broker should complete its controlled shutdown, or -1
         * if the broker is not performing a controlled shutdown.
         */
        private long controlledShutdownOffset;

        BrokerHeartbeatState(
            int id,
            boolean fenced,
            long metadataOffset,
            long controlledShutdownOffset
        ) {
            this.id = id;
            this.fenced = fenced;
            this.metadataOffset = metadataOffset;
            this.controlledShutdownOffset = controlledShutdownOffset;
        }

        /**
         * Returns the broker ID.
         */
        int id() {
            return id;
        }

        /**
         * Returns true only if the broker is fenced.
         */
        boolean fenced() {
            return fenced;
        }

        /**
         * Get the last metadata offset that was reported.
         */
        long metadataOffset() {
            return metadataOffset;
        }

        void setMetadataOffset(long metadataOffset) {
            this.metadataOffset = metadataOffset;
        }

        /**
         * Returns true only if the broker is in controlled shutdown state.
         */
        boolean shuttingDown() {
            return controlledShutdownOffset >= 0;
        }
    }

    static class MetadataOffsetComparator implements Comparator<BrokerHeartbeatState> {
        static final MetadataOffsetComparator INSTANCE = new MetadataOffsetComparator();

        @Override
        public int compare(BrokerHeartbeatState a, BrokerHeartbeatState b) {
            if (a.metadataOffset < b.metadataOffset) {
                return -1;
            } else if (a.metadataOffset > b.metadataOffset) {
                return 1;
            } else if (a.id < b.id) {
                return -1;
            } else if (a.id > b.id) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    private final Logger log;

    /**
     * Tracks the last time broker heartbeats were reported for each broker.
     */
    private final BrokerHeartbeatTracker tracker;

    /**
     * Maps broker IDs to heartbeat states.
     */
    private final HashMap<Integer, BrokerHeartbeatState> brokers;

    /**
     * The set of active brokers.  A broker is active if it is unfenced, and not shutting
     * down.
     */
    private final TreeSet<BrokerHeartbeatState> active;

    BrokerHeartbeatManager(
        LogContext logContext,
        Time time,
        long sessionTimeoutNs
    ) {
        this.log = logContext.logger(BrokerHeartbeatManager.class);
        this.tracker = new BrokerHeartbeatTracker(time, sessionTimeoutNs);
        this.brokers = new HashMap<>();
        this.active = new TreeSet<>(MetadataOffsetComparator.INSTANCE);
    }

    BrokerHeartbeatTracker tracker() {
        return tracker;
    }

    // VisibleForTesting
    Time time() {
        return tracker.time();
    }

    // VisibleForTesting
    Collection<BrokerHeartbeatState> brokers() {
        return brokers.values();
    }

    // VisibleForTesting
    OptionalLong controlledShutdownOffset(int brokerId) {
        BrokerHeartbeatState broker = brokers.get(brokerId);
        if (broker == null || broker.controlledShutdownOffset == -1) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(broker.controlledShutdownOffset);
    }

    /**
     * Mark a broker as fenced.
     *
     * @param brokerId      The ID of the broker to mark as fenced.
     */
    void fence(int brokerId) {
        BrokerHeartbeatState broker = brokers.get(brokerId);
        if (broker != null) {
            broker.fenced = true;
            active.remove(broker);
        }
    }

    /**
     * Remove a broker.
     *
     * @param brokerId      The ID of the broker to remove.
     */
    void remove(int brokerId) {
        BrokerHeartbeatState broker = brokers.remove(brokerId);
        if (broker != null) {
            active.remove(broker);
        }
    }

    /**
     * Stop tracking the broker in the unfenced list and active set, if it was tracked
     * in either of these.
     *
     * @param broker        The broker state to stop tracking.
     */
    private void untrack(BrokerHeartbeatState broker) {
        if (!broker.fenced()) {
            if (!broker.shuttingDown()) {
                active.remove(broker);
            }
        }
    }

    /**
     * Check if the given broker has a valid session.
     *
     * @param brokerId      The broker ID to check.
     * @param brokerEpoch   The broker epoch to check.
     *
     * @return              True if the given broker has a valid session.
     */
    boolean hasValidSession(int brokerId, long brokerEpoch) {
        return tracker.hasValidSession(new BrokerIdAndEpoch(brokerId, brokerEpoch));
    }

    /**
     * Register this broker if we haven't already, and make sure its fencing state is
     * correct.
     *
     * @param brokerId          The broker ID.
     * @param fenced            True only if the broker is currently fenced.
     */
    void register(int brokerId, boolean fenced) {
        BrokerHeartbeatState broker = brokers.get(brokerId);
        long metadataOffset = -1L;
        if (broker == null) {
            broker = new BrokerHeartbeatState(brokerId, fenced, -1L, -1L);
            brokers.put(brokerId, broker);
        } else if (broker.fenced() != fenced) {
            metadataOffset = broker.metadataOffset;
        }
        touch(brokerId, fenced, metadataOffset);
    }

    /**
     * Update broker state, including lastContactNs.
     *
     * @param brokerId          The broker ID.
     * @param fenced            True only if the broker is currently fenced.
     * @param metadataOffset    The latest metadata offset of the broker.
     * @throws IllegalStateException if the broker is not registered.
     */
    void touch(int brokerId, boolean fenced, long metadataOffset) {
        BrokerHeartbeatState broker = heartbeatStateOrThrow(brokerId);
        // Remove the broker from the unfenced list and/or the active set. Its
        // position in either of those data structures depends on values we are
        // changing here. We will re-add it if necessary at the end of this function.
        untrack(broker);
        broker.fenced = fenced;
        broker.metadataOffset = metadataOffset;
        boolean isActive = false;
        if (fenced) {
            // If a broker is fenced, it leaves controlled shutdown.  On its next heartbeat,
            // it will shut down immediately.
            broker.controlledShutdownOffset = -1;
        } else {
            if (!broker.shuttingDown()) {
                isActive = true;
            }
        }
        if (isActive) {
            active.add(broker);
        } else {
            active.remove(broker);
        }
    }

    long lowestActiveOffset() {
        Iterator<BrokerHeartbeatState> iterator = active.iterator();
        if (!iterator.hasNext()) {
            return Long.MAX_VALUE;
        }
        BrokerHeartbeatState first = iterator.next();
        return first.metadataOffset;
    }

    /**
     * Mark a broker as being in the controlled shutdown state. We only update the
     * controlledShutdownOffset if the broker was previously not in controlled shutdown state.
     *
     * @param brokerId                  The broker id.
     * @param controlledShutDownOffset  The offset at which controlled shutdown will be complete.
     */
    void maybeUpdateControlledShutdownOffset(int brokerId, long controlledShutDownOffset) {
        BrokerHeartbeatState broker = heartbeatStateOrThrow(brokerId);
        if (broker.fenced()) {
            throw new RuntimeException("Fenced brokers cannot enter controlled shutdown.");
        }
        active.remove(broker);
        if (broker.controlledShutdownOffset < 0) {
            broker.controlledShutdownOffset = controlledShutDownOffset;
            log.debug("Updated the controlled shutdown offset for broker {} to {}.",
                brokerId, controlledShutDownOffset);
        }
    }

    Iterator<UsableBroker> usableBrokers(
        Function<Integer, Optional<String>> idToRack
    ) {
        return new UsableBrokerIterator(brokers.values().iterator(),
            idToRack);
    }

    static class UsableBrokerIterator implements Iterator<UsableBroker> {
        private final Iterator<BrokerHeartbeatState> iterator;
        private final Function<Integer, Optional<String>> idToRack;
        private UsableBroker next;

        UsableBrokerIterator(Iterator<BrokerHeartbeatState> iterator,
                             Function<Integer, Optional<String>> idToRack) {
            this.iterator = iterator;
            this.idToRack = idToRack;
            this.next = null;
        }

        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            }
            BrokerHeartbeatState result;
            do {
                if (!iterator.hasNext()) {
                    return false;
                }
                result = iterator.next();
            } while (result.shuttingDown());
            Optional<String> rack = idToRack.apply(result.id());
            next = new UsableBroker(result.id(), rack, result.fenced());
            return true;
        }

        @Override
        public UsableBroker next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            UsableBroker result = next;
            next = null;
            return result;
        }
    }

    BrokerControlState currentBrokerState(BrokerHeartbeatState broker) {
        if (broker.shuttingDown()) {
            return CONTROLLED_SHUTDOWN;
        } else if (broker.fenced()) {
            return FENCED;
        } else {
            return UNFENCED;
        }
    }

    /**
     * Calculate the next broker state for a broker that just sent a heartbeat request.
     *
     * @param brokerId                     The broker id.
     * @param request                      The incoming heartbeat request.
     * @param registerBrokerRecordOffset   The offset of the broker's {@link org.apache.kafka.common.metadata.RegisterBrokerRecord}.
     * @param hasLeaderships               A callback which evaluates to true if the broker leads
     *                                     at least one partition.
     *
     * @throws IllegalStateException       If the broker is not registered.
     * @return                             The current and next broker states.
     */
    BrokerControlStates calculateNextBrokerState(int brokerId,
                                                 BrokerHeartbeatRequestData request,
                                                 long registerBrokerRecordOffset,
                                                 Supplier<Boolean> hasLeaderships) {
        BrokerHeartbeatState broker = heartbeatStateOrThrow(brokerId);
        BrokerControlState currentState = currentBrokerState(broker);
        switch (currentState) {
            case FENCED:
                if (request.wantShutDown()) {
                    log.info("Fenced broker {} has requested and been granted an immediate " +
                        "shutdown.", brokerId);
                    return new BrokerControlStates(currentState, SHUTDOWN_NOW);
                } else if (!request.wantFence()) {
                    if (request.currentMetadataOffset() >= registerBrokerRecordOffset) {
                        log.info("The request from broker {} to unfence has been granted " +
                                "because it has caught up with the offset of its register " +
                                "broker record {}.", brokerId, registerBrokerRecordOffset);
                        return new BrokerControlStates(currentState, UNFENCED);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("The request from broker {} to unfence cannot yet " +
                                "be granted because it has not caught up with the offset of " +
                                "its register broker record {}. It is still at offset {}.",
                                brokerId, registerBrokerRecordOffset, request.currentMetadataOffset());
                        }
                        return new BrokerControlStates(currentState, FENCED);
                    }
                }
                return new BrokerControlStates(currentState, FENCED);

            case UNFENCED:
                if (request.wantFence()) {
                    if (request.wantShutDown()) {
                        log.info("Unfenced broker {} has requested and been granted an " +
                            "immediate shutdown.", brokerId);
                        return new BrokerControlStates(currentState, SHUTDOWN_NOW);
                    } else {
                        log.info("Unfenced broker {} has requested and been granted " +
                            "fencing", brokerId);
                        return new BrokerControlStates(currentState, FENCED);
                    }
                } else if (request.wantShutDown()) {
                    if (hasLeaderships.get()) {
                        log.info("Unfenced broker {} has requested and been granted a " +
                            "controlled shutdown.", brokerId);
                        return new BrokerControlStates(currentState, CONTROLLED_SHUTDOWN);
                    } else {
                        log.info("Unfenced broker {} has requested and been granted an " +
                            "immediate shutdown.", brokerId);
                        return new BrokerControlStates(currentState, SHUTDOWN_NOW);
                    }
                }
                return new BrokerControlStates(currentState, UNFENCED);

            case CONTROLLED_SHUTDOWN:
                if (hasLeaderships.get()) {
                    log.debug("Broker {} is in controlled shutdown state, but can not " +
                        "shut down because more leaders still need to be moved.", brokerId);
                    return new BrokerControlStates(currentState, CONTROLLED_SHUTDOWN);
                }
                long lowestActiveOffset = lowestActiveOffset();
                if (broker.controlledShutdownOffset <= lowestActiveOffset) {
                    log.info("The request from broker {} to shut down has been granted " +
                        "since the lowest active offset {} is now greater than the " +
                        "broker's controlled shutdown offset {}.", brokerId,
                        lowestActiveOffset, broker.controlledShutdownOffset);
                    return new BrokerControlStates(currentState, SHUTDOWN_NOW);
                }
                log.debug("The request from broker {} to shut down can not yet be granted " +
                    "because the lowest active offset {} is not greater than the broker's " +
                    "shutdown offset {}.", brokerId, lowestActiveOffset,
                    broker.controlledShutdownOffset);
                return new BrokerControlStates(currentState, CONTROLLED_SHUTDOWN);

            default:
                return new BrokerControlStates(currentState, SHUTDOWN_NOW);
        }
    }

    private BrokerHeartbeatState heartbeatStateOrThrow(int brokerId) {
        BrokerHeartbeatState broker = brokers.get(brokerId);
        if (broker == null) {
            throw new IllegalStateException("Broker " + brokerId + " is not registered.");
        }
        return broker;
    }
}
