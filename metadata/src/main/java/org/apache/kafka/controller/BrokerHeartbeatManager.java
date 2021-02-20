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

import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.UsableBroker;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.controller.BrokerControlState.FENCED;
import static org.apache.kafka.controller.BrokerControlState.CONTROLLED_SHUTDOWN;
import static org.apache.kafka.controller.BrokerControlState.SHUTDOWN_NOW;
import static org.apache.kafka.controller.BrokerControlState.UNFENCED;


/**
 * The BrokerHeartbeatManager manages all the soft state associated with broker heartbeats.
 * Soft state is state which does not appear in the metadata log.  This state includes
 * things like the last time each broker sent us a heartbeat, and whether the broker is
 * trying to perform a controlled shutdown.
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
         * The last time we received a heartbeat from this broker, in monotonic nanoseconds.
         * When this field is updated, we also may have to update the broker's position in
         * the unfenced list.
         */
        long lastContactNs;

        /**
         * The last metadata offset which this broker reported.  When this field is updated,
         * we may also have to update the broker's position in the active set.
         */
        long metadataOffset;

        /**
         * The offset at which the broker should complete its controlled shutdown, or -1
         * if the broker is not performing a controlled shutdown.  When this field is
         * updated, we also have to update the broker's position in the shuttingDown set.
         */
        private long controlledShutDownOffset;

        /**
         * The previous entry in the unfenced list, or null if the broker is not in that list.
         */
        private BrokerHeartbeatState prev;

        /**
         * The next entry in the unfenced list, or null if the broker is not in that list.
         */
        private BrokerHeartbeatState next;

        BrokerHeartbeatState(int id) {
            this.id = id;
            this.lastContactNs = 0;
            this.prev = null;
            this.next = null;
            this.metadataOffset = -1;
            this.controlledShutDownOffset = -1;
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
            return prev == null;
        }

        /**
         * Returns true only if the broker is in controlled shutdown state.
         */
        boolean shuttingDown() {
            return controlledShutDownOffset >= 0;
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

    static class BrokerHeartbeatStateList {
        /**
         * The head of the list of unfenced brokers.  The list is sorted in ascending order
         * of last contact time.
         */
        private final BrokerHeartbeatState head;

        BrokerHeartbeatStateList() {
            this.head = new BrokerHeartbeatState(-1);
            head.prev = head;
            head.next = head;
        }

        /**
         * Return the head of the list, or null if the list is empty.
         */
        BrokerHeartbeatState first() {
            BrokerHeartbeatState result = head.next;
            return result == head ? null : result;
        }

        /**
         * Add the broker to the list. We start looking for a place to put it at the end
         * of the list.
         */
        void add(BrokerHeartbeatState broker) {
            BrokerHeartbeatState cur = head.prev;
            while (true) {
                if (cur == head || cur.lastContactNs <= broker.lastContactNs) {
                    broker.next = cur.next;
                    cur.next.prev = broker;
                    broker.prev = cur;
                    cur.next = broker;
                    break;
                }
                cur = cur.prev;
            }
        }

        /**
         * Remove a broker from the list.
         */
        void remove(BrokerHeartbeatState broker) {
            if (broker.next == null) {
                throw new RuntimeException(broker + " is not in the  list.");
            }
            broker.prev.next = broker.next;
            broker.next.prev = broker.prev;
            broker.prev = null;
            broker.next = null;
        }

        BrokerHeartbeatStateIterator iterator() {
            return new BrokerHeartbeatStateIterator(head);
        }
    }

    static class BrokerHeartbeatStateIterator implements Iterator<BrokerHeartbeatState> {
        private final BrokerHeartbeatState head;
        private BrokerHeartbeatState cur;

        BrokerHeartbeatStateIterator(BrokerHeartbeatState head) {
            this.head = head;
            this.cur = head;
        }

        @Override
        public boolean hasNext() {
            return cur.next != head;
        }

        @Override
        public BrokerHeartbeatState next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            BrokerHeartbeatState result = cur.next;
            cur = cur.next;
            return result;
        }
    }

    private final Logger log;

    /**
     * The Kafka clock object to use.
     */
    private final Time time;

    /**
     * The broker session timeout in nanoseconds.
     */
    private final long sessionTimeoutNs;

    /**
     * Maps broker IDs to heartbeat states.
     */
    private final HashMap<Integer, BrokerHeartbeatState> brokers;

    /**
     * The list of unfenced brokers, sorted by last contact time.
     */
    private final BrokerHeartbeatStateList unfenced;

    /**
     * The set of active brokers.  A broker is active if it is unfenced, and not shutting
     * down.
     */
    private final TreeSet<BrokerHeartbeatState> active;

    BrokerHeartbeatManager(LogContext logContext,
                           Time time,
                           long sessionTimeoutNs) {
        this.log = logContext.logger(BrokerHeartbeatManager.class);
        this.time = time;
        this.sessionTimeoutNs = sessionTimeoutNs;
        this.brokers = new HashMap<>();
        this.unfenced = new BrokerHeartbeatStateList();
        this.active = new TreeSet<>(MetadataOffsetComparator.INSTANCE);
    }

    // VisibleForTesting
    Time time() {
        return time;
    }

    // VisibleForTesting
    BrokerHeartbeatStateList unfenced() {
        return unfenced;
    }

    /**
     * Mark a broker as fenced.
     *
     * @param brokerId      The ID of the broker to mark as fenced.
     */
    void fence(int brokerId) {
        BrokerHeartbeatState broker = brokers.get(brokerId);
        if (broker != null) {
            untrack(broker);
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
            untrack(broker);
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
            unfenced.remove(broker);
            if (!broker.shuttingDown()) {
                active.remove(broker);
            }
        }
    }

    /**
     * Check if the given broker has a valid session.
     *
     * @param brokerId      The broker ID to check.
     *
     * @return              True if the given broker has a valid session.
     */
    boolean hasValidSession(int brokerId) {
        BrokerHeartbeatState broker = brokers.get(brokerId);
        if (broker == null) return false;
        return hasValidSession(broker);
    }

    /**
     * Check if the given broker has a valid session.
     *
     * @param broker        The broker to check.
     *
     * @return              True if the given broker has a valid session.
     */
    private boolean hasValidSession(BrokerHeartbeatState broker) {
        if (broker.fenced()) {
            return false;
        } else {
            return broker.lastContactNs + sessionTimeoutNs >= time.nanoseconds();
        }
    }

    /**
     * Update broker state, including lastContactNs.
     *
     * @param brokerId          The broker ID.
     * @param fenced            True only if the broker is currently fenced.
     * @param metadataOffset    The latest metadata offset of the broker.
     */
    void touch(int brokerId, boolean fenced, long metadataOffset) {
        BrokerHeartbeatState broker = brokers.get(brokerId);
        if (broker == null) {
            broker = new BrokerHeartbeatState(brokerId);
            brokers.put(brokerId, broker);
        } else {
            // Remove the broker from the unfenced list and/or the active set. Its
            // position in either of those data structures depends on values we are
            // changing here. We will re-add it if necessary at the end of this function.
            untrack(broker);
        }
        broker.lastContactNs = time.nanoseconds();
        broker.metadataOffset = metadataOffset;
        if (fenced) {
            // If a broker is fenced, it leaves controlled shutdown.  On its next heartbeat,
            // it will shut down immediately.
            broker.controlledShutDownOffset = -1;
        } else {
            unfenced.add(broker);
            if (!broker.shuttingDown()) {
                active.add(broker);
            }
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
     * Mark a broker as being in the controlled shutdown state.
     *
     * @param brokerId                  The broker id.
     * @param controlledShutDownOffset  The offset at which controlled shutdown will be complete.
     */
    void updateControlledShutdownOffset(int brokerId, long controlledShutDownOffset) {
        BrokerHeartbeatState broker = brokers.get(brokerId);
        if (broker == null) {
            throw new RuntimeException("Unable to locate broker " + brokerId);
        }
        if (broker.fenced()) {
            throw new RuntimeException("Fenced brokers cannot enter controlled shutdown.");
        }
        active.remove(broker);
        broker.controlledShutDownOffset = controlledShutDownOffset;
        log.debug("Updated the controlled shutdown offset for broker {} to {}.",
            brokerId, controlledShutDownOffset);
    }

    /**
     * Return the time in monotonic nanoseconds at which we should check if a broker
     * session needs to be expired.
     */
    long nextCheckTimeNs() {
        BrokerHeartbeatState broker = unfenced.first();
        if (broker == null) {
            return Long.MAX_VALUE;
        } else {
            return broker.lastContactNs + sessionTimeoutNs;
        }
    }

    /**
     * Find the stale brokers which haven't heartbeated in a long time, and which need to
     * be fenced.
     *
     * @return      A list of node IDs.
     */
    List<Integer> findStaleBrokers() {
        List<Integer> nodes = new ArrayList<>();
        BrokerHeartbeatStateIterator iterator = unfenced.iterator();
        while (iterator.hasNext()) {
            BrokerHeartbeatState broker = iterator.next();
            if (hasValidSession(broker)) {
                break;
            }
            nodes.add(broker.id);
        }
        return nodes;
    }

    /**
     * Place replicas on unfenced brokers.
     *
     * @param numPartitions     The number of partitions to place.
     * @param numReplicas       The number of replicas for each partition.
     * @param idToRack          A function mapping broker id to broker rack.
     * @param policy            The replica placement policy to use.
     *
     * @return                  A list of replica lists.
     *
     * @throws InvalidReplicationFactorException    If too many replicas were requested.
     */
    List<List<Integer>> placeReplicas(int numPartitions, short numReplicas,
                                      Function<Integer, Optional<String>> idToRack,
                                      ReplicaPlacementPolicy policy) {
        // TODO: support using fenced brokers here if necessary to get to the desired
        // number of replicas. We probably need to add a fenced boolean in UsableBroker.
        Iterator<UsableBroker> iterator = new UsableBrokerIterator(
            unfenced.iterator(), idToRack);
        return policy.createPlacement(numPartitions, numReplicas, iterator);
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
            next = new UsableBroker(result.id(), rack);
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
     * @param brokerId              The broker id.
     * @param request               The incoming heartbeat request.
     * @param lastCommittedOffset   The last committed offset of the quorum controller.
     * @param hasLeaderships        A callback which evaluates to true if the broker leads
     *                              at least one partition.
     *
     * @return                      The current and next broker states.
     */
    BrokerControlStates calculateNextBrokerState(int brokerId,
                                                 BrokerHeartbeatRequestData request,
                                                 long lastCommittedOffset,
                                                 Supplier<Boolean> hasLeaderships) {
        BrokerHeartbeatState broker = brokers.getOrDefault(brokerId,
            new BrokerHeartbeatState(brokerId));
        BrokerControlState currentState = currentBrokerState(broker);
        switch (currentState) {
            case FENCED:
                if (request.wantShutDown()) {
                    log.info("Fenced broker {} has requested and been granted an immediate " +
                        "shutdown.", brokerId);
                    return new BrokerControlStates(currentState, SHUTDOWN_NOW);
                } else if (!request.wantFence()) {
                    if (request.currentMetadataOffset() >= lastCommittedOffset) {
                        log.info("The request from broker {} to unfence has been granted " +
                                "because it has caught up with the last committed metadata " +
                                "offset {}.", brokerId, lastCommittedOffset);
                        return new BrokerControlStates(currentState, UNFENCED);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("The request from broker {} to unfence cannot yet " +
                                "be granted because it has not caught up with the last " +
                                "committed metadata offset {}. It is still at offset {}.",
                                brokerId, lastCommittedOffset, request.currentMetadataOffset());
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
                if (broker.controlledShutDownOffset <= lowestActiveOffset) {
                    log.info("The request from broker {} to shut down has been granted " +
                        "since the lowest active offset {} is now greater than the " +
                        "broker's controlled shutdown offset {}.", brokerId,
                        lowestActiveOffset, broker.controlledShutDownOffset);
                    return new BrokerControlStates(currentState, SHUTDOWN_NOW);
                }
                log.debug("The request from broker {} to shut down can not yet be granted " +
                    "because the lowest active offset {} is not greater than the broker's " +
                    "shutdown offset {}.", brokerId, lowestActiveOffset,
                    broker.controlledShutDownOffset);
                return new BrokerControlStates(currentState, CONTROLLED_SHUTDOWN);

            default:
                return new BrokerControlStates(currentState, SHUTDOWN_NOW);
        }
    }
}
