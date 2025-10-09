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
package org.apache.kafka.coordinator.transaction;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents the states of a transaction in the transaction coordinator.
 * This enum corresponds to the Scala sealed trait TransactionState in kafka.coordinator.transaction.
 */
public enum TransactionState {
    /**
     * Transaction has not existed yet
     * <p>
     * transition: received AddPartitionsToTxnRequest => Ongoing
     *             received AddOffsetsToTxnRequest => Ongoing
     *             received EndTxnRequest with abort and TransactionV2 enabled => PrepareAbort
     */
    EMPTY((byte) 0, org.apache.kafka.clients.admin.TransactionState.EMPTY.toString(), true),
    /**
     * Transaction has started and ongoing
     * <p>
     * transition: received EndTxnRequest with commit => PrepareCommit
     *             received EndTxnRequest with abort => PrepareAbort
     *             received AddPartitionsToTxnRequest => Ongoing
     *             received AddOffsetsToTxnRequest => Ongoing
     */
    ONGOING((byte) 1, org.apache.kafka.clients.admin.TransactionState.ONGOING.toString(), false),
    /**
     * Group is preparing to commit
     * transition: received acks from all partitions => CompleteCommit
     */
    PREPARE_COMMIT((byte) 2, org.apache.kafka.clients.admin.TransactionState.PREPARE_COMMIT.toString(), false),
    /**
     * Group is preparing to abort
     * <p>
     * transition: received acks from all partitions => CompleteAbort
     * <p>
     * Note, In transaction v2, we allow Empty, CompleteCommit, CompleteAbort to transition to PrepareAbort. because the
     * client may not know the txn state on the server side, it needs to send endTxn request when uncertain.
     */
    PREPARE_ABORT((byte) 3, org.apache.kafka.clients.admin.TransactionState.PREPARE_ABORT.toString(), false),
    /**
     * Group has completed commit
     * <p>
     * Will soon be removed from the ongoing transaction cache
     */
    COMPLETE_COMMIT((byte) 4, org.apache.kafka.clients.admin.TransactionState.COMPLETE_COMMIT.toString(), true),
    /**
     * Group has completed abort
     * <p>
     * Will soon be removed from the ongoing transaction cache
     */
    COMPLETE_ABORT((byte) 5, org.apache.kafka.clients.admin.TransactionState.COMPLETE_ABORT.toString(), true),
    /**
     * TransactionalId has expired and is about to be removed from the transaction cache
     */
    DEAD((byte) 6, "Dead", false),
    /**
     * We are in the middle of bumping the epoch and fencing out older producers.
     */
    PREPARE_EPOCH_FENCE((byte) 7, org.apache.kafka.clients.admin.TransactionState.PREPARE_EPOCH_FENCE.toString(), false);

    private static final Map<String, TransactionState> NAME_TO_ENUM = Arrays.stream(values())
        .collect(Collectors.toUnmodifiableMap(TransactionState::stateName, Function.identity()));

    private static final Map<Byte, TransactionState> ID_TO_ENUM = Arrays.stream(values())
        .collect(Collectors.toUnmodifiableMap(TransactionState::id, Function.identity()));

    public static final Set<TransactionState> ALL_STATES = Set.copyOf(EnumSet.allOf(TransactionState.class));

    private final byte id;
    private final String stateName;
    public static final Map<TransactionState, Set<TransactionState>> VALID_PREVIOUS_STATES = Map.of(
        EMPTY, Set.of(EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT),
        ONGOING, Set.of(ONGOING, EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT),
        PREPARE_COMMIT, Set.of(ONGOING),
        PREPARE_ABORT, Set.of(ONGOING, PREPARE_EPOCH_FENCE, EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT),
        COMPLETE_COMMIT, Set.of(PREPARE_COMMIT),
        COMPLETE_ABORT, Set.of(PREPARE_ABORT),
        DEAD, Set.of(EMPTY, COMPLETE_ABORT, COMPLETE_COMMIT),
        PREPARE_EPOCH_FENCE, Set.of(ONGOING)
    );

    private final boolean expirationAllowed;

    TransactionState(byte id, String name, boolean expirationAllowed) {
        this.id = id;
        this.stateName = name;
        this.expirationAllowed = expirationAllowed;
    }

    /**
     * @return The state id byte.
     */
    public byte id() {
        return id;
    }

    /**
     * Get the name of this state. This is exposed through the `DescribeTransactions` API.
     * @return The state name string.
     */
    public String stateName() {
        return stateName;
    }

    /**
     * @return The set of states from which it is valid to transition into this state.
     */
    public Set<TransactionState> validPreviousStates() {
        return VALID_PREVIOUS_STATES.getOrDefault(this, Set.of());
    }

    /**
     * @return True if expiration is allowed in this state, false otherwise.
     */
    public boolean isExpirationAllowed() {
        return expirationAllowed;
    }

    /**
     * Finds a TransactionState by its name.
     * @param name The name of the state.
     * @return An Optional containing the TransactionState if found, otherwise empty.
     */
    public static Optional<TransactionState> fromName(String name) {
        return Optional.ofNullable(NAME_TO_ENUM.get(name));
    }

    /**
     * Finds a TransactionState by its ID.
     * @param id The byte ID of the state.
     * @return The TransactionState corresponding to the ID.
     * @throws IllegalStateException if the ID is unknown.
     */
    public static TransactionState fromId(byte id) {
        TransactionState state = ID_TO_ENUM.get(id);
        if (state == null) {
            throw new IllegalStateException("Unknown transaction state id " + id + " from the transaction status message");
        }
        return state;
    }
}
