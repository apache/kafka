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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest.CommittedOffset;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.kafka.common.record.RecordBatch.NO_PRODUCER_EPOCH;
import static org.apache.kafka.common.record.RecordBatch.NO_PRODUCER_ID;

/**
 * A class which maintains state for transactions. Also keeps the state necessary to ensure idempotent production.
 */
public class TransactionManager {
    private static final int NO_INFLIGHT_REQUEST_CORRELATION_ID = -1;
    private static final int NO_LAST_ACKED_SEQUENCE_NUMBER = -1;

    private final Logger log;
    private final String transactionalId;
    private final int transactionTimeoutMs;

    private static class TopicPartitionBookkeeper {

        private final Map<TopicPartition, TopicPartitionEntry> topicPartitionBookkeeping = new HashMap<>();

        public TopicPartitionEntry getPartition(TopicPartition topic) {
            TopicPartitionEntry ent = topicPartitionBookkeeping.get(topic);
            if (ent == null)
                throw new IllegalStateException("Trying to get the sequence number for " + topic +
                        ", but the sequence number was never set for this partition.");
            return ent;
        }

        public void addPartition(TopicPartition topic) {
            if (!topicPartitionBookkeeping.containsKey(topic))
                topicPartitionBookkeeping.put(topic, new TopicPartitionEntry());
        }

        boolean contains(TopicPartition partition) {
            return topicPartitionBookkeeping.containsKey(partition);
        }

        public void reset() {
            topicPartitionBookkeeping.clear();
        }

        OptionalLong lastAckedOffset(TopicPartition partition) {
            TopicPartitionEntry entry = topicPartitionBookkeeping.get(partition);
            if (entry != null && entry.lastAckedOffset != ProduceResponse.INVALID_OFFSET)
                return OptionalLong.of(entry.lastAckedOffset);
            else
                return OptionalLong.empty();
        }

        OptionalInt lastAckedSequence(TopicPartition partition) {
            TopicPartitionEntry entry = topicPartitionBookkeeping.get(partition);
            if (entry != null && entry.lastAckedSequence != NO_LAST_ACKED_SEQUENCE_NUMBER)
                return OptionalInt.of(entry.lastAckedSequence);
            else
                return OptionalInt.empty();
        }
    }

    private static class TopicPartitionEntry {

        // The base sequence of the next batch bound for a given partition.
        private int nextSequence;

        // The sequence number of the last record of the last ack'd batch from the given partition. When there are no
        // in flight requests for a partition, the lastAckedSequence(topicPartition) == nextSequence(topicPartition) - 1.
        private int lastAckedSequence;

        // Keep track of the in flight batches bound for a partition, ordered by sequence. This helps us to ensure that
        // we continue to order batches by the sequence numbers even when the responses come back out of order during
        // leader failover. We add a batch to the queue when it is drained, and remove it when the batch completes
        // (either successfully or through a fatal failure).
        private SortedSet<ProducerBatch> inflightBatchesBySequence;

        // We keep track of the last acknowledged offset on a per partition basis in order to disambiguate UnknownProducer
        // responses which are due to the retention period elapsing, and those which are due to actual lost data.
        private long lastAckedOffset;

        TopicPartitionEntry() {
            this.nextSequence = 0;
            this.lastAckedSequence = NO_LAST_ACKED_SEQUENCE_NUMBER;
            this.lastAckedOffset = ProduceResponse.INVALID_OFFSET;
            this.inflightBatchesBySequence = new TreeSet<>(Comparator.comparingInt(ProducerBatch::baseSequence));
        }

        public void resetSequenceNumbers(Consumer<ProducerBatch> resetSequence) {
            TreeSet<ProducerBatch> newInflights = new TreeSet<>(Comparator.comparingInt(ProducerBatch::baseSequence));
            for (ProducerBatch inflightBatch : inflightBatchesBySequence) {
                resetSequence.accept(inflightBatch);
                newInflights.add(inflightBatch);
            }
            inflightBatchesBySequence = newInflights;
        }

    }

    private final TopicPartitionBookkeeper topicPartitionBookkeeper;

    private final Map<TopicPartition, CommittedOffset> pendingTxnOffsetCommits;

    // If a batch bound for a partition expired locally after being sent at least once, the partition has is considered
    // to have an unresolved state. We keep track fo such partitions here, and cannot assign any more sequence numbers
    // for this partition until the unresolved state gets cleared. This may happen if other inflight batches returned
    // successfully (indicating that the expired batch actually made it to the broker). If we don't get any successful
    // responses for the partition once the inflight request count falls to zero, we reset the producer id and
    // consequently clear this data structure as well.
    private final Set<TopicPartition> partitionsWithUnresolvedSequences;

    private final PriorityQueue<TxnRequestHandler> pendingRequests;
    private final Set<TopicPartition> newPartitionsInTransaction;
    private final Set<TopicPartition> pendingPartitionsInTransaction;
    private final Set<TopicPartition> partitionsInTransaction;
    private TransactionalRequestResult pendingResult;

    // This is used by the TxnRequestHandlers to control how long to back off before a given request is retried.
    // For instance, this value is lowered by the AddPartitionsToTxnHandler when it receives a CONCURRENT_TRANSACTIONS
    // error for the first AddPartitionsRequest in a transaction.
    private final long retryBackoffMs;

    // The retryBackoff is overridden to the following value if the first AddPartitions receives a
    // CONCURRENT_TRANSACTIONS error.
    private static final long ADD_PARTITIONS_RETRY_BACKOFF_MS = 20L;

    private int inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID;
    private Node transactionCoordinator;
    private Node consumerGroupCoordinator;

    private volatile State currentState = State.UNINITIALIZED;
    private volatile RuntimeException lastError = null;
    private volatile ProducerIdAndEpoch producerIdAndEpoch;
    private volatile boolean transactionStarted = false;

    private enum State {
        UNINITIALIZED,
        INITIALIZING,
        READY,
        IN_TRANSACTION,
        COMMITTING_TRANSACTION,
        ABORTING_TRANSACTION,
        ABORTABLE_ERROR,
        FATAL_ERROR;

        private boolean isTransitionValid(State source, State target) {
            switch (target) {
                case INITIALIZING:
                    return source == UNINITIALIZED;
                case READY:
                    return source == INITIALIZING || source == COMMITTING_TRANSACTION || source == ABORTING_TRANSACTION;
                case IN_TRANSACTION:
                    return source == READY;
                case COMMITTING_TRANSACTION:
                    return source == IN_TRANSACTION;
                case ABORTING_TRANSACTION:
                    return source == IN_TRANSACTION || source == ABORTABLE_ERROR;
                case ABORTABLE_ERROR:
                    return source == IN_TRANSACTION || source == COMMITTING_TRANSACTION || source == ABORTABLE_ERROR;
                case FATAL_ERROR:
                default:
                    // We can transition to FATAL_ERROR unconditionally.
                    // FATAL_ERROR is never a valid starting state for any transition. So the only option is to close the
                    // producer or do purely non transactional requests.
                    return true;
            }
        }
    }

    // We use the priority to determine the order in which requests need to be sent out. For instance, if we have
    // a pending FindCoordinator request, that must always go first. Next, If we need a producer id, that must go second.
    // The endTxn request must always go last.
    private enum Priority {
        FIND_COORDINATOR(0),
        INIT_PRODUCER_ID(1),
        ADD_PARTITIONS_OR_OFFSETS(2),
        END_TXN(3);

        final int priority;

        Priority(int priority) {
            this.priority = priority;
        }
    }

    public TransactionManager(LogContext logContext, String transactionalId, int transactionTimeoutMs, long retryBackoffMs) {
        this.producerIdAndEpoch = new ProducerIdAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH);
        this.transactionalId = transactionalId;
        this.log = logContext.logger(TransactionManager.class);
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.transactionCoordinator = null;
        this.consumerGroupCoordinator = null;
        this.newPartitionsInTransaction = new HashSet<>();
        this.pendingPartitionsInTransaction = new HashSet<>();
        this.partitionsInTransaction = new HashSet<>();
        this.pendingRequests = new PriorityQueue<>(10, Comparator.comparingInt(o -> o.priority().priority));
        this.pendingTxnOffsetCommits = new HashMap<>();
        this.partitionsWithUnresolvedSequences = new HashSet<>();
        this.retryBackoffMs = retryBackoffMs;
        this.topicPartitionBookkeeper = new TopicPartitionBookkeeper();
    }

    TransactionManager() {
        this(new LogContext(), null, 0, 100L);
    }

    public synchronized TransactionalRequestResult initializeTransactions() {
        return handleCachedTransactionRequestResult(() -> {
            transitionTo(State.INITIALIZING);
            setProducerIdAndEpoch(ProducerIdAndEpoch.NONE);
            InitProducerIdRequestData requestData = new InitProducerIdRequestData()
                    .setTransactionalId(transactionalId)
                    .setTransactionTimeoutMs(transactionTimeoutMs);
            InitProducerIdHandler handler = new InitProducerIdHandler(new InitProducerIdRequest.Builder(requestData));
            enqueueRequest(handler);
            return handler.result;
        }, State.INITIALIZING);
    }

    public synchronized void beginTransaction() {
        ensureTransactional();
        maybeFailWithError();
        transitionTo(State.IN_TRANSACTION);
    }

    public synchronized TransactionalRequestResult beginCommit() {
        return handleCachedTransactionRequestResult(() -> {
            maybeFailWithError();
            transitionTo(State.COMMITTING_TRANSACTION);
            return beginCompletingTransaction(TransactionResult.COMMIT);
        }, State.COMMITTING_TRANSACTION);
    }

    public synchronized TransactionalRequestResult beginAbort() {
        return handleCachedTransactionRequestResult(() -> {
            if (currentState != State.ABORTABLE_ERROR)
                maybeFailWithError();
            transitionTo(State.ABORTING_TRANSACTION);

            // We're aborting the transaction, so there should be no need to add new partitions
            newPartitionsInTransaction.clear();
            return beginCompletingTransaction(TransactionResult.ABORT);
        }, State.ABORTING_TRANSACTION);
    }

    private TransactionalRequestResult beginCompletingTransaction(TransactionResult transactionResult) {
        if (!newPartitionsInTransaction.isEmpty())
            enqueueRequest(addPartitionsToTransactionHandler());
        EndTxnRequest.Builder builder = new EndTxnRequest.Builder(transactionalId, producerIdAndEpoch.producerId,
                producerIdAndEpoch.epoch, transactionResult);
        EndTxnHandler handler = new EndTxnHandler(builder);
        enqueueRequest(handler);
        return handler.result;
    }

    public synchronized TransactionalRequestResult sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                                                            String consumerGroupId) {
        ensureTransactional();
        maybeFailWithError();
        if (currentState != State.IN_TRANSACTION)
            throw new KafkaException("Cannot send offsets to transaction either because the producer is not in an " +
                    "active transaction");

        log.debug("Begin adding offsets {} for consumer group {} to transaction", offsets, consumerGroupId);
        AddOffsetsToTxnRequest.Builder builder = new AddOffsetsToTxnRequest.Builder(transactionalId,
            producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, consumerGroupId);
        AddOffsetsToTxnHandler handler = new AddOffsetsToTxnHandler(builder, offsets);
        enqueueRequest(handler);
        return handler.result;
    }

    public synchronized void maybeAddPartitionToTransaction(TopicPartition topicPartition) {
        if (isPartitionAdded(topicPartition) || isPartitionPendingAdd(topicPartition))
            return;

        log.debug("Begin adding new partition {} to transaction", topicPartition);
        topicPartitionBookkeeper.addPartition(topicPartition);
        newPartitionsInTransaction.add(topicPartition);
    }

    RuntimeException lastError() {
        return lastError;
    }

    public synchronized void failIfNotReadyForSend() {
        if (hasError())
            throw new KafkaException("Cannot perform send because at least one previous transactional or " +
                    "idempotent request has failed with errors.", lastError);

        if (isTransactional()) {
            if (!hasProducerId())
                throw new IllegalStateException("Cannot perform a 'send' before completing a call to initTransactions " +
                        "when transactions are enabled.");

            if (currentState != State.IN_TRANSACTION)
                throw new IllegalStateException("Cannot call send in state " + currentState);
        }
    }

    synchronized boolean isSendToPartitionAllowed(TopicPartition tp) {
        if (hasFatalError())
            return false;
        return !isTransactional() || partitionsInTransaction.contains(tp);
    }

    public String transactionalId() {
        return transactionalId;
    }

    public boolean hasProducerId() {
        return producerIdAndEpoch.isValid();
    }

    public boolean isTransactional() {
        return transactionalId != null;
    }

    synchronized boolean hasPartitionsToAdd() {
        return !newPartitionsInTransaction.isEmpty() || !pendingPartitionsInTransaction.isEmpty();
    }

    synchronized boolean isCompleting() {
        return currentState == State.COMMITTING_TRANSACTION || currentState == State.ABORTING_TRANSACTION;
    }

    synchronized boolean hasError() {
        return currentState == State.ABORTABLE_ERROR || currentState == State.FATAL_ERROR;
    }

    synchronized boolean isAborting() {
        return currentState == State.ABORTING_TRANSACTION;
    }

    synchronized void transitionToAbortableError(RuntimeException exception) {
        if (currentState == State.ABORTING_TRANSACTION) {
            log.debug("Skipping transition to abortable error state since the transaction is already being " +
                    "aborted. Underlying exception: ", exception);
            return;
        }
        transitionTo(State.ABORTABLE_ERROR, exception);
    }

    synchronized void transitionToFatalError(RuntimeException exception) {
        transitionTo(State.FATAL_ERROR, exception);
    }

    // visible for testing
    synchronized boolean isPartitionAdded(TopicPartition partition) {
        return partitionsInTransaction.contains(partition);
    }

    // visible for testing
    synchronized boolean isPartitionPendingAdd(TopicPartition partition) {
        return newPartitionsInTransaction.contains(partition) || pendingPartitionsInTransaction.contains(partition);
    }

    /**
     * Get the current producer id and epoch without blocking. Callers must use {@link ProducerIdAndEpoch#isValid()} to
     * verify that the result is valid.
     *
     * @return the current ProducerIdAndEpoch.
     */
    ProducerIdAndEpoch producerIdAndEpoch() {
        return producerIdAndEpoch;
    }

    boolean hasProducerId(long producerId) {
        return producerIdAndEpoch.producerId == producerId;
    }

    boolean hasProducerIdAndEpoch(long producerId, short producerEpoch) {
        ProducerIdAndEpoch idAndEpoch = this.producerIdAndEpoch;
        return idAndEpoch.producerId == producerId && idAndEpoch.epoch == producerEpoch;
    }

    /**
     * Set the producer id and epoch atomically.
     */
    void setProducerIdAndEpoch(ProducerIdAndEpoch producerIdAndEpoch) {
        log.info("ProducerId set to {} with epoch {}", producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        this.producerIdAndEpoch = producerIdAndEpoch;
    }

    /**
     * This method is used when the producer needs to reset its internal state because of an irrecoverable exception
     * from the broker.
     *
     * We need to reset the producer id and associated state when we have sent a batch to the broker, but we either get
     * a non-retriable exception or we run out of retries, or the batch expired in the producer queue after it was already
     * sent to the broker.
     *
     * In all of these cases, we don't know whether batch was actually committed on the broker, and hence whether the
     * sequence number was actually updated. If we don't reset the producer state, we risk the chance that all future
     * messages will return an OutOfOrderSequenceException.
     *
     * Note that we can't reset the producer state for the transactional producer as this would mean bumping the epoch
     * for the same producer id. This might involve aborting the ongoing transaction during the initPidRequest, and the user
     * would not have any way of knowing this happened. So for the transactional producer, it's best to return the
     * produce error to the user and let them abort the transaction and close the producer explicitly.
     */
    synchronized void resetProducerId() {
        if (isTransactional())
            throw new IllegalStateException("Cannot reset producer state for a transactional producer. " +
                    "You must either abort the ongoing transaction or reinitialize the transactional producer instead");
        setProducerIdAndEpoch(ProducerIdAndEpoch.NONE);
        topicPartitionBookkeeper.reset();
        this.partitionsWithUnresolvedSequences.clear();
    }

    synchronized void resetProducerIdIfNeeded() {
        if (shouldResetProducerStateAfterResolvingSequences())
            // Check if the previous run expired batches which requires a reset of the producer state.
            resetProducerId();
    }

    /**
     * Returns the next sequence number to be written to the given TopicPartition.
     */
    synchronized Integer sequenceNumber(TopicPartition topicPartition) {
        if (!isTransactional())
            topicPartitionBookkeeper.addPartition(topicPartition);

        return topicPartitionBookkeeper.getPartition(topicPartition).nextSequence;
    }

    synchronized void incrementSequenceNumber(TopicPartition topicPartition, int increment) {
        Integer currentSequence = sequenceNumber(topicPartition);

        currentSequence = DefaultRecordBatch.incrementSequence(currentSequence, increment);
        topicPartitionBookkeeper.getPartition(topicPartition).nextSequence = currentSequence;
    }

    synchronized void addInFlightBatch(ProducerBatch batch) {
        if (!batch.hasSequence())
            throw new IllegalStateException("Can't track batch for partition " + batch.topicPartition + " when sequence is not set.");
        topicPartitionBookkeeper.getPartition(batch.topicPartition).inflightBatchesBySequence.add(batch);
    }

    /**
     * Returns the first inflight sequence for a given partition. This is the base sequence of an inflight batch with
     * the lowest sequence number.
     * @return the lowest inflight sequence if the transaction manager is tracking inflight requests for this partition.
     *         If there are no inflight requests being tracked for this partition, this method will return
     *         RecordBatch.NO_SEQUENCE.
     */
    synchronized int firstInFlightSequence(TopicPartition topicPartition) {
        if (!hasInflightBatches(topicPartition))
            return RecordBatch.NO_SEQUENCE;

        SortedSet<ProducerBatch> inflightBatches = topicPartitionBookkeeper.getPartition(topicPartition).inflightBatchesBySequence;
        if (inflightBatches.isEmpty())
            return RecordBatch.NO_SEQUENCE;
        else
            return inflightBatches.first().baseSequence();
    }

    synchronized ProducerBatch nextBatchBySequence(TopicPartition topicPartition) {
        SortedSet<ProducerBatch> queue = topicPartitionBookkeeper.getPartition(topicPartition).inflightBatchesBySequence;
        return queue.isEmpty() ? null : queue.first();
    }

    synchronized void removeInFlightBatch(ProducerBatch batch) {
        if (hasInflightBatches(batch.topicPartition)) {
            topicPartitionBookkeeper.getPartition(batch.topicPartition).inflightBatchesBySequence.remove(batch);
        }
    }

    private void maybeUpdateLastAckedSequence(TopicPartition topicPartition, int sequence) {
        if (sequence > lastAckedSequence(topicPartition).orElse(NO_LAST_ACKED_SEQUENCE_NUMBER))
            topicPartitionBookkeeper.getPartition(topicPartition).lastAckedSequence = sequence;
    }

    synchronized OptionalInt lastAckedSequence(TopicPartition topicPartition) {
        return topicPartitionBookkeeper.lastAckedSequence(topicPartition);
    }

    synchronized OptionalLong lastAckedOffset(TopicPartition topicPartition) {
        return topicPartitionBookkeeper.lastAckedOffset(topicPartition);
    }

    private void updateLastAckedOffset(ProduceResponse.PartitionResponse response, ProducerBatch batch) {
        if (response.baseOffset == ProduceResponse.INVALID_OFFSET)
            return;
        long lastOffset = response.baseOffset + batch.recordCount - 1;
        OptionalLong lastAckedOffset = lastAckedOffset(batch.topicPartition);
        // It might happen that the TransactionManager has been reset while a request was reenqueued and got a valid
        // response for this. This can happen only if the producer is only idempotent (not transactional) and in
        // this case there will be no tracked bookkeeper entry about it, so we have to insert one.
        if (!lastAckedOffset.isPresent() && !isTransactional()) {
            topicPartitionBookkeeper.addPartition(batch.topicPartition);
        }
        if (lastOffset > lastAckedOffset.orElse(ProduceResponse.INVALID_OFFSET)) {
            topicPartitionBookkeeper.getPartition(batch.topicPartition).lastAckedOffset = lastOffset;
        } else {
            log.trace("Partition {} keeps lastOffset at {}", batch.topicPartition, lastOffset);
        }
    }

    public synchronized void handleCompletedBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
        if (!hasProducerIdAndEpoch(batch.producerId(), batch.producerEpoch())) {
            log.debug("Ignoring completed batch {} with producer id {}, epoch {}, and sequence number {} " +
                            "since the producerId has been reset internally", batch, batch.producerId(),
                    batch.producerEpoch(), batch.baseSequence());
            return;
        }

        maybeUpdateLastAckedSequence(batch.topicPartition, batch.baseSequence() + batch.recordCount - 1);
        log.debug("ProducerId: {}; Set last ack'd sequence number for topic-partition {} to {}",
                batch.producerId(),
                batch.topicPartition,
                lastAckedSequence(batch.topicPartition).orElse(-1));

        updateLastAckedOffset(response, batch);
        removeInFlightBatch(batch);
    }

    private void maybeTransitionToErrorState(RuntimeException exception) {
        if (exception instanceof ClusterAuthorizationException
                || exception instanceof TransactionalIdAuthorizationException
                || exception instanceof ProducerFencedException
                || exception instanceof UnsupportedVersionException) {
            transitionToFatalError(exception);
        } else if (isTransactional()) {
            transitionToAbortableError(exception);
        }
    }

    public synchronized void handleFailedBatch(ProducerBatch batch, RuntimeException exception, boolean adjustSequenceNumbers) {
        maybeTransitionToErrorState(exception);

        if (!hasProducerIdAndEpoch(batch.producerId(), batch.producerEpoch())) {
            log.debug("Ignoring failed batch {} with producer id {}, epoch {}, and sequence number {} " +
                    "since the producerId has been reset internally", batch, batch.producerId(),
                    batch.producerEpoch(), batch.baseSequence(), exception);
            return;
        }

        if (exception instanceof OutOfOrderSequenceException && !isTransactional()) {
            log.error("The broker returned {} for topic-partition {} with producerId {}, epoch {}, and sequence number {}",
                    exception, batch.topicPartition, batch.producerId(), batch.producerEpoch(), batch.baseSequence());

            // Reset the producerId since we have hit an irrecoverable exception and cannot make any guarantees
            // about the previously committed message. Note that this will discard the producer id and sequence
            // numbers for all existing partitions.
            resetProducerId();
        } else {
            removeInFlightBatch(batch);
            if (adjustSequenceNumbers)
                adjustSequencesDueToFailedBatch(batch);
        }
    }

    // If a batch is failed fatally, the sequence numbers for future batches bound for the partition must be adjusted
    // so that they don't fail with the OutOfOrderSequenceException.
    //
    // This method must only be called when we know that the batch is question has been unequivocally failed by the broker,
    // ie. it has received a confirmed fatal status code like 'Message Too Large' or something similar.
    private void adjustSequencesDueToFailedBatch(ProducerBatch batch) {
        if (!topicPartitionBookkeeper.contains(batch.topicPartition))
            // Sequence numbers are not being tracked for this partition. This could happen if the producer id was just
            // reset due to a previous OutOfOrderSequenceException.
            return;
        log.debug("producerId: {}, send to partition {} failed fatally. Reducing future sequence numbers by {}",
                batch.producerId(), batch.topicPartition, batch.recordCount);
        int currentSequence = sequenceNumber(batch.topicPartition);
        currentSequence -= batch.recordCount;
        if (currentSequence < 0)
            throw new IllegalStateException("Sequence number for partition " + batch.topicPartition + " is going to become negative: " + currentSequence);

        setNextSequence(batch.topicPartition, currentSequence);

        topicPartitionBookkeeper.getPartition(batch.topicPartition).resetSequenceNumbers(inFlightBatch -> {
            if (inFlightBatch.baseSequence() < batch.baseSequence())
                return;

            int newSequence = inFlightBatch.baseSequence() - batch.recordCount;
            if (newSequence < 0)
                throw new IllegalStateException("Sequence number for batch with sequence " + inFlightBatch.baseSequence()
                        + " for partition " + batch.topicPartition + " is going to become negative: " + newSequence);

            log.info("Resetting sequence number of batch with current sequence {} for partition {} to {}", inFlightBatch.baseSequence(), batch.topicPartition, newSequence);
            inFlightBatch.resetProducerState(new ProducerIdAndEpoch(inFlightBatch.producerId(), inFlightBatch.producerEpoch()), newSequence, inFlightBatch.isTransactional());

        });
    }

    private void startSequencesAtBeginning(TopicPartition topicPartition) {
        final PrimitiveRef.IntRef sequence = PrimitiveRef.ofInt(0);
        topicPartitionBookkeeper.getPartition(topicPartition).resetSequenceNumbers(inFlightBatch -> {
            log.info("Resetting sequence number of batch with current sequence {} for partition {} to {}",
                    inFlightBatch.baseSequence(), inFlightBatch.topicPartition, sequence.value);
            inFlightBatch.resetProducerState(new ProducerIdAndEpoch(inFlightBatch.producerId(),
                    inFlightBatch.producerEpoch()), sequence.value, inFlightBatch.isTransactional());
            sequence.value += inFlightBatch.recordCount;
        });
        setNextSequence(topicPartition, sequence.value);
        topicPartitionBookkeeper.getPartition(topicPartition).lastAckedSequence = NO_LAST_ACKED_SEQUENCE_NUMBER;
    }

    private boolean hasInflightBatches(TopicPartition topicPartition) {
        return topicPartitionBookkeeper.contains(topicPartition)
                && !topicPartitionBookkeeper.getPartition(topicPartition).inflightBatchesBySequence.isEmpty();
    }

    synchronized boolean hasUnresolvedSequences() {
        return !partitionsWithUnresolvedSequences.isEmpty();
    }

    synchronized boolean hasUnresolvedSequence(TopicPartition topicPartition) {
        return partitionsWithUnresolvedSequences.contains(topicPartition);
    }

    synchronized void markSequenceUnresolved(TopicPartition topicPartition) {
        log.debug("Marking partition {} unresolved", topicPartition);
        partitionsWithUnresolvedSequences.add(topicPartition);
    }

    // Checks if there are any partitions with unresolved partitions which may now be resolved. Returns true if
    // the producer id needs a reset, false otherwise.
    private boolean shouldResetProducerStateAfterResolvingSequences() {
        if (isTransactional())
            // We should not reset producer state if we are transactional. We will transition to a fatal error instead.
            return false;
        for (Iterator<TopicPartition> iter = partitionsWithUnresolvedSequences.iterator(); iter.hasNext(); ) {
            TopicPartition topicPartition = iter.next();
            if (!hasInflightBatches(topicPartition)) {
                // The partition has been fully drained. At this point, the last ack'd sequence should be once less than
                // next sequence destined for the partition. If so, the partition is fully resolved. If not, we should
                // reset the sequence number if necessary.
                if (isNextSequence(topicPartition, sequenceNumber(topicPartition))) {
                    // This would happen when a batch was expired, but subsequent batches succeeded.
                    iter.remove();
                } else {
                    // We would enter this branch if all in flight batches were ultimately expired in the producer.
                    log.info("No inflight batches remaining for {}, last ack'd sequence for partition is {}, next sequence is {}. " +
                            "Going to reset producer state.", topicPartition,
                            lastAckedSequence(topicPartition).orElse(NO_LAST_ACKED_SEQUENCE_NUMBER), sequenceNumber(topicPartition));
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isNextSequence(TopicPartition topicPartition, int sequence) {
        return sequence - lastAckedSequence(topicPartition).orElse(NO_LAST_ACKED_SEQUENCE_NUMBER) == 1;
    }

    private void setNextSequence(TopicPartition topicPartition, int sequence) {
        topicPartitionBookkeeper.getPartition(topicPartition).nextSequence = sequence;
    }

    synchronized TxnRequestHandler nextRequestHandler(boolean hasIncompleteBatches) {
        if (!newPartitionsInTransaction.isEmpty())
            enqueueRequest(addPartitionsToTransactionHandler());

        TxnRequestHandler nextRequestHandler = pendingRequests.peek();
        if (nextRequestHandler == null)
            return null;

        // Do not send the EndTxn until all batches have been flushed
        if (nextRequestHandler.isEndTxn() && hasIncompleteBatches)
            return null;

        pendingRequests.poll();
        if (maybeTerminateRequestWithError(nextRequestHandler)) {
            log.trace("Not sending transactional request {} because we are in an error state",
                    nextRequestHandler.requestBuilder());
            return null;
        }

        if (nextRequestHandler.isEndTxn() && !transactionStarted) {
            nextRequestHandler.result.done();
            if (currentState != State.FATAL_ERROR) {
                log.debug("Not sending EndTxn for completed transaction since no partitions " +
                        "or offsets were successfully added");
                completeTransaction();
            }
            nextRequestHandler = pendingRequests.poll();
        }

        if (nextRequestHandler != null)
            log.trace("Request {} dequeued for sending", nextRequestHandler.requestBuilder());

        return nextRequestHandler;
    }

    synchronized void retry(TxnRequestHandler request) {
        request.setRetry();
        enqueueRequest(request);
    }

    synchronized void authenticationFailed(AuthenticationException e) {
        for (TxnRequestHandler request : pendingRequests)
            request.fatalError(e);
    }

    synchronized void close() {
        KafkaException shutdownException = new KafkaException("The producer closed forcefully");
        pendingRequests.forEach(handler ->
                handler.fatalError(shutdownException));
        if (pendingResult != null) {
            pendingResult.setError(shutdownException);
            pendingResult.done();
        }
    }

    Node coordinator(FindCoordinatorRequest.CoordinatorType type) {
        switch (type) {
            case GROUP:
                return consumerGroupCoordinator;
            case TRANSACTION:
                return transactionCoordinator;
            default:
                throw new IllegalStateException("Received an invalid coordinator type: " + type);
        }
    }

    void lookupCoordinator(TxnRequestHandler request) {
        lookupCoordinator(request.coordinatorType(), request.coordinatorKey());
    }

    void setInFlightCorrelationId(int correlationId) {
        inFlightRequestCorrelationId = correlationId;
    }

    private void clearInFlightCorrelationId() {
        inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    boolean hasInFlightTransactionalRequest() {
        return inFlightRequestCorrelationId != NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    // visible for testing.
    boolean hasFatalError() {
        return currentState == State.FATAL_ERROR;
    }

    // visible for testing.
    boolean hasAbortableError() {
        return currentState == State.ABORTABLE_ERROR;
    }

    // visible for testing
    synchronized boolean transactionContainsPartition(TopicPartition topicPartition) {
        return partitionsInTransaction.contains(topicPartition);
    }

    // visible for testing
    synchronized boolean hasPendingOffsetCommits() {
        return !pendingTxnOffsetCommits.isEmpty();
    }

    synchronized boolean hasPendingRequests() {
        return !pendingRequests.isEmpty();
    }

    // visible for testing
    synchronized boolean hasOngoingTransaction() {
        // transactions are considered ongoing once started until completion or a fatal error
        return currentState == State.IN_TRANSACTION || isCompleting() || hasAbortableError();
    }

    synchronized boolean canRetry(ProduceResponse.PartitionResponse response, ProducerBatch batch) {
        if (!hasProducerIdAndEpoch(batch.producerId(), batch.producerEpoch()))
            return false;

        Errors error = response.error;
        if (error == Errors.OUT_OF_ORDER_SEQUENCE_NUMBER && !hasUnresolvedSequence(batch.topicPartition) &&
                (batch.sequenceHasBeenReset() || !isNextSequence(batch.topicPartition, batch.baseSequence())))
            // We should retry the OutOfOrderSequenceException if the batch is _not_ the next batch, ie. its base
            // sequence isn't the lastAckedSequence + 1. However, if the first in flight batch fails fatally, we will
            // adjust the sequences of the other inflight batches to account for the 'loss' of the sequence range in
            // the batch which failed. In this case, an inflight batch will have a base sequence which is
            // the lastAckedSequence + 1 after adjustment. When this batch fails with an OutOfOrderSequence, we want to retry it.
            // To account for the latter case, we check whether the sequence has been reset since the last drain.
            // If it has, we will retry it anyway.
            return true;

        if (error == Errors.UNKNOWN_PRODUCER_ID) {
            if (response.logStartOffset == -1)
                // We don't know the log start offset with this response. We should just retry the request until we get it.
                // The UNKNOWN_PRODUCER_ID error code was added along with the new ProduceResponse which includes the
                // logStartOffset. So the '-1' sentinel is not for backward compatibility. Instead, it is possible for
                // a broker to not know the logStartOffset at when it is returning the response because the partition
                // may have moved away from the broker from the time the error was initially raised to the time the
                // response was being constructed. In these cases, we should just retry the request: we are guaranteed
                // to eventually get a logStartOffset once things settle down.
                return true;

            if (batch.sequenceHasBeenReset()) {
                // When the first inflight batch fails due to the truncation case, then the sequences of all the other
                // in flight batches would have been restarted from the beginning. However, when those responses
                // come back from the broker, they would also come with an UNKNOWN_PRODUCER_ID error. In this case, we should not
                // reset the sequence numbers to the beginning.
                return true;
            } else if (lastAckedOffset(batch.topicPartition).orElse(NO_LAST_ACKED_SEQUENCE_NUMBER) < response.logStartOffset) {
                // The head of the log has been removed, probably due to the retention time elapsing. In this case,
                // we expect to lose the producer state. Reset the sequences of all inflight batches to be from the beginning
                // and retry them.
                startSequencesAtBeginning(batch.topicPartition);
                return true;
            }
        }
        return false;
    }

    // visible for testing
    synchronized boolean isReady() {
        return isTransactional() && currentState == State.READY;
    }

    private void transitionTo(State target) {
        transitionTo(target, null);
    }

    private void transitionTo(State target, RuntimeException error) {
        if (!currentState.isTransitionValid(currentState, target)) {
            String idString = transactionalId == null ?  "" : "TransactionalId " + transactionalId + ": ";
            throw new KafkaException(idString + "Invalid transition attempted from state "
                    + currentState.name() + " to state " + target.name());
        }

        if (target == State.FATAL_ERROR || target == State.ABORTABLE_ERROR) {
            if (error == null)
                throw new IllegalArgumentException("Cannot transition to " + target + " with a null exception");
            lastError = error;
        } else {
            lastError = null;
        }

        if (lastError != null)
            log.debug("Transition from state {} to error state {}", currentState, target, lastError);
        else
            log.debug("Transition from state {} to {}", currentState, target);

        currentState = target;
    }

    private void ensureTransactional() {
        if (!isTransactional())
            throw new IllegalStateException("Transactional method invoked on a non-transactional producer.");
    }

    private void maybeFailWithError() {
        if (hasError()) {
            // for ProducerFencedException, do not wrap it as a KafkaException
            // but create a new instance without the call trace since it was not thrown because of the current call
            if (lastError instanceof ProducerFencedException) {
                throw new ProducerFencedException("The producer has been rejected from the broker because " +
                    "it tried to use an old epoch with the transactionalId");
            } else {
                throw new KafkaException("Cannot execute transactional method because we are in an error state", lastError);
            }
        }
    }

    private boolean maybeTerminateRequestWithError(TxnRequestHandler requestHandler) {
        if (hasError()) {
            if (hasAbortableError() && requestHandler instanceof FindCoordinatorHandler)
                // No harm letting the FindCoordinator request go through if we're expecting to abort
                return false;

            requestHandler.fail(lastError);
            return true;
        }
        return false;
    }

    private void enqueueRequest(TxnRequestHandler requestHandler) {
        log.debug("Enqueuing transactional request {}", requestHandler.requestBuilder());
        pendingRequests.add(requestHandler);
    }

    private void lookupCoordinator(FindCoordinatorRequest.CoordinatorType type, String coordinatorKey) {
        switch (type) {
            case GROUP:
                consumerGroupCoordinator = null;
                break;
            case TRANSACTION:
                transactionCoordinator = null;
                break;
            default:
                throw new IllegalStateException("Invalid coordinator type: " + type);
        }

        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(
                new FindCoordinatorRequestData()
                    .setKeyType(type.id())
                    .setKey(coordinatorKey));
        enqueueRequest(new FindCoordinatorHandler(builder));
    }

    private void completeTransaction() {
        transitionTo(State.READY);
        lastError = null;
        transactionStarted = false;
        newPartitionsInTransaction.clear();
        pendingPartitionsInTransaction.clear();
        partitionsInTransaction.clear();
    }

    private TxnRequestHandler addPartitionsToTransactionHandler() {
        pendingPartitionsInTransaction.addAll(newPartitionsInTransaction);
        newPartitionsInTransaction.clear();
        AddPartitionsToTxnRequest.Builder builder = new AddPartitionsToTxnRequest.Builder(transactionalId,
                producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, new ArrayList<>(pendingPartitionsInTransaction));
        return new AddPartitionsToTxnHandler(builder);
    }

    private TxnOffsetCommitHandler txnOffsetCommitHandler(TransactionalRequestResult result,
                                                          Map<TopicPartition, OffsetAndMetadata> offsets,
                                                          String consumerGroupId) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            CommittedOffset committedOffset = new CommittedOffset(offsetAndMetadata.offset(),
                    offsetAndMetadata.metadata(), offsetAndMetadata.leaderEpoch());
            pendingTxnOffsetCommits.put(entry.getKey(), committedOffset);
        }
        TxnOffsetCommitRequest.Builder builder = new TxnOffsetCommitRequest.Builder(transactionalId, consumerGroupId,
                producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, pendingTxnOffsetCommits);
        return new TxnOffsetCommitHandler(result, builder);
    }

    private TransactionalRequestResult handleCachedTransactionRequestResult(
            Supplier<TransactionalRequestResult> transactionalRequestResultSupplier,
            State targetState) {
        ensureTransactional();

        if (pendingResult != null && currentState == targetState) {
            TransactionalRequestResult result = pendingResult;
            if (result.isCompleted())
                pendingResult = null;
            return result;
        }

        pendingResult = transactionalRequestResultSupplier.get();
        return pendingResult;
    }

    abstract class TxnRequestHandler implements RequestCompletionHandler {
        protected final TransactionalRequestResult result;
        private boolean isRetry = false;

        TxnRequestHandler(TransactionalRequestResult result) {
            this.result = result;
        }

        TxnRequestHandler(String operation) {
            this(new TransactionalRequestResult(operation));
        }

        void fatalError(RuntimeException e) {
            result.setError(e);
            transitionToFatalError(e);
            result.done();
        }

        void abortableError(RuntimeException e) {
            result.setError(e);
            transitionToAbortableError(e);
            result.done();
        }

        void fail(RuntimeException e) {
            result.setError(e);
            result.done();
        }

        void reenqueue() {
            synchronized (TransactionManager.this) {
                this.isRetry = true;
                enqueueRequest(this);
            }
        }

        long retryBackoffMs() {
            return retryBackoffMs;
        }

        @Override
        public void onComplete(ClientResponse response) {
            if (response.requestHeader().correlationId() != inFlightRequestCorrelationId) {
                fatalError(new RuntimeException("Detected more than one in-flight transactional request."));
            } else {
                clearInFlightCorrelationId();
                if (response.wasDisconnected()) {
                    log.debug("Disconnected from {}. Will retry.", response.destination());
                    if (this.needsCoordinator())
                        lookupCoordinator(this.coordinatorType(), this.coordinatorKey());
                    reenqueue();
                } else if (response.versionMismatch() != null) {
                    fatalError(response.versionMismatch());
                } else if (response.hasResponse()) {
                    log.trace("Received transactional response {} for request {}", response.responseBody(),
                            requestBuilder());
                    synchronized (TransactionManager.this) {
                        handleResponse(response.responseBody());
                    }
                } else {
                    fatalError(new KafkaException("Could not execute transactional request for unknown reasons"));
                }
            }
        }

        boolean needsCoordinator() {
            return coordinatorType() != null;
        }

        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return FindCoordinatorRequest.CoordinatorType.TRANSACTION;
        }

        String coordinatorKey() {
            return transactionalId;
        }

        void setRetry() {
            this.isRetry = true;
        }

        boolean isRetry() {
            return isRetry;
        }

        boolean isEndTxn() {
            return false;
        }

        abstract AbstractRequest.Builder<?> requestBuilder();

        abstract void handleResponse(AbstractResponse responseBody);

        abstract Priority priority();
    }

    private class InitProducerIdHandler extends TxnRequestHandler {
        private final InitProducerIdRequest.Builder builder;

        private InitProducerIdHandler(InitProducerIdRequest.Builder builder) {
            super("InitProducerId");
            this.builder = builder;
        }

        @Override
        InitProducerIdRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.INIT_PRODUCER_ID;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            InitProducerIdResponse initProducerIdResponse = (InitProducerIdResponse) response;
            Errors error = initProducerIdResponse.error();

            if (error == Errors.NONE) {
                ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(initProducerIdResponse.data.producerId(),
                        initProducerIdResponse.data.producerEpoch());
                setProducerIdAndEpoch(producerIdAndEpoch);
                transitionTo(State.READY);
                lastError = null;
                result.done();
            } else if (error == Errors.NOT_COORDINATOR || error == Errors.COORDINATOR_NOT_AVAILABLE) {
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
                reenqueue();
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatalError(error.exception());
            } else {
                fatalError(new KafkaException("Unexpected error in InitProducerIdResponse; " + error.message()));
            }
        }
    }

    private class AddPartitionsToTxnHandler extends TxnRequestHandler {
        private final AddPartitionsToTxnRequest.Builder builder;
        private long retryBackoffMs;

        private AddPartitionsToTxnHandler(AddPartitionsToTxnRequest.Builder builder) {
            super("AddPartitionsToTxn");
            this.builder = builder;
            this.retryBackoffMs = TransactionManager.this.retryBackoffMs;
        }

        @Override
        AddPartitionsToTxnRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.ADD_PARTITIONS_OR_OFFSETS;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            AddPartitionsToTxnResponse addPartitionsToTxnResponse = (AddPartitionsToTxnResponse) response;
            Map<TopicPartition, Errors> errors = addPartitionsToTxnResponse.errors();
            boolean hasPartitionErrors = false;
            Set<String> unauthorizedTopics = new HashSet<>();
            retryBackoffMs = TransactionManager.this.retryBackoffMs;

            for (Map.Entry<TopicPartition, Errors> topicPartitionErrorEntry : errors.entrySet()) {
                TopicPartition topicPartition = topicPartitionErrorEntry.getKey();
                Errors error = topicPartitionErrorEntry.getValue();

                if (error == Errors.NONE) {
                    continue;
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                    lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                    reenqueue();
                    return;
                } else if (error == Errors.CONCURRENT_TRANSACTIONS) {
                    maybeOverrideRetryBackoffMs();
                    reenqueue();
                    return;
                } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                    reenqueue();
                    return;
                } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                    fatalError(error.exception());
                    return;
                } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                    fatalError(error.exception());
                    return;
                } else if (error == Errors.INVALID_PRODUCER_ID_MAPPING
                        || error == Errors.INVALID_TXN_STATE) {
                    fatalError(new KafkaException(error.exception()));
                    return;
                } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                    unauthorizedTopics.add(topicPartition.topic());
                } else if (error == Errors.OPERATION_NOT_ATTEMPTED) {
                    log.debug("Did not attempt to add partition {} to transaction because other partitions in the " +
                            "batch had errors.", topicPartition);
                    hasPartitionErrors = true;
                } else {
                    log.error("Could not add partition {} due to unexpected error {}", topicPartition, error);
                    hasPartitionErrors = true;
                }
            }

            Set<TopicPartition> partitions = errors.keySet();

            // Remove the partitions from the pending set regardless of the result. We use the presence
            // of partitions in the pending set to know when it is not safe to send batches. However, if
            // the partitions failed to be added and we enter an error state, we expect the batches to be
            // aborted anyway. In this case, we must be able to continue sending the batches which are in
            // retry for partitions that were successfully added.
            pendingPartitionsInTransaction.removeAll(partitions);

            if (!unauthorizedTopics.isEmpty()) {
                abortableError(new TopicAuthorizationException(unauthorizedTopics));
            } else if (hasPartitionErrors) {
                abortableError(new KafkaException("Could not add partitions to transaction due to errors: " + errors));
            } else {
                log.debug("Successfully added partitions {} to transaction", partitions);
                partitionsInTransaction.addAll(partitions);
                transactionStarted = true;
                result.done();
            }
        }

        @Override
        public long retryBackoffMs() {
            return Math.min(TransactionManager.this.retryBackoffMs, this.retryBackoffMs);
        }

        private void maybeOverrideRetryBackoffMs() {
            // We only want to reduce the backoff when retrying the first AddPartition which errored out due to a
            // CONCURRENT_TRANSACTIONS error since this means that the previous transaction is still completing and
            // we don't want to wait too long before trying to start the new one.
            //
            // This is only a temporary fix, the long term solution is being tracked in
            // https://issues.apache.org/jira/browse/KAFKA-5482
            if (partitionsInTransaction.isEmpty())
                this.retryBackoffMs = ADD_PARTITIONS_RETRY_BACKOFF_MS;
        }
    }

    private class FindCoordinatorHandler extends TxnRequestHandler {
        private final FindCoordinatorRequest.Builder builder;

        private FindCoordinatorHandler(FindCoordinatorRequest.Builder builder) {
            super("FindCoordinator");
            this.builder = builder;
        }

        @Override
        FindCoordinatorRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.FIND_COORDINATOR;
        }

        @Override
        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return null;
        }

        @Override
        String coordinatorKey() {
            return null;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            FindCoordinatorResponse findCoordinatorResponse = (FindCoordinatorResponse) response;
            Errors error = findCoordinatorResponse.error();
            CoordinatorType coordinatorType = CoordinatorType.forId(builder.data().keyType());

            if (error == Errors.NONE) {
                Node node = findCoordinatorResponse.node();
                switch (coordinatorType) {
                    case GROUP:
                        consumerGroupCoordinator = node;
                        break;
                    case TRANSACTION:
                        transactionCoordinator = node;
                }
                result.done();
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE) {
                reenqueue();
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatalError(error.exception());
            } else if (findCoordinatorResponse.error() == Errors.GROUP_AUTHORIZATION_FAILED) {
                abortableError(GroupAuthorizationException.forGroupId(builder.data().key()));
            } else {
                fatalError(new KafkaException(String.format("Could not find a coordinator with type %s with key %s due to" +
                        "unexpected error: %s", coordinatorType, builder.data().key(),
                        findCoordinatorResponse.data().errorMessage())));
            }
        }
    }

    private class EndTxnHandler extends TxnRequestHandler {
        private final EndTxnRequest.Builder builder;

        private EndTxnHandler(EndTxnRequest.Builder builder) {
            super("EndTxn(" + builder.result() + ")");
            this.builder = builder;
        }

        @Override
        EndTxnRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.END_TXN;
        }

        @Override
        boolean isEndTxn() {
            return true;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            EndTxnResponse endTxnResponse = (EndTxnResponse) response;
            Errors error = endTxnResponse.error();

            if (error == Errors.NONE) {
                completeTransaction();
                result.done();
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
                reenqueue();
            } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                fatalError(error.exception());
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatalError(error.exception());
            } else if (error == Errors.INVALID_TXN_STATE) {
                fatalError(error.exception());
            } else {
                fatalError(new KafkaException("Unhandled error in EndTxnResponse: " + error.message()));
            }
        }
    }

    private class AddOffsetsToTxnHandler extends TxnRequestHandler {
        private final AddOffsetsToTxnRequest.Builder builder;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        private AddOffsetsToTxnHandler(AddOffsetsToTxnRequest.Builder builder,
                                       Map<TopicPartition, OffsetAndMetadata> offsets) {
            super("AddOffsetsToTxn");
            this.builder = builder;
            this.offsets = offsets;
        }

        @Override
        AddOffsetsToTxnRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.ADD_PARTITIONS_OR_OFFSETS;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            AddOffsetsToTxnResponse addOffsetsToTxnResponse = (AddOffsetsToTxnResponse) response;
            Errors error = addOffsetsToTxnResponse.error();

            if (error == Errors.NONE) {
                log.debug("Successfully added partition for consumer group {} to transaction", builder.consumerGroupId());

                // note the result is not completed until the TxnOffsetCommit returns
                pendingRequests.add(txnOffsetCommitHandler(result, offsets, builder.consumerGroupId()));
                transactionStarted = true;
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.GROUP, builder.consumerGroupId());
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
                reenqueue();
            } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                fatalError(error.exception());
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatalError(error.exception());
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                abortableError(GroupAuthorizationException.forGroupId(builder.consumerGroupId()));
            } else {
                fatalError(new KafkaException("Unexpected error in AddOffsetsToTxnResponse: " + error.message()));
            }
        }
    }

    private class TxnOffsetCommitHandler extends TxnRequestHandler {
        private final TxnOffsetCommitRequest.Builder builder;

        private TxnOffsetCommitHandler(TransactionalRequestResult result,
                                       TxnOffsetCommitRequest.Builder builder) {
            super(result);
            this.builder = builder;
        }

        @Override
        TxnOffsetCommitRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.ADD_PARTITIONS_OR_OFFSETS;
        }

        @Override
        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return FindCoordinatorRequest.CoordinatorType.GROUP;
        }

        @Override
        String coordinatorKey() {
            return builder.consumerGroupId();
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            TxnOffsetCommitResponse txnOffsetCommitResponse = (TxnOffsetCommitResponse) response;
            boolean coordinatorReloaded = false;
            Map<TopicPartition, Errors> errors = txnOffsetCommitResponse.errors();

            log.debug("Received TxnOffsetCommit response for consumer group {}: {}", builder.consumerGroupId(),
                    errors);

            for (Map.Entry<TopicPartition, Errors> entry : errors.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                Errors error = entry.getValue();
                if (error == Errors.NONE) {
                    pendingTxnOffsetCommits.remove(topicPartition);
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR
                        || error == Errors.REQUEST_TIMED_OUT) {
                    if (!coordinatorReloaded) {
                        coordinatorReloaded = true;
                        lookupCoordinator(FindCoordinatorRequest.CoordinatorType.GROUP, builder.consumerGroupId());
                    }
                } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION
                        || error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                    // If the topic is unknown or the coordinator is loading, retry with the current coordinator
                    continue;
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    abortableError(GroupAuthorizationException.forGroupId(builder.consumerGroupId()));
                    break;
                } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED
                        || error == Errors.INVALID_PRODUCER_EPOCH
                        || error == Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT) {
                    fatalError(error.exception());
                    break;
                } else {
                    fatalError(new KafkaException("Unexpected error in TxnOffsetCommitResponse: " + error.message()));
                    break;
                }
            }

            if (result.isCompleted()) {
                pendingTxnOffsetCommits.clear();
            } else if (pendingTxnOffsetCommits.isEmpty()) {
                result.done();
            } else {
                // Retry the commits which failed with a retriable error
                reenqueue();
            }
        }
    }
}
