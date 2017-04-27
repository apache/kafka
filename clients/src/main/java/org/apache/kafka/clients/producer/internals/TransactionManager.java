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
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitPidRequest;
import org.apache.kafka.common.requests.InitPidResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import static org.apache.kafka.common.record.RecordBatch.NO_PRODUCER_EPOCH;
import static org.apache.kafka.common.record.RecordBatch.NO_PRODUCER_ID;

/**
 * A class which maintains state for transactions. Also keeps the state necessary to ensure idempotent production.
 */
public class TransactionManager {
    private static final Logger log = LoggerFactory.getLogger(TransactionManager.class);

    private static final int NO_INFLIGHT_REQUEST_CORRELATION_ID = -1;

    private volatile PidAndEpoch pidAndEpoch;
    private final Map<TopicPartition, Integer> sequenceNumbers;
    private final String transactionalId;
    private final int transactionTimeoutMs;
    private final PriorityQueue<TransactionalRequest> pendingTransactionalRequests;
    private final Set<TopicPartition> newPartitionsToBeAddedToTransaction;
    private final Set<TopicPartition> pendingPartitionsToBeAddedToTransaction;
    private final Set<TopicPartition> partitionsInTransaction;
    private final Map<TopicPartition, TxnOffsetCommitRequest.CommittedOffset> pendingTxnOffsetCommits;
    private int inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID;

    private Node transactionCoordinator;
    private Node consumerGroupCoordinator;
    private volatile State currentState = State.UNINITIALIZED;
    private Exception lastError = null;

    private enum State {
        UNINITIALIZED,
        INITIALIZING,
        READY,
        IN_TRANSACTION,
        COMMITTING_TRANSACTION,
        ABORTING_TRANSACTION,
        FENCED,
        ERROR;

        private boolean isTransitionValid(State source, State target) {
            switch (target) {
                case INITIALIZING:
                    return source == UNINITIALIZED || source == ERROR;
                case READY:
                    return source == INITIALIZING || source == COMMITTING_TRANSACTION || source == ABORTING_TRANSACTION;
                case IN_TRANSACTION:
                    return source == READY;
                case COMMITTING_TRANSACTION:
                    return source == IN_TRANSACTION;
                case ABORTING_TRANSACTION:
                    return source == IN_TRANSACTION || source == ERROR;
                default:
                    // We can transition to FENCED or ERROR unconditionally.
                    // FENCED is never a valid starting state for any transition. So the only option is to close the
                    // producer or do purely non transactional requests.
                    return true;
            }
        }
    }

    public TransactionManager(String transactionalId, int transactionTimeoutMs) {
        pidAndEpoch = new PidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH);
        sequenceNumbers = new HashMap<>();
        this.transactionalId = transactionalId;
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.pendingTransactionalRequests = new PriorityQueue<>(10, new Comparator<TransactionalRequest>() {
            @Override
            public int compare(TransactionalRequest o1, TransactionalRequest o2) {
                return Integer.compare(o1.priority().priority(), o2.priority.priority());
            }
        });
        this.transactionCoordinator = null;
        this.consumerGroupCoordinator = null;
        this.newPartitionsToBeAddedToTransaction = new HashSet<>();
        this.pendingPartitionsToBeAddedToTransaction = new HashSet<>();
        this.partitionsInTransaction = new HashSet<>();
        this.pendingTxnOffsetCommits = new HashMap<>();
    }

    public TransactionManager() {
        this("", 0);
    }

    static class TransactionalRequest {
        private enum Priority {
            FIND_COORDINATOR(0),
            INIT_PRODUCER_ID(1),
            ADD_PARTITIONS_OR_OFFSETS(2),
            END_TXN(4);

            private final int priority;

            Priority(int priority) {
                this.priority = priority;
            }

            public int priority() {
                return this.priority;
            }
        }

        private final AbstractRequest.Builder<?> requestBuilder;

        private final FindCoordinatorRequest.CoordinatorType coordinatorType;
        private final String coordinatorKey;
        private final RequestCompletionHandler handler;
        // We use the priority to determine the order in which requests need to be sent out. For instance, if we have
        // a pending FindCoordinator request, that must always go first. Next, If we need a Pid, that must go second.
        // The endTxn request must always go last.
        private final Priority priority;
        private final TransactionalRequestResult result;
        private boolean isRetry;

        private TransactionalRequest(AbstractRequest.Builder<?> requestBuilder, RequestCompletionHandler handler,
                                     FindCoordinatorRequest.CoordinatorType coordinatorType, Priority priority,
                                     boolean isRetry, String coordinatorKey, TransactionalRequestResult result) {
            this.requestBuilder = requestBuilder;
            this.handler = handler;
            this.coordinatorType = coordinatorType;
            this.priority = priority;
            this.isRetry = isRetry;
            this.coordinatorKey = coordinatorKey;
            this.result = result;
        }

        AbstractRequest.Builder<?> requestBuilder() {
            return requestBuilder;
        }

        boolean needsCoordinator() {
            return coordinatorType != null;
        }

        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return coordinatorType;
        }

        RequestCompletionHandler responseHandler() {
            return handler;
        }

        boolean isRetry() {
            return isRetry;
        }

        boolean isEndTxnRequest() {
            return priority == Priority.END_TXN;
        }

        boolean maybeTerminateWithError(RuntimeException error) {
            if (result != null) {
                result.setError(error);
                result.done();
                return true;
            }
            return false;
        }

        private void setRetry() {
            isRetry = true;
        }

        private Priority priority() {
            return priority;
        }
    }

    static class PidAndEpoch {
        public final long producerId;
        public final short epoch;

        PidAndEpoch(long producerId, short epoch) {
            this.producerId = producerId;
            this.epoch = epoch;
        }

        public boolean isValid() {
            return NO_PRODUCER_ID < producerId;
        }
    }

    public synchronized FutureTransactionalResult initializeTransactions() {
        ensureTransactional();
        transitionTo(State.INITIALIZING);
        setPidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH);
        this.sequenceNumbers.clear();
        if (transactionCoordinator == null)
            pendingTransactionalRequests.add(findCoordinatorRequest(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId, false));

        TransactionalRequestResult result = new TransactionalRequestResult();
        FutureTransactionalResult resultFuture = new FutureTransactionalResult(result);
        if (!hasPid())
            pendingTransactionalRequests.add(initPidRequest(false, result));
        else
            result.done();

        return resultFuture;
    }

    public synchronized void beginTransaction() {
        ensureTransactional();
        maybeFailWithError();
        transitionTo(State.IN_TRANSACTION);
    }

    public synchronized FutureTransactionalResult beginCommittingTransaction() {
        ensureTransactional();
        maybeFailWithError();
        transitionTo(State.COMMITTING_TRANSACTION);
        return beginCompletingTransaction(true);
    }

    public synchronized FutureTransactionalResult beginAbortingTransaction() {
        ensureTransactional();
        if (isFenced())
            throw new ProducerFencedException("There is a newer producer using the same transactional.id.");
        transitionTo(State.ABORTING_TRANSACTION);
        return beginCompletingTransaction(false);
    }

    private FutureTransactionalResult beginCompletingTransaction(boolean isCommit) {
        TransactionalRequestResult result = new TransactionalRequestResult();
        FutureTransactionalResult resultFuture = new FutureTransactionalResult(result);

        if (!newPartitionsToBeAddedToTransaction.isEmpty()) {
            pendingTransactionalRequests.add(addPartitionsToTransactionRequest(false));
        }
        pendingTransactionalRequests.add(endTxnRequest(isCommit, false, result));
        return resultFuture;
    }

    public synchronized FutureTransactionalResult sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                                                           String consumerGroupId) {
        ensureTransactional();
        maybeFailWithError();
        if (currentState != State.IN_TRANSACTION)
            throw new KafkaException("Cannot send offsets to transaction either because the producer is not in an " +
                    "active transaction");

        TransactionalRequestResult result = new TransactionalRequestResult();
        FutureTransactionalResult resultFuture = new FutureTransactionalResult(result);
        pendingTransactionalRequests.add(addOffsetsToTxnRequest(offsets, consumerGroupId, false, result));
        return resultFuture;
    }

    public synchronized void maybeAddPartitionToTransaction(TopicPartition topicPartition) {
        if (!isInTransaction() || partitionsInTransaction.contains(topicPartition))
            return;
        newPartitionsToBeAddedToTransaction.add(topicPartition);
    }

    public Exception lastError() {
        return lastError;
    }

    public String transactionalId() {
        return transactionalId;
    }

    public boolean hasPid() {
        return pidAndEpoch.isValid();
    }

    public boolean isTransactional() {
        return transactionalId != null && !transactionalId.isEmpty();
    }

    public boolean isFenced() {
        return currentState == State.FENCED;
    }

    public boolean isCompletingTransaction() {
        return currentState == State.COMMITTING_TRANSACTION || currentState == State.ABORTING_TRANSACTION;
    }

    public boolean isInTransaction() {
        return currentState == State.IN_TRANSACTION || isCompletingTransaction();
    }

    public boolean isInErrorState() {
        return currentState == State.ERROR;
    }

    public synchronized boolean maybeSetError(Exception exception) {
        if (isTransactional() && isInTransaction()) {
            if (exception instanceof ProducerFencedException)
                transitionTo(State.FENCED, exception);
            else
                transitionTo(State.ERROR, exception);
            return true;
        }
        return false;
    }

    /**
     * Get the current pid and epoch without blocking. Callers must use {@link PidAndEpoch#isValid()} to
     * verify that the result is valid.
     *
     * @return the current PidAndEpoch.
     */
    PidAndEpoch pidAndEpoch() {
        return pidAndEpoch;
    }

    /**
     * Set the pid and epoch atomically. This method will signal any callers blocked on the `pidAndEpoch` method
     * once the pid is set. This method will be called on the background thread when the broker responds with the pid.
     */
    synchronized void setPidAndEpoch(long pid, short epoch) {
        this.pidAndEpoch = new PidAndEpoch(pid, epoch);
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
     * for the same pid. This might involve aborting the ongoing transaction during the initPidRequest, and the user
     * would not have any way of knowing this happened. So for the transactional producer, it's best to return the
     * produce error to the user and let them abort the transaction and close the producer explicitly.
     */
    synchronized void resetProducerId() {
        if (isTransactional())
            throw new IllegalStateException("Cannot reset producer state for a transactional producer. " +
                    "You must either abort the ongoing transaction or reinitialize the transactional producer instead");
        setPidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH);
        this.sequenceNumbers.clear();
    }

    /**
     * Returns the next sequence number to be written to the given TopicPartition.
     */
    synchronized Integer sequenceNumber(TopicPartition topicPartition) {
        Integer currentSequenceNumber = sequenceNumbers.get(topicPartition);
        if (currentSequenceNumber == null) {
            currentSequenceNumber = 0;
            sequenceNumbers.put(topicPartition, currentSequenceNumber);
        }
        return currentSequenceNumber;
    }

    synchronized void incrementSequenceNumber(TopicPartition topicPartition, int increment) {
        Integer currentSequenceNumber = sequenceNumbers.get(topicPartition);
        if (currentSequenceNumber == null)
            throw new IllegalStateException("Attempt to increment sequence number for a partition with no current sequence.");

        currentSequenceNumber += increment;
        sequenceNumbers.put(topicPartition, currentSequenceNumber);
    }

    boolean hasPendingTransactionalRequests() {
        return !(pendingTransactionalRequests.isEmpty()
                && newPartitionsToBeAddedToTransaction.isEmpty());
    }

    TransactionalRequest nextTransactionalRequest() {
        if (!hasPendingTransactionalRequests())
            return null;

        if (!newPartitionsToBeAddedToTransaction.isEmpty())
            pendingTransactionalRequests.add(addPartitionsToTransactionRequest(false));

        return pendingTransactionalRequests.poll();
    }

    void needsRetry(TransactionalRequest request) {
        request.setRetry();
        pendingTransactionalRequests.add(request);
    }

    void reenqueue(TransactionalRequest request) {
        pendingTransactionalRequests.add(request);
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

    void needsCoordinator(TransactionalRequest request) {
        needsCoordinator(request.coordinatorType, request.coordinatorKey);
    }

    void setInFlightRequestCorrelationId(int correlationId) {
        inFlightRequestCorrelationId = correlationId;
    }

    void resetInFlightRequestCorrelationId() {
        inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    boolean hasInflightTransactionalRequest() {
        return inFlightRequestCorrelationId != NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    // visible for testing
    boolean transactionContainsPartition(TopicPartition topicPartition) {
        return isInTransaction() && partitionsInTransaction.contains(topicPartition);
    }

    // visible for testing
    boolean hasPendingOffsetCommits() {
        return isInTransaction() && !pendingTxnOffsetCommits.isEmpty();
    }

    // visible for testing
    boolean isReadyForTransaction() {
        return isTransactional() && currentState == State.READY;
    }

    private void transitionTo(State target) {
        transitionTo(target, null);
    }

    private void transitionTo(State target, Exception error) {
        if (target == State.ERROR && error != null)
            lastError = error;
        if (currentState.isTransitionValid(currentState, target)) {
            currentState = target;
        } else {
            throw new KafkaException("Invalid transition attempted from state " + currentState.name() + " to state " + target.name());
        }
    }

    private void ensureTransactional() {
        if (!isTransactional())
            throw new IllegalStateException("Transactional method invoked on a non-transactional producer.");
    }

    private void maybeFailWithError() {
        if (isFenced())
            throw new ProducerFencedException("There is a newer producer instance using the same transactional id.");
        if (isInErrorState()) {
            String errorMessage = "Cannot execute transactional method because we are in an error state.";
            if (lastError != null)
                throw new KafkaException(errorMessage, lastError);
            else
                throw new KafkaException(errorMessage);
        }
    }

    private void needsCoordinator(FindCoordinatorRequest.CoordinatorType type, String coordinatorKey) {
        switch (type) {
            case GROUP:
                consumerGroupCoordinator = null;
                break;
            case TRANSACTION:
                transactionCoordinator = null;
                break;
            default:
                throw new IllegalStateException("Got an invalid coordinator type: " + type);
        }
        pendingTransactionalRequests.add(findCoordinatorRequest(type, coordinatorKey, false));
    }


    private void completeTransaction() {
        transitionTo(State.READY);
        lastError = null;
        partitionsInTransaction.clear();
    }

    private TransactionalRequest initPidRequest(boolean isRetry, TransactionalRequestResult result) {
        InitPidRequest.Builder builder = new InitPidRequest.Builder(transactionalId, transactionTimeoutMs);
        return new TransactionalRequest(builder, new InitPidCallback(result),
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, TransactionalRequest.Priority.INIT_PRODUCER_ID, isRetry, transactionalId, result);
    }

    private synchronized TransactionalRequest addPartitionsToTransactionRequest(boolean isRetry) {
        pendingPartitionsToBeAddedToTransaction.addAll(newPartitionsToBeAddedToTransaction);
        newPartitionsToBeAddedToTransaction.clear();
        AddPartitionsToTxnRequest.Builder builder = new AddPartitionsToTxnRequest.Builder(transactionalId,
                pidAndEpoch.producerId, pidAndEpoch.epoch, new ArrayList<>(pendingPartitionsToBeAddedToTransaction));
        return new TransactionalRequest(builder, new AddPartitionsToTransactionCallback(),
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, TransactionalRequest.Priority.ADD_PARTITIONS_OR_OFFSETS, isRetry, transactionalId, null);
    }

    private TransactionalRequest findCoordinatorRequest(FindCoordinatorRequest.CoordinatorType type, String coordinatorKey, boolean isRetry) {
        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(type, coordinatorKey);
        return new TransactionalRequest(builder, new FindCoordinatorCallback(type, coordinatorKey),
                null, TransactionalRequest.Priority.FIND_COORDINATOR, isRetry, null, null);
    }

    private TransactionalRequest endTxnRequest(boolean isCommit, boolean isRetry, TransactionalRequestResult result) {
        EndTxnRequest.Builder builder = new EndTxnRequest.Builder(transactionalId,
                pidAndEpoch.producerId, pidAndEpoch.epoch, isCommit ? TransactionResult.COMMIT : TransactionResult.ABORT);
        return new TransactionalRequest(builder, new EndTxnCallback(isCommit, result),
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, TransactionalRequest.Priority.END_TXN, isRetry, transactionalId, result);
    }

    private TransactionalRequest addOffsetsToTxnRequest(Map<TopicPartition, OffsetAndMetadata> offsets,
                                                        String consumerGroupId, boolean isRetry, TransactionalRequestResult result) {
        AddOffsetsToTxnRequest.Builder builder = new AddOffsetsToTxnRequest.Builder(transactionalId,
                pidAndEpoch.producerId, pidAndEpoch.epoch, consumerGroupId);
        return new TransactionalRequest(builder, new AddOffsetsToTxnCallback(offsets, consumerGroupId, result),
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, TransactionalRequest.Priority.ADD_PARTITIONS_OR_OFFSETS, isRetry, transactionalId, result);
    }

    private TransactionalRequest txnOffsetCommitRequest(Map<TopicPartition, OffsetAndMetadata> offsets,
                                                        String consumerGroupId, boolean isRetry, TransactionalRequestResult result) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            pendingTxnOffsetCommits.put(entry.getKey(),
                    new TxnOffsetCommitRequest.CommittedOffset(offsetAndMetadata.offset(), offsetAndMetadata.metadata()));
        }
        return txnOffsetCommitRequest(consumerGroupId, isRetry, result);
    }

    private TransactionalRequest txnOffsetCommitRequest(String consumerGroupId, boolean isRetry, TransactionalRequestResult result) {
        TxnOffsetCommitRequest.Builder builder = new TxnOffsetCommitRequest.Builder(consumerGroupId,
                pidAndEpoch.producerId, pidAndEpoch.epoch, OffsetCommitRequest.DEFAULT_RETENTION_TIME, pendingTxnOffsetCommits);
        return new TransactionalRequest(builder, new TxnOffsetCommitCallback(consumerGroupId, result),
                FindCoordinatorRequest.CoordinatorType.GROUP, TransactionalRequest.Priority.ADD_PARTITIONS_OR_OFFSETS, isRetry, consumerGroupId, result);
    }

    private abstract class TransactionalRequestCallBack implements RequestCompletionHandler {
        protected final TransactionalRequestResult result;

        TransactionalRequestCallBack(TransactionalRequestResult result) {
            this.result = result;
        }

        @Override
        public void onComplete(ClientResponse response) {
            if (response.requestHeader().correlationId() != inFlightRequestCorrelationId) {
                log.error("Detected more than one inflight transactional request. This should never happen.");
                transitionTo(State.ERROR, new RuntimeException("Detected more than one inflight transactional request. This should never happen."));
                return;
            }

            resetInFlightRequestCorrelationId();
            if (response.wasDisconnected()) {
                reenqueue();
            } else if (response.versionMismatch() != null) {
                if (result != null) {
                    result.setError(Errors.UNSUPPORTED_VERSION.exception());
                    result.done();
                }
                log.error("Could not execute transactional request because the broker isn't on the right version.");
                transitionTo(State.ERROR, Errors.UNSUPPORTED_VERSION.exception());
            } else if (response.hasResponse()) {
                handleResponse(response.responseBody());
            } else {
                if (result != null) {
                    result.setError(Errors.UNKNOWN.exception());
                    result.done();
                }
                log.error("Could not execute transactional request for unknown reasons");
                transitionTo(State.ERROR, Errors.UNKNOWN.exception());
            }
        }

        public abstract void handleResponse(AbstractResponse responseBody);

        public abstract void reenqueue();
    }

    private class InitPidCallback extends TransactionalRequestCallBack {

        InitPidCallback(TransactionalRequestResult result) {
           super(result);
        }

        @Override
        public void handleResponse(AbstractResponse responseBody) {
            InitPidResponse initPidResponse = (InitPidResponse) responseBody;
            Errors error = initPidResponse.error();
            if (error == Errors.NONE) {
                setPidAndEpoch(initPidResponse.producerId(), initPidResponse.epoch());
                transitionTo(State.READY);
                lastError = null;
            } else if (error == Errors.NOT_COORDINATOR || error == Errors.COORDINATOR_NOT_AVAILABLE) {
                needsCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
                reenqueue();
            } else {
                result.setError(error.exception());
                transitionTo(State.ERROR, error.exception());
            }

            if (error == Errors.NONE || !result.isSuccessful())
                result.done();
        }

        @Override
        public void reenqueue() {
            pendingTransactionalRequests.add(initPidRequest(true, result));
        }
    }

    private class AddPartitionsToTransactionCallback extends TransactionalRequestCallBack {

        AddPartitionsToTransactionCallback() {
            super(null);
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            AddPartitionsToTxnResponse addPartitionsToTxnResponse = (AddPartitionsToTxnResponse) response;
            Errors error = addPartitionsToTxnResponse.error();
            if (error == Errors.NONE) {
                partitionsInTransaction.addAll(pendingPartitionsToBeAddedToTransaction);
                pendingPartitionsToBeAddedToTransaction.clear();
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                needsCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
                reenqueue();
            } else if (error == Errors.INVALID_PID_MAPPING || error == Errors.INVALID_TXN_STATE) {
                log.error("Seems like the broker has bad transaction state. producerId: {}, error: {}. message: {}",
                        pidAndEpoch.producerId, error, error.message());
                transitionTo(State.ERROR, error.exception());
            } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                transitionTo(State.FENCED, error.exception());
                log.error("Epoch has become invalid: producerId: {}. epoch: {}. Message: {}", pidAndEpoch.producerId, pidAndEpoch.epoch, error.message());
            } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                transitionTo(State.ERROR, error.exception());
                log.error("No permissions add some partitions to the transaction: {}", error.message());
            } else {
                transitionTo(State.ERROR, error.exception());
                log.error("Could not add partitions to transaction due to unknown error: {}", error.message());
            }
        }

        @Override
        public void reenqueue() {
            pendingTransactionalRequests.add(addPartitionsToTransactionRequest(true));
        }
    }

    private class FindCoordinatorCallback extends TransactionalRequestCallBack {
        private final FindCoordinatorRequest.CoordinatorType type;
        private final String coordinatorKey;

        FindCoordinatorCallback(FindCoordinatorRequest.CoordinatorType type, String coordinatorKey) {
            super(null);
            this.type = type;
            this.coordinatorKey = coordinatorKey;
        }
        @Override
        public void handleResponse(AbstractResponse responseBody) {
            FindCoordinatorResponse findCoordinatorResponse = (FindCoordinatorResponse) responseBody;
            if (findCoordinatorResponse.error() == Errors.NONE) {
                Node node = findCoordinatorResponse.node();
                switch (type) {
                    case GROUP:
                        consumerGroupCoordinator = node;
                        break;
                    case TRANSACTION:
                        transactionCoordinator = node;
                }
            } else if (findCoordinatorResponse.error() == Errors.COORDINATOR_NOT_AVAILABLE) {
                reenqueue();
            } else if (findCoordinatorResponse.error() == Errors.GROUP_AUTHORIZATION_FAILED) {
                transitionTo(State.ERROR, findCoordinatorResponse.error().exception());
                log.error("Not authorized to access the group with type {} and key {}. Message: {} ", type,
                        coordinatorKey, findCoordinatorResponse.error().message());
            } else {
                transitionTo(State.ERROR, findCoordinatorResponse.error().exception());
                log.error("Could not find a coordinator with type {} for unknown reasons. coordinatorKey: {}", type,
                        coordinatorKey, findCoordinatorResponse.error().message());
            }
        }

        @Override
        public void reenqueue() {
            pendingTransactionalRequests.add(findCoordinatorRequest(type, coordinatorKey, true));
        }
    }

    private class EndTxnCallback extends TransactionalRequestCallBack {
        private final boolean isCommit;

        EndTxnCallback(boolean isCommit, TransactionalRequestResult result) {
            super(result);
            this.isCommit = isCommit;
        }

        @Override
        public void handleResponse(AbstractResponse responseBody) {
            EndTxnResponse endTxnResponse = (EndTxnResponse) responseBody;
            Errors error = endTxnResponse.error();
            if (error == Errors.NONE) {
                completeTransaction();
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                needsCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
                reenqueue();
            } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                transitionTo(State.FENCED, error.exception());
                result.setError(error.exception());
            } else {
                result.setError(error.exception());
                transitionTo(State.ERROR, error.exception());
            }

            if (error == Errors.NONE || !result.isSuccessful())
                result.done();
        }

        @Override
        public void reenqueue() {
            pendingTransactionalRequests.add(endTxnRequest(isCommit, true, result));
        }
    }

    private class AddOffsetsToTxnCallback extends TransactionalRequestCallBack {
        String consumerGroupId;
        Map<TopicPartition, OffsetAndMetadata> offsets;

        AddOffsetsToTxnCallback(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId, TransactionalRequestResult result) {
            super(result);
            this.offsets = offsets;
            this.consumerGroupId = consumerGroupId;
        }

        @Override
        public void handleResponse(AbstractResponse responseBody) {
            AddOffsetsToTxnResponse addOffsetsToTxnResponse = (AddOffsetsToTxnResponse) responseBody;
            Errors error = addOffsetsToTxnResponse.error();
            if (error == Errors.NONE) {
                pendingTransactionalRequests.add(txnOffsetCommitRequest(offsets, consumerGroupId, false, result));
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                needsCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
                reenqueue();
            } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                transitionTo(State.FENCED, error.exception());
                result.setError(error.exception());
            } else {
                transitionTo(State.ERROR, error.exception());
                result.setError(error.exception());
            }

            if (!result.isSuccessful())
                result.done();
        }

        @Override
        public void reenqueue() {
            pendingTransactionalRequests.add(addOffsetsToTxnRequest(offsets, consumerGroupId, true, result));
        }
    }

    private class TxnOffsetCommitCallback extends TransactionalRequestCallBack {
        private final String consumerGroupId;

        TxnOffsetCommitCallback(String consumerGroupId, TransactionalRequestResult result) {
            super(result);
            this.consumerGroupId = consumerGroupId;
        }

        @Override
        public void handleResponse(AbstractResponse responseBody) {
            TxnOffsetCommitResponse txnOffsetCommitResponse = (TxnOffsetCommitResponse) responseBody;
            boolean coordinatorReloaded = false;
            boolean hadFailure = false;
            for (Map.Entry<TopicPartition, Errors> entry : txnOffsetCommitResponse.errors().entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                Errors error = entry.getValue();
                if (error == Errors.NONE) {
                    pendingTxnOffsetCommits.remove(topicPartition);
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                    hadFailure = true;
                    if (!coordinatorReloaded) {
                        coordinatorReloaded = true;
                        needsCoordinator(FindCoordinatorRequest.CoordinatorType.GROUP, consumerGroupId);
                    }
                } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                    transitionTo(State.FENCED, error.exception());
                    result.setError(error.exception());
                    break;
                } else {
                    result.setError(error.exception());
                    transitionTo(State.ERROR, error.exception());
                    break;
                }
            }

            if (!hadFailure || !result.isSuccessful()) {
                // all attempted partitions were either successful, or there was a fatal failure.
                // either way, we are not retrying, so complete the request.
                result.done();
                return;
            }

            // retry the commits which failed with a retriable error.
            if (!pendingTxnOffsetCommits.isEmpty())
                pendingTransactionalRequests.add(txnOffsetCommitRequest(consumerGroupId, true, result));

        }

        @Override
        public void reenqueue() {
            pendingTransactionalRequests.add(txnOffsetCommitRequest(consumerGroupId, true, result));
        }
    }
}
