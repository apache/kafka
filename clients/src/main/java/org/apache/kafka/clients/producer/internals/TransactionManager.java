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
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
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
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest.CommittedOffset;
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

    private final String transactionalId;
    private final int transactionTimeoutMs;
    public final String logPrefix;

    private final Map<TopicPartition, Integer> sequenceNumbers;
    private final PriorityQueue<TxnRequestHandler> pendingRequests;
    private final Set<TopicPartition> newPartitionsInTransaction;
    private final Set<TopicPartition> pendingPartitionsInTransaction;
    private final Set<TopicPartition> partitionsInTransaction;
    private final Map<TopicPartition, CommittedOffset> pendingTxnOffsetCommits;

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
                    return source == INITIALIZING || source == COMMITTING_TRANSACTION
                            || source == ABORTING_TRANSACTION || source == ABORTABLE_ERROR;
                case IN_TRANSACTION:
                    return source == READY;
                case COMMITTING_TRANSACTION:
                    return source == IN_TRANSACTION;
                case ABORTING_TRANSACTION:
                    return source == IN_TRANSACTION || source == ABORTABLE_ERROR;
                case ABORTABLE_ERROR:
                    return source == IN_TRANSACTION || source == COMMITTING_TRANSACTION || source == ABORTING_TRANSACTION;
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

    public TransactionManager(String transactionalId, int transactionTimeoutMs) {
        this.producerIdAndEpoch = new ProducerIdAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH);
        this.sequenceNumbers = new HashMap<>();
        this.transactionalId = transactionalId;
        this.logPrefix = transactionalId == null ? "" : "[TransactionalId " + transactionalId + "] ";
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.transactionCoordinator = null;
        this.consumerGroupCoordinator = null;
        this.newPartitionsInTransaction = new HashSet<>();
        this.pendingPartitionsInTransaction = new HashSet<>();
        this.partitionsInTransaction = new HashSet<>();
        this.pendingTxnOffsetCommits = new HashMap<>();
        this.pendingRequests = new PriorityQueue<>(10, new Comparator<TxnRequestHandler>() {
            @Override
            public int compare(TxnRequestHandler o1, TxnRequestHandler o2) {
                return Integer.compare(o1.priority().priority, o2.priority().priority);
            }
        });
    }

    TransactionManager() {
        this(null, 0);
    }

    public synchronized TransactionalRequestResult initializeTransactions() {
        ensureTransactional();
        transitionTo(State.INITIALIZING);
        setProducerIdAndEpoch(ProducerIdAndEpoch.NONE);
        this.sequenceNumbers.clear();
        InitProducerIdRequest.Builder builder = new InitProducerIdRequest.Builder(transactionalId, transactionTimeoutMs);
        InitProducerIdHandler handler = new InitProducerIdHandler(builder);
        enqueueRequest(handler);
        return handler.result;
    }

    public synchronized void beginTransaction() {
        ensureTransactional();
        maybeFailWithError();
        transitionTo(State.IN_TRANSACTION);
    }

    public synchronized TransactionalRequestResult beginCommittingTransaction() {
        ensureTransactional();
        maybeFailWithError();
        transitionTo(State.COMMITTING_TRANSACTION);
        return beginCompletingTransaction(true);
    }

    public synchronized TransactionalRequestResult beginAbortingTransaction() {
        ensureTransactional();
        if (currentState != State.ABORTABLE_ERROR)
            maybeFailWithError();
        transitionTo(State.ABORTING_TRANSACTION);

        // We're aborting the transaction, so there should be no need to add new partitions
        newPartitionsInTransaction.clear();
        return beginCompletingTransaction(false);
    }

    private TransactionalRequestResult beginCompletingTransaction(boolean isCommit) {
        if (!newPartitionsInTransaction.isEmpty())
            enqueueRequest(addPartitionsToTransactionHandler());

        TransactionResult transactionResult = isCommit ? TransactionResult.COMMIT : TransactionResult.ABORT;
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

        log.debug("{}Begin adding offsets {} for consumer group {} to transaction", logPrefix, offsets, consumerGroupId);
        AddOffsetsToTxnRequest.Builder builder = new AddOffsetsToTxnRequest.Builder(transactionalId,
                producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, consumerGroupId);
        AddOffsetsToTxnHandler handler = new AddOffsetsToTxnHandler(builder, offsets);
        enqueueRequest(handler);
        return handler.result;
    }

    public synchronized void maybeAddPartitionToTransaction(TopicPartition topicPartition) {
        if (currentState != State.IN_TRANSACTION)
            throw new IllegalStateException("Cannot add partitions to a transaction in state " + currentState);

        if (partitionsInTransaction.contains(topicPartition) || pendingPartitionsInTransaction.contains(topicPartition))
            return;

        log.debug("{}Begin adding new partition {} to transaction", logPrefix, topicPartition);
        newPartitionsInTransaction.add(topicPartition);
    }

    RuntimeException lastError() {
        return lastError;
    }

    public synchronized void failIfUnreadyForSend() {
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

    synchronized boolean sendToPartitionAllowed(TopicPartition tp) {
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

    synchronized boolean isCompletingTransaction() {
        return currentState == State.COMMITTING_TRANSACTION || currentState == State.ABORTING_TRANSACTION;
    }

    synchronized boolean hasError() {
        return currentState == State.ABORTABLE_ERROR || currentState == State.FATAL_ERROR;
    }

    synchronized boolean isAborting() {
        return currentState == State.ABORTING_TRANSACTION;
    }

    synchronized void transitionToAbortableError(RuntimeException exception) {
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
        log.info("{}ProducerId set to {} with epoch {}", logPrefix, producerIdAndEpoch.producerId,
                producerIdAndEpoch.epoch);
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

    synchronized TxnRequestHandler nextRequestHandler() {
        if (!newPartitionsInTransaction.isEmpty())
            enqueueRequest(addPartitionsToTransactionHandler());

        TxnRequestHandler nextRequestHandler = pendingRequests.poll();
        if (nextRequestHandler != null && maybeTerminateRequestWithError(nextRequestHandler)) {
            log.trace("{}Not sending transactional request {} because we are in an error state",
                    logPrefix, nextRequestHandler.requestBuilder());
            return null;
        }

        if (nextRequestHandler != null && nextRequestHandler.isEndTxn() && !transactionStarted) {
            nextRequestHandler.result.done();
            if (currentState != State.FATAL_ERROR) {
                log.debug("{}Not sending EndTxn for completed transaction since no partitions " +
                        "or offsets were successfully added", logPrefix);
                completeTransaction();
            }
            nextRequestHandler = pendingRequests.poll();
        }


        if (nextRequestHandler != null)
            log.trace("{}Request {} dequeued for sending", logPrefix, nextRequestHandler.requestBuilder());

        return nextRequestHandler;
    }

    synchronized void retry(TxnRequestHandler request) {
        request.setRetry();
        enqueueRequest(request);
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

    void setInFlightRequestCorrelationId(int correlationId) {
        inFlightRequestCorrelationId = correlationId;
    }

    void clearInFlightRequestCorrelationId() {
        inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    boolean hasInflightRequest() {
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

    // visible for testing
    synchronized boolean hasOngoingTransaction() {
        // transactions are considered ongoing once started until completion or a fatal error
        return currentState == State.IN_TRANSACTION || isCompletingTransaction() || hasAbortableError();
    }

    // visible for testing
    synchronized boolean isReady() {
        return isTransactional() && currentState == State.READY;
    }

    private void transitionTo(State target) {
        transitionTo(target, null);
    }

    private synchronized void transitionTo(State target, RuntimeException error) {
        if (!currentState.isTransitionValid(currentState, target))
            throw new KafkaException("Invalid transition attempted from state " + currentState.name() +
                    " to state " + target.name());

        if (target == State.FATAL_ERROR || target == State.ABORTABLE_ERROR) {
            if (error == null)
                throw new IllegalArgumentException("Cannot transition to " + target + " with an null exception");
            lastError = error;
        } else {
            lastError = null;
        }

        if (lastError != null)
            log.debug("{}Transition from state {} to error state {}", logPrefix, currentState, target, lastError);
        else
            log.debug("{}Transition from state {} to {}", logPrefix, currentState, target);

        currentState = target;
    }

    private void ensureTransactional() {
        if (!isTransactional())
            throw new IllegalStateException("Transactional method invoked on a non-transactional producer.");
    }

    private void maybeFailWithError() {
        if (hasError())
            throw new KafkaException("Cannot execute transactional method because we are in an error state", lastError);
    }

    private boolean maybeTerminateRequestWithError(TxnRequestHandler requestHandler) {
        if (hasError()) {
            if (requestHandler instanceof EndTxnHandler) {
                // we allow abort requests to break out of the error state. The state and the last error
                // will be cleared when the request returns
                EndTxnHandler endTxnHandler = (EndTxnHandler) requestHandler;
                if (endTxnHandler.builder.result() == TransactionResult.ABORT)
                    return false;
            }
            requestHandler.fail(lastError);
            return true;
        }
        return false;
    }

    private void enqueueRequest(TxnRequestHandler requestHandler) {
        log.debug("{}Enqueuing transactional request {}", logPrefix, requestHandler.requestBuilder());
        pendingRequests.add(requestHandler);
    }

    private synchronized void lookupCoordinator(FindCoordinatorRequest.CoordinatorType type, String coordinatorKey) {
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

        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(type, coordinatorKey);
        enqueueRequest(new FindCoordinatorHandler(builder));
    }

    private synchronized void completeTransaction() {
        transitionTo(State.READY);
        lastError = null;
        transactionStarted = false;
        partitionsInTransaction.clear();
    }

    private synchronized TxnRequestHandler addPartitionsToTransactionHandler() {
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
            CommittedOffset committedOffset = new CommittedOffset(offsetAndMetadata.offset(), offsetAndMetadata.metadata());
            pendingTxnOffsetCommits.put(entry.getKey(), committedOffset);
        }
        TxnOffsetCommitRequest.Builder builder = new TxnOffsetCommitRequest.Builder(transactionalId, consumerGroupId,
                producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, pendingTxnOffsetCommits);
        return new TxnOffsetCommitHandler(result, builder);
    }

    abstract class TxnRequestHandler implements RequestCompletionHandler {
        protected final TransactionalRequestResult result;
        private boolean isRetry = false;

        TxnRequestHandler(TransactionalRequestResult result) {
            this.result = result;
        }

        TxnRequestHandler() {
            this(new TransactionalRequestResult());
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

        @Override
        @SuppressWarnings("unchecked")
        public void onComplete(ClientResponse response) {
            if (response.requestHeader().correlationId() != inFlightRequestCorrelationId) {
                fatalError(new RuntimeException("Detected more than one in-flight transactional request."));
            } else {
                clearInFlightRequestCorrelationId();
                if (response.wasDisconnected()) {
                    log.trace("{}Disconnected from {}. Will retry.", logPrefix, response.destination());
                    reenqueue();
                } else if (response.versionMismatch() != null) {
                    fatalError(response.versionMismatch());
                } else if (response.hasResponse()) {
                    log.trace("{}Received transactional response {} for request {}", logPrefix,
                            response.responseBody(), requestBuilder());
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
                ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(initProducerIdResponse.producerId(), initProducerIdResponse.epoch());
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

        private AddPartitionsToTxnHandler(AddPartitionsToTxnRequest.Builder builder) {
            this.builder = builder;
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

            for (Map.Entry<TopicPartition, Errors> topicPartitionErrorEntry : errors.entrySet()) {
                TopicPartition topicPartition = topicPartitionErrorEntry.getKey();
                Errors error = topicPartitionErrorEntry.getValue();

                if (error == Errors.NONE) {
                    continue;
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                    lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                    reenqueue();
                    return;
                } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS
                        || error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
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
                    log.debug("{}Did not attempt to add partition {} to transaction because other partitions in the batch had errors.", logPrefix, topicPartition);
                    hasPartitionErrors = true;
                } else {
                    log.error("{}Could not add partition {} due to unexpected error {}", logPrefix, topicPartition, error);
                    hasPartitionErrors = true;
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                abortableError(new TopicAuthorizationException(unauthorizedTopics));
            } else if (hasPartitionErrors) {
                abortableError(new KafkaException("Could not add partitions to transaction due to partition level errors"));
            } else {
                Set<TopicPartition> addedPartitions = errors.keySet();
                log.debug("{}Successfully added partitions {} to transaction", logPrefix, addedPartitions);
                partitionsInTransaction.addAll(addedPartitions);
                pendingPartitionsInTransaction.removeAll(addedPartitions);
                transactionStarted = true;
                result.done();
            }
        }
    }

    private class FindCoordinatorHandler extends TxnRequestHandler {
        private final FindCoordinatorRequest.Builder builder;

        private FindCoordinatorHandler(FindCoordinatorRequest.Builder builder) {
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

            if (error == Errors.NONE) {
                Node node = findCoordinatorResponse.node();
                switch (builder.coordinatorType()) {
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
                abortableError(new GroupAuthorizationException(builder.coordinatorKey()));
            } else {
                fatalError(new KafkaException(String.format("Could not find a coordinator with type %s with key %s due to" +
                        "unexpected error: %s", builder.coordinatorType(), builder.coordinatorKey(),
                        findCoordinatorResponse.error().message())));
            }
        }
    }

    private class EndTxnHandler extends TxnRequestHandler {
        private final EndTxnRequest.Builder builder;

        private EndTxnHandler(EndTxnRequest.Builder builder) {
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
                log.debug("{}Successfully added partition for consumer group {} to transaction", logPrefix,
                        builder.consumerGroupId());

                // note the result is not completed until the TxnOffsetCommit returns
                pendingRequests.add(txnOffsetCommitHandler(result, offsets, builder.consumerGroupId()));
                transactionStarted = true;
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
                reenqueue();
            } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                fatalError(error.exception());
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatalError(error.exception());
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                abortableError(new GroupAuthorizationException(builder.consumerGroupId()));
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
            boolean hadFailure = false;
            Map<TopicPartition, Errors> errors = txnOffsetCommitResponse.errors();

            for (Map.Entry<TopicPartition, Errors> entry : errors.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                Errors error = entry.getValue();
                if (error == Errors.NONE) {
                    log.debug("{}Successfully added offsets {} from consumer group {} to transaction.", logPrefix,
                            builder.offsets(), builder.consumerGroupId());
                    pendingTxnOffsetCommits.remove(topicPartition);
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR
                        || error == Errors.REQUEST_TIMED_OUT) {
                    hadFailure = true;
                    if (!coordinatorReloaded) {
                        coordinatorReloaded = true;
                        lookupCoordinator(FindCoordinatorRequest.CoordinatorType.GROUP, builder.consumerGroupId());
                    }
                } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                    hadFailure = true;
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    abortableError(new GroupAuthorizationException(builder.consumerGroupId()));
                    return;
                } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                    fatalError(error.exception());
                    return;
                } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                    fatalError(error.exception());
                    return;
                } else {
                    fatalError(new KafkaException("Unexpected error in TxnOffsetCommitResponse: " + error.message()));
                    return;
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
                reenqueue();
        }
    }

}
