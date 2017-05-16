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

    private final Map<TopicPartition, Integer> sequenceNumbers;
    private final PriorityQueue<TxnRequestHandler> pendingRequests;
    private final Set<TopicPartition> newPartitionsToBeAddedToTransaction;
    private final Set<TopicPartition> pendingPartitionsToBeAddedToTransaction;
    private final Set<TopicPartition> partitionsInTransaction;
    private final Map<TopicPartition, CommittedOffset> pendingTxnOffsetCommits;

    private int inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID;
    private Node transactionCoordinator;
    private Node consumerGroupCoordinator;

    private volatile State currentState = State.UNINITIALIZED;
    private volatile Exception lastError = null;
    private volatile ProducerIdAndEpoch producerIdAndEpoch;

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
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.transactionCoordinator = null;
        this.consumerGroupCoordinator = null;
        this.newPartitionsToBeAddedToTransaction = new HashSet<>();
        this.pendingPartitionsToBeAddedToTransaction = new HashSet<>();
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
        this("", 0);
    }

    public synchronized TransactionalRequestResult initializeTransactions() {
        ensureTransactional();
        transitionTo(State.INITIALIZING);
        setProducerIdAndEpoch(ProducerIdAndEpoch.NONE);
        this.sequenceNumbers.clear();
        InitProducerIdRequest.Builder builder = new InitProducerIdRequest.Builder(transactionalId, transactionTimeoutMs);
        InitProducerIdHandler handler = new InitProducerIdHandler(builder);
        pendingRequests.add(handler);
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
        if (isFenced())
            throw new ProducerFencedException("There is a newer producer using the same transactional.id.");
        transitionTo(State.ABORTING_TRANSACTION);
        return beginCompletingTransaction(false);
    }

    private TransactionalRequestResult beginCompletingTransaction(boolean isCommit) {
        if (!newPartitionsToBeAddedToTransaction.isEmpty()) {
            pendingRequests.add(addPartitionsToTransactionHandler());
        }

        TransactionResult transactionResult = isCommit ? TransactionResult.COMMIT : TransactionResult.ABORT;
        EndTxnRequest.Builder builder = new EndTxnRequest.Builder(transactionalId, producerIdAndEpoch.producerId,
                producerIdAndEpoch.epoch, transactionResult);
        EndTxnHandler handler = new EndTxnHandler(builder);
        pendingRequests.add(handler);
        return handler.result;
    }

    public synchronized TransactionalRequestResult sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                                                           String consumerGroupId) {
        ensureTransactional();
        maybeFailWithError();
        if (currentState != State.IN_TRANSACTION)
            throw new KafkaException("Cannot send offsets to transaction either because the producer is not in an " +
                    "active transaction");

        AddOffsetsToTxnRequest.Builder builder = new AddOffsetsToTxnRequest.Builder(transactionalId,
                producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, consumerGroupId);
        AddOffsetsToTxnHandler handler = new AddOffsetsToTxnHandler(builder, offsets);
        pendingRequests.add(handler);
        return handler.result;
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

    public boolean hasProducerId() {
        return producerIdAndEpoch.isValid();
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

    public synchronized void setError(Exception exception) {
        if (exception instanceof ProducerFencedException)
            transitionTo(State.FENCED, exception);
        else
            transitionTo(State.ERROR, exception);
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

    /**
     * Set the producer id and epoch atomically.
     */
    void setProducerIdAndEpoch(ProducerIdAndEpoch producerIdAndEpoch) {
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

    boolean hasPendingTransactionalRequests() {
        return !(pendingRequests.isEmpty() && newPartitionsToBeAddedToTransaction.isEmpty());
    }

    TxnRequestHandler nextRequestHandler() {
        if (!hasPendingTransactionalRequests())
            return null;

        if (!newPartitionsToBeAddedToTransaction.isEmpty())
            pendingRequests.add(addPartitionsToTransactionHandler());

        return pendingRequests.poll();
    }

    void retry(TxnRequestHandler request) {
        request.setRetry();
        pendingRequests.add(request);
    }

    void reenqueue(TxnRequestHandler request) {
        pendingRequests.add(request);
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

    private synchronized void transitionTo(State target, Exception error) {
        if (target == State.ERROR && error != null)
            lastError = error;
        if (currentState.isTransitionValid(currentState, target)) {
            currentState = target;
        } else {
            throw new KafkaException("Invalid transition attempted from state " + currentState.name() +
                    " to state " + target.name());
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

    private void lookupCoordinator(FindCoordinatorRequest.CoordinatorType type, String coordinatorKey) {
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

        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(type, coordinatorKey);
        FindCoordinatorHandler request = new FindCoordinatorHandler(builder);
        pendingRequests.add(request);
    }

    private void completeTransaction() {
        transitionTo(State.READY);
        lastError = null;
        partitionsInTransaction.clear();
    }

    private synchronized TxnRequestHandler addPartitionsToTransactionHandler() {
        pendingPartitionsToBeAddedToTransaction.addAll(newPartitionsToBeAddedToTransaction);
        newPartitionsToBeAddedToTransaction.clear();
        AddPartitionsToTxnRequest.Builder builder = new AddPartitionsToTxnRequest.Builder(transactionalId,
                producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, new ArrayList<>(pendingPartitionsToBeAddedToTransaction));
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
        TxnOffsetCommitRequest.Builder builder = new TxnOffsetCommitRequest.Builder(consumerGroupId,
                producerIdAndEpoch.producerId, producerIdAndEpoch.epoch,
                pendingTxnOffsetCommits);
        return new TxnOffsetCommitHandler(result, builder);
    }

    abstract class TxnRequestHandler implements  RequestCompletionHandler {
        protected final TransactionalRequestResult result;
        private boolean isRetry = false;

        TxnRequestHandler(TransactionalRequestResult result) {
            this.result = result;
        }

        TxnRequestHandler() {
            this(new TransactionalRequestResult());
        }

        void fatal(RuntimeException e) {
            result.setError(e);
            transitionTo(State.ERROR, e);
            result.done();
        }

        void fenced() {
            log.error("Producer has become invalid, which typically means another producer with the same " +
                            "transactional.id has been started: producerId: {}. epoch: {}.",
                    producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
            result.setError(Errors.INVALID_PRODUCER_EPOCH.exception());
            transitionTo(State.FENCED, Errors.INVALID_PRODUCER_EPOCH.exception());
            result.done();
        }

        void reenqueue() {
            this.isRetry = true;
            pendingRequests.add(this);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onComplete(ClientResponse response) {
            if (response.requestHeader().correlationId() != inFlightRequestCorrelationId) {
                fatal(new RuntimeException("Detected more than one in-flight transactional request."));
            } else {
                clearInFlightRequestCorrelationId();
                if (response.wasDisconnected()) {
                    reenqueue();
                } else if (response.versionMismatch() != null) {
                    fatal(response.versionMismatch());
                } else if (response.hasResponse()) {
                    handleResponse(response.responseBody());
                } else {
                    fatal(new KafkaException("Could not execute transactional request for unknown reasons"));
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
                fatal(error.exception());
            } else {
                fatal(new KafkaException("Unexpected error in InitProducerIdResponse; " + error.message()));
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
            for (TopicPartition topicPartition : pendingPartitionsToBeAddedToTransaction) {
                final Errors error = errors.get(topicPartition);
                if (error == Errors.NONE || error == null) {
                    continue;
                }

                if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                    lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                    reenqueue();
                    return;
                } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
                    reenqueue();
                    return;
                } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                    fenced();
                    return;
                } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                    fatal(error.exception());
                    return;
                } else if (error == Errors.INVALID_PRODUCER_ID_MAPPING
                        || error == Errors.INVALID_TXN_STATE) {
                    fatal(new KafkaException(error.exception()));
                    return;
                } else {
                    log.error("Could not add partitions to transaction due to partition error. partition={}, error={}", topicPartition, error);
                    hasPartitionErrors = true;
                }
            }

            if (hasPartitionErrors) {
                fatal(new KafkaException("Could not add partitions to transaction due to partition level errors"));
            } else {
                partitionsInTransaction.addAll(pendingPartitionsToBeAddedToTransaction);
                pendingPartitionsToBeAddedToTransaction.clear();
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
            if (findCoordinatorResponse.error() == Errors.NONE) {
                Node node = findCoordinatorResponse.node();
                switch (builder.coordinatorType()) {
                    case GROUP:
                        consumerGroupCoordinator = node;
                        break;
                    case TRANSACTION:
                        transactionCoordinator = node;
                }
                result.done();
            } else if (findCoordinatorResponse.error() == Errors.COORDINATOR_NOT_AVAILABLE) {
                reenqueue();
            } else if (findCoordinatorResponse.error() == Errors.GROUP_AUTHORIZATION_FAILED) {
                fatal(new GroupAuthorizationException("Not authorized to commit offsets " + builder.coordinatorKey()));
            } else {
                fatal(new KafkaException(String.format("Could not find a coordinator with type %s with key %s due to" +
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
                fenced();
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatal(error.exception());
            } else {
                fatal(new KafkaException("Unhandled error in EndTxnResponse: " + error.message()));
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
                // note the result is not completed until the TxnOffsetCommit returns
                pendingRequests.add(txnOffsetCommitHandler(result, offsets, builder.consumerGroupId()));
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
                reenqueue();
            } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                fenced();
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatal(error.exception());
            } else {
                fatal(new KafkaException("Unexpected error in AddOffsetsToTxnResponse: " + error.message()));
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
            for (Map.Entry<TopicPartition, Errors> entry : txnOffsetCommitResponse.errors().entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                Errors error = entry.getValue();
                if (error == Errors.NONE) {
                    pendingTxnOffsetCommits.remove(topicPartition);
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                    hadFailure = true;
                    if (!coordinatorReloaded) {
                        coordinatorReloaded = true;
                        lookupCoordinator(FindCoordinatorRequest.CoordinatorType.GROUP, builder.consumerGroupId());
                    }
                } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                    fenced();
                    return;
                } else {
                    fatal(new KafkaException("Unexpected error in TxnOffsetCommitResponse: " + error.message()));
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
