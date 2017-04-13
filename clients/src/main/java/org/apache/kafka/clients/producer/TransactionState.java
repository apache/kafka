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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.internals.FutureTransactionalResult;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
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
public class TransactionState {
    private static final Logger log = LoggerFactory.getLogger(TransactionState.class);

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
    private volatile boolean isInTransaction = false;
    private volatile boolean isCompletingTransaction = false;
    private volatile boolean isInitializing = false;
    private volatile boolean isFenced = false;

    public TransactionState(String transactionalId, int transactionTimeoutMs) {
        pidAndEpoch = new PidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH);
        sequenceNumbers = new HashMap<>();
        this.transactionalId = transactionalId;
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.pendingTransactionalRequests = new PriorityQueue<>(2, new Comparator<TransactionalRequest>() {
            @Override
            public int compare(TransactionalRequest o1, TransactionalRequest o2) {
                return o1.priority().priority() - o2.priority.priority();
            }
        });
        this.transactionCoordinator = null;
        this.consumerGroupCoordinator = null;
        this.newPartitionsToBeAddedToTransaction = new HashSet<>();
        this.pendingPartitionsToBeAddedToTransaction = new HashSet<>();
        this.partitionsInTransaction = new HashSet<>();
        this.pendingTxnOffsetCommits = new HashMap<>();
    }

    public TransactionState() {
        this("", 0);
    }

    public static class TransactionalRequest {
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
        private boolean isRetry;

        private TransactionalRequest(AbstractRequest.Builder<?> requestBuilder, RequestCompletionHandler handler,
                                     FindCoordinatorRequest.CoordinatorType coordinatorType, Priority priority,
                                     boolean isRetry, String coordinatorKey) {
            this.requestBuilder = requestBuilder;
            this.handler = handler;
            this.coordinatorType = coordinatorType;
            this.priority = priority;
            this.isRetry = isRetry;
            this.coordinatorKey = coordinatorKey;
        }

        public AbstractRequest.Builder<?> requestBuilder() {
            return requestBuilder;
        }

        public boolean needsCoordinator() {
            return coordinatorType != null;
        }

        public FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return coordinatorType;
        }

        public RequestCompletionHandler responseHandler() {
            return handler;
        }

        public boolean isRetry() {
            return isRetry;
        }

        public boolean isEndTxnRequest() {
            return priority == Priority.END_TXN;
        }

        private void setRetry() {
            isRetry = true;
        }

        private Priority priority() {
            return priority;
        }
    }

    public static class PidAndEpoch {
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

    public boolean hasPendingTransactionalRequests() {
        return !(pendingTransactionalRequests.isEmpty()
                && newPartitionsToBeAddedToTransaction.isEmpty());
    }


    public TransactionalRequest nextTransactionalRequest() {
        if (!hasPendingTransactionalRequests())
            return null;

        if (!newPartitionsToBeAddedToTransaction.isEmpty())
            pendingTransactionalRequests.add(addPartitionsToTransactionRequest(false));

        return pendingTransactionalRequests.poll();
    }


    public boolean isTransactional() {
        return transactionalId != null && !transactionalId.isEmpty();
    }

    public String transactionalId() {
        return transactionalId;
    }

    public boolean hasPid() {
        return pidAndEpoch.isValid() && !isFenced;
    }

    public boolean isFenced() {
        return isFenced;
    }

    public void beginTransaction() {
        isInTransaction = true;
    }

    public boolean isCompletingTransaction() {
        return isInTransaction && isCompletingTransaction;
    }

    public synchronized FutureTransactionalResult beginCommittingTransaction() {
        return beginCompletingTransaction(true);
    }

    public synchronized FutureTransactionalResult beginAbortingTransaction() {
        return beginCompletingTransaction(false);
    }

    private FutureTransactionalResult beginCompletingTransaction(boolean isCommit) {
        if (!isCompletingTransaction) {
            TransactionalRequestResult result = new TransactionalRequestResult();
            FutureTransactionalResult resultFuture = new FutureTransactionalResult(result);

            isCompletingTransaction = true;
            if (!newPartitionsToBeAddedToTransaction.isEmpty()) {
                pendingTransactionalRequests.add(addPartitionsToTransactionRequest(false));
            }
            pendingTransactionalRequests.add(endTxnRequest(isCommit, false, result));
            return resultFuture;
        }
        return null;
    }

    public synchronized FutureTransactionalResult sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) {
        TransactionalRequestResult result = new TransactionalRequestResult();
        FutureTransactionalResult resultFuture = new FutureTransactionalResult(result);
        pendingTransactionalRequests.add(addOffsetsToTxnRequest(offsets, consumerGroupId, false, result));
        return resultFuture;
    }

    public boolean isInTransaction() {
        return isTransactional() && isInTransaction;
    }


    public synchronized void maybeAddPartitionToTransaction(TopicPartition topicPartition) {
        if (partitionsInTransaction.contains(topicPartition))
            return;
        newPartitionsToBeAddedToTransaction.add(topicPartition);
    }

    public void needsRetry(TransactionalRequest request) {
        request.setRetry();
        pendingTransactionalRequests.add(request);
    }

    public void didNotSend(TransactionalRequest request) {
        pendingTransactionalRequests.add(request);
    }

    public Node coordinator(FindCoordinatorRequest.CoordinatorType type) {
        switch (type) {
            case GROUP:
                return consumerGroupCoordinator;
            case TRANSACTION:
                return transactionCoordinator;
            default:
                throw new IllegalStateException("Received an invalid coordinator type: " + type);
        }
    }

    public void needsCoordinator(TransactionalRequest request) {
        needsCoordinator(request.coordinatorType, request.coordinatorKey);
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
                throw new IllegalStateException("Got an invalid coordintor type: " + type);
        }
        pendingTransactionalRequests.add(findCoordinatorRequest(type, coordinatorKey, false));
    }


    public void setInFlightRequestCorrelationId(int correlationId) {
        inFlightRequestCorrelationId = correlationId;
    }

    public void resetInFlightRequestCorrelationId() {
        inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    public boolean hasInflightTransactionalRequest() {
        return inFlightRequestCorrelationId != NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    // visible for testing
    public boolean transactionContainsPartition(TopicPartition topicPartition) {
        return isInTransaction && partitionsInTransaction.contains(topicPartition);
    }

    // visible for testing
    public boolean hasPendingOffsetCommits() {
        return isInTransaction && 0 < pendingTxnOffsetCommits.size();
    }

    public synchronized FutureTransactionalResult initializeTransactions() {
        if (isInitializing) {
            throw new IllegalStateException("Multiple concurrent calls to initTransactions are not allowed.");
        }
        isInitializing = true;
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

    /**
     * Get the current pid and epoch without blocking. Callers must use {@link PidAndEpoch#isValid()} to
     * verify that the result is valid.
     *
     * @return the current PidAndEpoch.
     */
    public PidAndEpoch pidAndEpoch() {
        return pidAndEpoch;
    }

    /**
     * Set the pid and epoch atomically. This method will signal any callers blocked on the `pidAndEpoch` method
     * once the pid is set. This method will be called on the background thread when the broker responds with the pid.
     */
    public synchronized void setPidAndEpoch(long pid, short epoch) {
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
    public synchronized void resetProducerId() {
        if (isTransactional())
            return;
        setPidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH);
        this.sequenceNumbers.clear();
    }

    /**
     * Returns the next sequence number to be written to the given TopicPartition.
     */
    public synchronized Integer sequenceNumber(TopicPartition topicPartition) {
        Integer currentSequenceNumber = sequenceNumbers.get(topicPartition);
        if (currentSequenceNumber == null) {
            currentSequenceNumber = 0;
            sequenceNumbers.put(topicPartition, currentSequenceNumber);
        }
        return currentSequenceNumber;
    }

    public synchronized void incrementSequenceNumber(TopicPartition topicPartition, int increment) {
        Integer currentSequenceNumber = sequenceNumbers.get(topicPartition);
        if (currentSequenceNumber == null)
            throw new IllegalStateException("Attempt to increment sequence number for a partition with no current sequence.");

        currentSequenceNumber += increment;
        sequenceNumbers.put(topicPartition, currentSequenceNumber);
    }

    private void completeTransaction() {
        partitionsInTransaction.clear();
        isInTransaction = false;
        isCompletingTransaction = false;
    }

    private TransactionalRequest initPidRequest(boolean isRetry, TransactionalRequestResult result) {
        InitPidRequest.Builder builder = new InitPidRequest.Builder(transactionalId, transactionTimeoutMs);
        return new TransactionalRequest(builder, new InitPidCallback(result),
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, TransactionalRequest.Priority.INIT_PRODUCER_ID, isRetry, transactionalId);
    }

    private synchronized TransactionalRequest addPartitionsToTransactionRequest(boolean isRetry) {
        pendingPartitionsToBeAddedToTransaction.addAll(newPartitionsToBeAddedToTransaction);
        newPartitionsToBeAddedToTransaction.clear();
        AddPartitionsToTxnRequest.Builder builder = new AddPartitionsToTxnRequest.Builder(transactionalId,
                pidAndEpoch.producerId, pidAndEpoch.epoch, new ArrayList<>(pendingPartitionsToBeAddedToTransaction));
        return new TransactionalRequest(builder, new AddPartitionsToTransactionCallback(),
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, TransactionalRequest.Priority.ADD_PARTITIONS_OR_OFFSETS, isRetry, transactionalId);
    }

    private TransactionalRequest findCoordinatorRequest(FindCoordinatorRequest.CoordinatorType type, String coordinatorKey, boolean isRetry) {
        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(type, coordinatorKey);
        return new TransactionalRequest(builder, new FindCoordinatorCallback(type, coordinatorKey),
                null, TransactionalRequest.Priority.FIND_COORDINATOR, isRetry, null);
    }

    private TransactionalRequest endTxnRequest(boolean isCommit, boolean isRetry, TransactionalRequestResult result) {
        EndTxnRequest.Builder builder = new EndTxnRequest.Builder(transactionalId,
                pidAndEpoch.producerId, pidAndEpoch.epoch, isCommit ? TransactionResult.COMMIT : TransactionResult.ABORT);
        return new TransactionalRequest(builder, new EndTxnCallback(isCommit, result),
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, TransactionalRequest.Priority.END_TXN, isRetry, transactionalId);
    }

    private TransactionalRequest addOffsetsToTxnRequest(Map<TopicPartition, OffsetAndMetadata> offsets,
                                                        String consumerGroupId, boolean isRetry, TransactionalRequestResult result) {
        AddOffsetsToTxnRequest.Builder builder = new AddOffsetsToTxnRequest.Builder(transactionalId,
                pidAndEpoch.producerId, pidAndEpoch.epoch, consumerGroupId);
        return new TransactionalRequest(builder, new AddOffsetsToTxnCallback(offsets, consumerGroupId, result),
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, TransactionalRequest.Priority.ADD_PARTITIONS_OR_OFFSETS, isRetry, transactionalId);
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
                FindCoordinatorRequest.CoordinatorType.GROUP, TransactionalRequest.Priority.ADD_PARTITIONS_OR_OFFSETS, isRetry, consumerGroupId);
    }

    private abstract class TransactionalRequestCallBack implements RequestCompletionHandler {
        protected final TransactionalRequestResult result;

        TransactionalRequestCallBack(TransactionalRequestResult result) {
            this.result = result;
        }

        @Override
        public void onComplete(ClientResponse response) {
            if (response.requestHeader().correlationId() != inFlightRequestCorrelationId)
                throw new IllegalStateException("Cannot have more than one transactional request in flight.");
            resetInFlightRequestCorrelationId();
            if (response.wasDisconnected()) {
                reenqueue();
            } else if (response.versionMismatch() != null) {
                if (result != null) {
                    result.setError(Errors.UNSUPPORTED_VERSION.exception());
                    result.done();
                } else {
                    throw Errors.UNSUPPORTED_VERSION.exception();
                }
            } else if (response.hasResponse()) {
                handleResponse(response.responseBody());
            } else {
                if (result != null) {
                    result.setError(Errors.UNKNOWN.exception());
                    result.done();
                } else {
                    throw Errors.UNKNOWN.exception();
                }
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
                isInitializing = false;
            } else if (error == Errors.NOT_COORDINATOR || error == Errors.COORDINATOR_NOT_AVAILABLE) {
                needsCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                reenqueue();
            } else if (error == Errors.INVALID_TRANSACTION_TIMEOUT) {
                result.setError(error.exception());
            } else {
                result.setError(error.exception());
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
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                reenqueue();
            } else if (error == Errors.INVALID_PID_MAPPING || error == Errors.INVALID_TXN_STATE) {
                throw error.exception();
            } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                isFenced = true;
                throw error.exception();
            } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                throw error.exception();
            } else {
                throw Errors.UNKNOWN.exception();
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
                throw Errors.GROUP_AUTHORIZATION_FAILED.exception();
            } else {
                throw Errors.UNKNOWN.exception();
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
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                reenqueue();
            } else if (error == Errors.INVALID_PID_MAPPING || error == Errors.INVALID_TXN_STATE) {
                result.setError(error.exception());
            } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                isFenced = true;
                result.setError(error.exception());
            } else {
                result.setError(error.exception());
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
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                reenqueue();
            } else if (error == Errors.INVALID_PID_MAPPING || error == Errors.INVALID_TXN_STATE) {
                result.setError(error.exception());
            } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                isFenced = true;
                result.setError(error.exception());
            } else {
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
                    isFenced = true;
                    result.setError(error.exception());
                    break;
                }
            }

            if (!hadFailure || !result.isSuccessful()) {
                // all attempted partitions were either successful, or there was a fatal failure.
                // either way, we are not retrying, so complete the request.
                result.done();
                return;
            }

            if (0 < pendingTxnOffsetCommits.size()) {
                pendingTransactionalRequests.add(txnOffsetCommitRequest(consumerGroupId, true, result));
            }
        }

        @Override
        public void reenqueue() {
            pendingTransactionalRequests.add(txnOffsetCommitRequest(consumerGroupId, true, result));
        }
    }
}
