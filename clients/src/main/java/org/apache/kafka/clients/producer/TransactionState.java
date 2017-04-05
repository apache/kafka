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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitPidRequest;
import org.apache.kafka.common.requests.InitPidResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.record.RecordBatch.NO_PRODUCER_EPOCH;
import static org.apache.kafka.common.record.RecordBatch.NO_PRODUCER_ID;

/**
 * A class which maintains state for transactions. Also keeps the state necessary to ensure idempotent production.
 */
public class TransactionState {
    private static final Logger log = LoggerFactory.getLogger(TransactionState.class);

    private volatile PidAndEpoch pidAndEpoch;
    private final Map<TopicPartition, Integer> sequenceNumbers;
    private final Time time;
    private final String transactionalId;
    private final TransactionCoordinator coordinator;
    private final int transactionTimeoutMs;
    private final Deque<TransactionalRequest> producerTransactionalRequests;
    private final Set<TopicPartition> newPartitionsToBeAddedToTransaction;
    private final Set<TopicPartition> pendingPartitionsToBeAddedToTransaction;
    private final Set<TopicPartition> partitionsInTransaction;
    private int inFlightRequestCorrelationId = Integer.MIN_VALUE;

    private volatile boolean isInTransaction = false;
    private volatile boolean isCompletingTransaction = false;

    public TransactionState(Time time, String transactionalId, int transactionTimeoutMs) {
        pidAndEpoch = new PidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH);
        sequenceNumbers = new HashMap<>();
        this.time = time;
        this.transactionalId = transactionalId;
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.producerTransactionalRequests = new ArrayDeque<>();
        this.coordinator = new TransactionCoordinator();
        this.newPartitionsToBeAddedToTransaction = new HashSet<>();
        this.pendingPartitionsToBeAddedToTransaction = new HashSet<>();
        this.partitionsInTransaction = new HashSet<>();
    }

    public static class TransactionalRequest {
        private final AbstractRequest.Builder<?> requestBuilder;

        private final boolean mustBeSentToCoordinator;
        private final RequestCompletionHandler handler;

        TransactionalRequest(AbstractRequest.Builder<?> requestBuilder, RequestCompletionHandler handler, boolean mustBeSentToCoordinator) {
            this.requestBuilder = requestBuilder;
            this.handler = handler;
            this.mustBeSentToCoordinator = mustBeSentToCoordinator;
        }

        public AbstractRequest.Builder<?> requestBuilder() {
            return requestBuilder;
        }

        public boolean mustBeSentToCoordinator() {
            return mustBeSentToCoordinator;
        }

        public RequestCompletionHandler responseHandler() {
            return handler;
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
        return !(producerTransactionalRequests.isEmpty()
                && newPartitionsToBeAddedToTransaction.isEmpty()
                && pendingPartitionsToBeAddedToTransaction.isEmpty());
    }

    public TransactionalRequest nextTransactionalRequest() {
        if (!hasPendingTransactionalRequests())
            return null;

        if (!producerTransactionalRequests.isEmpty())
            return producerTransactionalRequests.pollFirst();

        return addPartitionsToTransactionRequest();
    }

    public TransactionState(Time time) {
        this(time, "", 0);
    }

    public boolean isTransactional() {
        return !transactionalId.isEmpty();
    }

    public String transactionalId() {
        return transactionalId;
    }

    public boolean hasPid() {
        return pidAndEpoch.isValid();
    }

    public void beginTransaction() {
        isInTransaction = true;
    }

    public boolean isCompletingTransaction() {
        return isCompletingTransaction;
    }

    public void beginCommittingTransaction() {
        isCompletingTransaction = true;
        producerTransactionalRequests.add(endTransactionRequest(true));
    }

    public void beginAbortingTransaction() {
        isCompletingTransaction = true;
        producerTransactionalRequests.add(endTransactionRequest(false));
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
        producerTransactionalRequests.addFirst(request);
    }

    public Node coordinator() {
        return coordinator.node();
    }

    public void markCoordinatorDead(Node node) {
       coordinator.markCoordinatorDead(node);
       producerTransactionalRequests.addFirst(findCoordinatorRequest());
    }

    public void setInFlightRequestCorrelationId(int correlationId) {
        inFlightRequestCorrelationId = correlationId;
    }

    public void resetInFlightRequestCorrelationId() {
        inFlightRequestCorrelationId = Integer.MIN_VALUE;
    }

    public boolean hasInflightTransactionalRequest() {
        return inFlightRequestCorrelationId != Integer.MIN_VALUE;
    }

    /**
     * A blocking call to get the pid and epoch for the producer. If the PID and epoch has not been set, this method
     * will block for at most maxWaitTimeMs. It is expected that this method be called from application thread
     * contexts (ie. through Producer.send). The PID it self will be retrieved in the background thread.
     * @param maxWaitTimeMs The maximum time to block.
     * @return a PidAndEpoch object. Callers must call the 'isValid' method fo the returned object to ensure that a
     *         valid Pid and epoch is actually returned.
     */
    public synchronized PidAndEpoch awaitPidAndEpoch(long maxWaitTimeMs) throws InterruptedException {
        if (isTransactional()) {
            if (!coordinator.isValid())
                producerTransactionalRequests.add(findCoordinatorRequest());

            if (!hasPid())
                producerTransactionalRequests.add(initPidRequest());
        }
        long start = time.milliseconds();
        long elapsed = 0;
        while (!hasPid() && elapsed < maxWaitTimeMs) {
            wait(maxWaitTimeMs);
            elapsed = time.milliseconds() - start;
        }
        return pidAndEpoch;
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
        if (this.pidAndEpoch.isValid())
            notifyAll();
    }

    /**
     * This method is used when the producer needs to reset it's internal state because of an irrecoverable exception
     * from the broker.
     *
     * We need to reset the producer id and associated state when we have sent a batch to the broker, but we either get
     * a non-retriable exception or we run out of retries, or the batch expired in the producer queue after it was already
     * sent to the broker.
     *
     * In all of these cases, we don't know whether batch was actually committed on the broker, and hence whether the
     * sequence number was actually updated. If we don't reset the producer state, we risk the chance that all future
     * messages will return an OutOfOrderSequenceException.
     */
    public synchronized void resetProducerId() {
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

    private TransactionalRequest initPidRequest() {
        InitPidRequest.Builder builder = new InitPidRequest.Builder(transactionalId, transactionTimeoutMs);
        return new TransactionalRequest(builder, new InitPidCallback(), true);
    }

    private TransactionalRequest addPartitionsToTransactionRequest() {
        synchronized (newPartitionsToBeAddedToTransaction) {
            pendingPartitionsToBeAddedToTransaction.addAll(newPartitionsToBeAddedToTransaction);
            newPartitionsToBeAddedToTransaction.clear();
        }
        AddPartitionsToTxnRequest.Builder builder = new AddPartitionsToTxnRequest.Builder(transactionalId,
                pidAndEpoch.producerId, pidAndEpoch.epoch, new ArrayList<>(pendingPartitionsToBeAddedToTransaction));
        return new TransactionalRequest(builder, new AddPartitionsToTransactionCallback(), true);
    }

    private TransactionalRequest findCoordinatorRequest() {
        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
        return new TransactionalRequest(builder, new FindCoordinatorCallback(), false);
    }

    private TransactionalRequest endTransactionRequest(boolean isCommit) {
        EndTxnRequest.Builder builder = new EndTxnRequest.Builder(transactionalId,
                pidAndEpoch.producerId, pidAndEpoch.epoch, isCommit ? TransactionResult.COMMIT : TransactionResult.ABORT);
        return new TransactionalRequest(builder, new EndTxnCallback(), true);
    }

    private abstract class TransactionalRequestCallBack implements RequestCompletionHandler {
        @Override
        public void onComplete(ClientResponse response) {
            if (response.requestHeader().correlationId() != inFlightRequestCorrelationId)
                throw new IllegalStateException("Cannot have more than one transactional request in flight.");
            resetInFlightRequestCorrelationId();
            if (response.wasDisconnected()) {
                maybeReenqueue();
            } else if (response.versionMismatch() != null) {
                throw new KafkaException("Could not send " + response + " to node " + response.destination() +
                        " due to a version mismatch. Please ensure that all your brokers have been upgraded before enabling transactions.");
            } else if (response.hasResponse()) {
                handleResponse(response.responseBody());
            } else {
                throw new KafkaException("Failed to send " + response + " for unknown reasons. Please check your broker logs or your network. Cannot use the transactional producer at the moment.");
            }
        }

        protected abstract void handleResponse(AbstractResponse responseBody);

        protected abstract void maybeReenqueue();
    }

    private class InitPidCallback extends TransactionalRequestCallBack {
       @Override
       protected void handleResponse(AbstractResponse responseBody) {
           InitPidResponse initPidResponse = (InitPidResponse) responseBody;
           if (initPidResponse.error() == Errors.NONE) {
               setPidAndEpoch(initPidResponse.producerId(), initPidResponse.epoch());
           } else {
               throw new KafkaException("Need to handle error: " + initPidResponse.error());
           }
       }

        @Override
        protected  void maybeReenqueue() {
           producerTransactionalRequests.addFirst(initPidRequest());
        }

    }

    private class AddPartitionsToTransactionCallback extends TransactionalRequestCallBack {
        @Override
        public void handleResponse(AbstractResponse response) {
            AddPartitionsToTxnResponse addPartitionsToTxnResponse = (AddPartitionsToTxnResponse) response;
            if (addPartitionsToTxnResponse.error() == Errors.NONE) {
                partitionsInTransaction.addAll(pendingPartitionsToBeAddedToTransaction);
                pendingPartitionsToBeAddedToTransaction.clear();
            }
        }

        @Override
        protected void maybeReenqueue() {
           // no need to reenqueue since the pending partitions will automatically be added back in the next loop.
        }
    }

    private class FindCoordinatorCallback extends TransactionalRequestCallBack {
        @Override
        protected void handleResponse(AbstractResponse responseBody) {
            FindCoordinatorResponse findCoordinatorResponse = (FindCoordinatorResponse) responseBody;
            if (findCoordinatorResponse.error() == Errors.NONE) {
                Node node = findCoordinatorResponse.node();
                log.debug("Found transaction node {} for transactionalId {}", node, transactionalId);
                coordinator.setNode(node);
            } else {
                throw new KafkaException("Need to handle error: " + findCoordinatorResponse.error());
            }
        }

        @Override
        protected void maybeReenqueue() {
           producerTransactionalRequests.addFirst(initPidRequest());
        }
    }

    private class EndTxnCallback extends TransactionalRequestCallBack {
        @Override
        protected void handleResponse(AbstractResponse responseBody) {
            EndTxnResponse endTxnResponse = (EndTxnResponse) responseBody;
            if (endTxnResponse.error() == Errors.NONE) {
                isCompletingTransaction = false;
                isInTransaction = false;
            }
        }

        @Override
        protected void maybeReenqueue() {

        }
    }

    private static class TransactionCoordinator {
        private Node node;

        TransactionCoordinator() {
            node = null;
        }

        boolean isValid() {
            return node != null;
        }

        Node node() {
            return node;
        }

        synchronized void setNode(Node node) {
            this.node = node;
        }

        synchronized void markCoordinatorDead(Node node) {
            if (this.node.equals(node)) {
                this.node = null;
            }
        }
    }

}
