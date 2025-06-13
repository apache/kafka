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

package org.apache.kafka.server.transaction;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransaction;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransactionCollection;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.server.transaction.AddPartitionsToTxnManager.TransactionSupportedOperation.ADD_PARTITION;
import static org.apache.kafka.server.transaction.AddPartitionsToTxnManager.TransactionSupportedOperation.DEFAULT_ERROR;
import static org.apache.kafka.server.transaction.AddPartitionsToTxnManager.TransactionSupportedOperation.GENERIC_ERROR_SUPPORTED;

public class AddPartitionsToTxnManager extends InterBrokerSendThread {

    public static final String VERIFICATION_FAILURE_RATE_METRIC_NAME = "VerificationFailureRate";
    public static final String VERIFICATION_TIME_MS_METRIC_NAME = "VerificationTimeMs";

    /**
     * handles the Partition Response based on the Request Version and the exact operation.
     */
    public enum TransactionSupportedOperation {
        /**
         * This is the default workflow which maps to cases when the Produce Request Version or the
         * Txn_offset_commit request was lower than the first version supporting the new Error Class.
         */
        DEFAULT_ERROR(false),
        /**
         * This maps to the case when the clients are updated to handle the TransactionAbortableException.
         */
        GENERIC_ERROR_SUPPORTED(false),
        /**
         * This allows the partition to be added to the transactions inflight with the Produce and TxnOffsetCommit requests.
         * Plus the behaviors in genericErrorSupported.
         */
        ADD_PARTITION(true);

        public final boolean supportsEpochBump;

        TransactionSupportedOperation(boolean supportsEpochBump) {
            this.supportsEpochBump = supportsEpochBump;
        }
    }

    @FunctionalInterface
    public interface AppendCallback {
        void complete(Map<TopicPartition, Errors> partitionErrors);
    }

    public static TransactionSupportedOperation produceRequestVersionToTransactionSupportedOperation(short version) {
        if (version > 11) {
            return ADD_PARTITION;
        } else if (version > 10) {
            return GENERIC_ERROR_SUPPORTED;
        } else {
            return DEFAULT_ERROR;
        }
    }

    public static TransactionSupportedOperation txnOffsetCommitRequestVersionToTransactionSupportedOperation(int version) {
        if (version > 4) {
            return ADD_PARTITION;
        } else if (version > 3) {
            return GENERIC_ERROR_SUPPORTED;
        } else {
            return DEFAULT_ERROR;
        }
    }

    /*
     * Data structure to hold the transactional data to send to a node. Note -- at most one request per transactional ID
     * will exist at a time in the map. If a given transactional ID exists in the map, and a new request with the same ID
     * comes in, one request will be in the map and one will return to the producer with a response depending on the epoch.
     */
    public record TransactionDataAndCallbacks(
            AddPartitionsToTxnTransactionCollection transactionData,
            Map<String, AppendCallback> callbacks,
            Map<String, Long> startTimeMs,
            TransactionSupportedOperation transactionSupportedOperation) { }

    private class AddPartitionsToTxnHandler implements RequestCompletionHandler {
        private final Node node;
        private final TransactionDataAndCallbacks transactionDataAndCallbacks;

        public AddPartitionsToTxnHandler(Node node, TransactionDataAndCallbacks transactionDataAndCallbacks) {
            this.node = node;
            this.transactionDataAndCallbacks = transactionDataAndCallbacks;
        }

        @Override
        public void onComplete(ClientResponse response) {
            // Note: Synchronization is not needed on inflightNodes since it is always accessed from this thread.
            inflightNodes.remove(node);
            if (response.authenticationException() != null) {
                log.error("AddPartitionsToTxnRequest failed for node {} with an authentication exception.", response.destination(), response.authenticationException());
                sendCallbacksToAll(Errors.forException(response.authenticationException()).code());
            } else if (response.versionMismatch() != null) {
                // We may see unsupported version exception if we try to send a verify only request to a broker that can't handle it.
                // In this case, skip verification.
                log.warn("AddPartitionsToTxnRequest failed for node {} with invalid version exception. " +
                        "This suggests verification is not supported. Continuing handling the produce request.", response.destination());
                transactionDataAndCallbacks.callbacks().forEach((txnId, callback) ->
                        sendCallback(callback, Map.of(), transactionDataAndCallbacks.startTimeMs.get(txnId)));
            } else if (response.wasDisconnected() || response.wasTimedOut()) {
                log.warn("AddPartitionsToTxnRequest failed for node {} with a network exception.", response.destination());
                sendCallbacksToAll(Errors.NETWORK_EXCEPTION.code());
            } else {
                AddPartitionsToTxnResponseData responseData = ((AddPartitionsToTxnResponse) response.responseBody()).data();
                if (responseData.errorCode() != 0) {
                    log.error("AddPartitionsToTxnRequest for node {} returned with error {}.",
                            response.destination(), Errors.forCode(responseData.errorCode()));
                    // The client should not be exposed to CLUSTER_AUTHORIZATION_FAILED so modify the error to signify the verification did not complete.
                    // Return INVALID_TXN_STATE.
                    short finalError = responseData.errorCode() == Errors.CLUSTER_AUTHORIZATION_FAILED.code()
                            ? Errors.INVALID_TXN_STATE.code() : responseData.errorCode();
                    sendCallbacksToAll(finalError);
                } else {
                    for (AddPartitionsToTxnResponseData.AddPartitionsToTxnResult txnResult : responseData.resultsByTransaction()) {
                        Map<TopicPartition, Errors> unverified = new HashMap<>();
                        for (AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult topicResult : txnResult.topicResults()) {
                            for (AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult partitionResult : topicResult.resultsByPartition()) {
                                TopicPartition tp = new TopicPartition(topicResult.name(), partitionResult.partitionIndex());
                                if (partitionResult.partitionErrorCode() != Errors.NONE.code()) {
                                    // Producers expect to handle INVALID_PRODUCER_EPOCH in this scenario.
                                    short code;
                                    if (partitionResult.partitionErrorCode() == Errors.PRODUCER_FENCED.code()) {
                                        code = Errors.INVALID_PRODUCER_EPOCH.code();
                                    } else if (partitionResult.partitionErrorCode() == Errors.TRANSACTION_ABORTABLE.code()
                                            && transactionDataAndCallbacks.transactionSupportedOperation().equals(DEFAULT_ERROR)) { // For backward compatibility with clients
                                        code = Errors.INVALID_TXN_STATE.code();
                                    } else {
                                        code = partitionResult.partitionErrorCode();
                                    }
                                    unverified.put(tp, Errors.forCode(code));
                                }
                            }
                        }
                        verificationFailureRate.mark(unverified.size());
                        AppendCallback callback = transactionDataAndCallbacks.callbacks().get(txnResult.transactionalId());
                        sendCallback(callback, unverified, transactionDataAndCallbacks.startTimeMs.get(txnResult.transactionalId()));
                    }
                }
            }
            wakeup();
        }

        private Map<TopicPartition, Errors> buildErrorMap(String transactionalId, short errorCode) {
            AddPartitionsToTxnTransaction transactionData = transactionDataAndCallbacks.transactionData.find(transactionalId);
            return topicPartitionsToError(transactionData, Errors.forCode(errorCode));
        }

        private void sendCallbacksToAll(short errorCode) {
            transactionDataAndCallbacks.callbacks.forEach((txnId, cb) ->
                    sendCallback(cb, buildErrorMap(txnId, errorCode), transactionDataAndCallbacks.startTimeMs.get(txnId)));
        }
    }

    private final MetadataCache metadataCache;
    private final Function<String, Integer> partitionFor;
    private final Time time;

    private final ListenerName interBrokerListenerName;
    private final Set<Node> inflightNodes = new HashSet<>();
    private final Map<Node, TransactionDataAndCallbacks> nodesToTransactions = new HashMap<>();

    // For compatibility - this metrics group was previously defined within
    // a Scala class named `kafka.server.AddPartitionsToTxnManager`
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.server", "AddPartitionsToTxnManager");
    private final Meter verificationFailureRate = metricsGroup.newMeter(VERIFICATION_FAILURE_RATE_METRIC_NAME, "failures", TimeUnit.SECONDS);
    private final Histogram verificationTimeMs = metricsGroup.newHistogram(VERIFICATION_TIME_MS_METRIC_NAME);

    public AddPartitionsToTxnManager(
            AbstractKafkaConfig config,
            NetworkClient client,
            MetadataCache metadataCache,
            Function<String, Integer> partitionFor,
            Time time) {
        super("AddPartitionsToTxnSenderThread-" + config.brokerId(), client, config.requestTimeoutMs(), time);
        this.interBrokerListenerName = config.interBrokerListenerName();
        this.metadataCache = metadataCache;
        this.partitionFor = partitionFor;
        this.time = time;
    }

    public void addOrVerifyTransaction(
            String transactionalId,
            long producerId,
            short producerEpoch,
            Collection<TopicPartition> topicPartitions,
            AppendCallback callback,
            TransactionSupportedOperation transactionSupportedOperation) {
        Optional<Node> coordinator = getTransactionCoordinator(partitionFor.apply(transactionalId));
        if (coordinator.isEmpty()) {
            callback.complete(topicPartitions.stream().collect(
                    Collectors.toMap(Function.identity(), tp -> Errors.COORDINATOR_NOT_AVAILABLE)));
        } else {
            AddPartitionsToTxnTopicCollection topicCollection = new AddPartitionsToTxnTopicCollection();
            topicPartitions.forEach(tp -> {
                var toTxnTopic = topicCollection.find(tp.topic());
                if (toTxnTopic == null) {
                    toTxnTopic = new AddPartitionsToTxnTopic().setName(tp.topic());
                    topicCollection.add(toTxnTopic);
                }
                toTxnTopic.partitions().add(tp.partition());
            });

            AddPartitionsToTxnTransaction transactionData = new AddPartitionsToTxnTransaction()
                    .setTransactionalId(transactionalId)
                    .setProducerId(producerId)
                    .setProducerEpoch(producerEpoch)
                    .setVerifyOnly(!transactionSupportedOperation.supportsEpochBump)
                    .setTopics(topicCollection);

            addTxnData(coordinator.get(), transactionData, callback, transactionSupportedOperation);
        }
    }

    private void addTxnData(
            Node node,
            AddPartitionsToTxnTransaction transactionData,
            AppendCallback callback,
            TransactionSupportedOperation transactionSupportedOperation) {
        synchronized (nodesToTransactions) {
            long curTime = time.milliseconds();
            // Check if we have already had either node or individual transaction. Add the Node if it isn't there.
            TransactionDataAndCallbacks existingNodeAndTransactionData = nodesToTransactions.computeIfAbsent(node,
                    ignored -> new TransactionDataAndCallbacks(
                            new AddPartitionsToTxnTransactionCollection(1),
                            new HashMap<>(),
                            new HashMap<>(),
                            transactionSupportedOperation));

            AddPartitionsToTxnTransaction existingTransactionData = existingNodeAndTransactionData.transactionData.find(transactionData.transactionalId());

            // There are 3 cases if we already have existing data
            // 1. Incoming data has a higher epoch -- return INVALID_PRODUCER_EPOCH for existing data since it is fenced
            // 2. Incoming data has the same epoch -- return NETWORK_EXCEPTION for existing data, since the client is likely retrying and we want another retriable exception
            // 3. Incoming data has a lower epoch -- return INVALID_PRODUCER_EPOCH for the incoming data since it is fenced, do not add incoming data to verify
            if (existingTransactionData != null) {
                if (existingTransactionData.producerEpoch() <= transactionData.producerEpoch()) {
                    Errors error = (existingTransactionData.producerEpoch() < transactionData.producerEpoch())
                            ? Errors.INVALID_PRODUCER_EPOCH : Errors.NETWORK_EXCEPTION;
                    AppendCallback oldCallback = existingNodeAndTransactionData.callbacks.get(transactionData.transactionalId());
                    existingNodeAndTransactionData.transactionData.remove(transactionData);
                    sendCallback(oldCallback, topicPartitionsToError(existingTransactionData, error), existingNodeAndTransactionData.startTimeMs.get(transactionData.transactionalId()));
                } else {
                    // If the incoming transactionData's epoch is lower, we can return with INVALID_PRODUCER_EPOCH immediately.
                    sendCallback(callback, topicPartitionsToError(transactionData, Errors.INVALID_PRODUCER_EPOCH), curTime);
                    return;
                }
            }

            existingNodeAndTransactionData.transactionData.add(transactionData);
            existingNodeAndTransactionData.callbacks.put(transactionData.transactionalId(), callback);
            existingNodeAndTransactionData.startTimeMs.put(transactionData.transactionalId(), curTime);
            wakeup();
        }
    }

    private Optional<Node> getTransactionCoordinator(int partition) {
        return metadataCache.getLeaderAndIsr(Topic.TRANSACTION_STATE_TOPIC_NAME, partition)
                .filter(leaderAndIsr -> leaderAndIsr.leader() != MetadataResponse.NO_LEADER_ID)
                .flatMap(metadata -> metadataCache.getAliveBrokerNode(metadata.leader(), interBrokerListenerName));
    }

    private Map<TopicPartition, Errors> topicPartitionsToError(AddPartitionsToTxnTransaction txnData, Errors error) {
        Map<TopicPartition, Errors> topicPartitionsToError = new HashMap<>();
        txnData.topics().forEach(topic ->
            topic.partitions().forEach(partition ->
                topicPartitionsToError.put(new TopicPartition(topic.name(), partition), error)));
        verificationFailureRate.mark(topicPartitionsToError.size());
        return topicPartitionsToError;
    }

    private void sendCallback(AppendCallback callback, Map<TopicPartition, Errors> errors, long startTimeMs) {
        verificationTimeMs.update(time.milliseconds() - startTimeMs);
        callback.complete(errors);
    }

    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
        // build and add requests to the queue
        List<RequestAndCompletionHandler> list = new ArrayList<>();
        var currentTimeMs = time.milliseconds();
        synchronized (nodesToTransactions) {
            var iter = nodesToTransactions.entrySet().iterator();
            while (iter.hasNext()) {
                var entry = iter.next();
                var node = entry.getKey();
                var transactionDataAndCallbacks = entry.getValue();
                if (!inflightNodes.contains(node)) {
                    list.add(new RequestAndCompletionHandler(
                            currentTimeMs,
                            node,
                            AddPartitionsToTxnRequest.Builder.forBroker(transactionDataAndCallbacks.transactionData()),
                            new AddPartitionsToTxnHandler(node, transactionDataAndCallbacks)
                    ));
                    inflightNodes.add(node);
                    iter.remove();
                }
            }
        }
        return list;
    }

    @Override
    public void shutdown() throws InterruptedException {
        super.shutdown();
        metricsGroup.removeMetric(VERIFICATION_FAILURE_RATE_METRIC_NAME);
        metricsGroup.removeMetric(VERIFICATION_TIME_MS_METRIC_NAME);
    }
}
