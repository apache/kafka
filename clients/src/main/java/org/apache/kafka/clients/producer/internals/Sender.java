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

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Optional;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientRDMARequest;
import org.apache.kafka.clients.ClientRDMAResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.ProduceRDMAWriteRequest;
import org.apache.kafka.clients.RdmaClient;
import org.apache.kafka.clients.RDMARequestCompletionHandler;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RDMAProduceAddressRequest;
import org.apache.kafka.common.requests.RDMAProduceAddressResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.IOException;

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;

/**
 * The background thread that handles the sending of produce requests to the Kafka cluster. This thread makes metadata
 * requests to renew its view of the cluster and then sends produce requests to the appropriate nodes.
 */
public class Sender implements Runnable {

    private final Logger log;

    /* the state of each nodes connection */
    private final KafkaClient client;

    /* the record accumulator that batches records */
    private final RecordAccumulator accumulator;

    /* the metadata for the client */
    private final Metadata metadata;

    /* the flag indicating whether the producer should guarantee the message order on the broker or not. */
    private final boolean guaranteeMessageOrder;

    /* the maximum request size to attempt to send to the server */
    private final int maxRequestSize;

    /* the number of acknowledgements to request from the server */
    private final short acks;

    /* the number of times to retry a failed request before giving up */
    private final int retries;

    /* the clock instance used for getting the time */
    private final Time time;

    /* true while the sender thread is still running */
    private volatile boolean running;

    /* true when the caller wants to ignore all unsent/inflight messages and force close.  */
    private volatile boolean forceClose;

    /* metrics */
    private final SenderMetrics sensors;

    /* the max time to wait for the server to respond to the request*/
    private final int requestTimeoutMs;

    /* The max time to wait before retrying a request which has failed */
    private final long retryBackoffMs;

    /* current request API versions supported by the known brokers */
    private final ApiVersions apiVersions;

    /* all the state related to transactions, in particular the producer id, producer epoch, and sequence numbers */
    private final TransactionManager transactionManager;

    // A per-partition queue of batches ordered by creation time for tracking the in-flight batches
    private final Map<TopicPartition, List<ProducerBatch>> inFlightBatches;

    private final RdmaClient rdmaClient;

    private Integer pendingRdma = 0;

    public Sender(LogContext logContext,
                  KafkaClient client,
                  Metadata metadata,
                  RecordAccumulator accumulator,
                  boolean guaranteeMessageOrder,
                  int maxRequestSize,
                  short acks,
                  int retries,
                  SenderMetricsRegistry metricsRegistry,
                  Time time,
                  int requestTimeoutMs,
                  long retryBackoffMs,
                  TransactionManager transactionManager,
                  ApiVersions apiVersions) {
        this(logContext, client, metadata, accumulator, guaranteeMessageOrder, maxRequestSize, acks, retries,
                metricsRegistry, time, requestTimeoutMs, retryBackoffMs, transactionManager, apiVersions, null);
    }

    public Sender(LogContext logContext,
                  KafkaClient client,
                  Metadata metadata,
                  RecordAccumulator accumulator,
                  boolean guaranteeMessageOrder,
                  int maxRequestSize,
                  short acks,
                  int retries,
                  SenderMetricsRegistry metricsRegistry,
                  Time time,
                  int requestTimeoutMs,
                  long retryBackoffMs,
                  TransactionManager transactionManager,
                  ApiVersions apiVersions,
                  RdmaClient rdmaClient) {
        this.log = logContext.logger(Sender.class);
        this.client = client;
        this.accumulator = accumulator;
        this.metadata = metadata;
        this.guaranteeMessageOrder = guaranteeMessageOrder;
        this.maxRequestSize = maxRequestSize;
        this.running = true;
        this.acks = acks;
        this.retries = retries;
        this.time = time;
        this.sensors = new SenderMetrics(metricsRegistry, metadata, client, time);
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.apiVersions = apiVersions;
        this.transactionManager = transactionManager;
        this.inFlightBatches = new HashMap<>();
        this.rdmaClient = rdmaClient;
    }

    public List<ProducerBatch> inFlightBatches(TopicPartition tp) {
        return inFlightBatches.containsKey(tp) ? inFlightBatches.get(tp) : new ArrayList<>();
    }

    public void maybeRemoveFromInflightBatches(ProducerBatch batch) {
        List<ProducerBatch> batches = inFlightBatches.get(batch.topicPartition);
        if (batches != null) {
            batches.remove(batch);
            if (batches.isEmpty()) {
                inFlightBatches.remove(batch.topicPartition);
            }
        }
    }

    /**
     *  Get the in-flight batches that has reached delivery timeout.
     */
    private List<ProducerBatch> getExpiredInflightBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();

        for (Iterator<Map.Entry<TopicPartition, List<ProducerBatch>>> batchIt = inFlightBatches.entrySet().iterator(); batchIt.hasNext();) {
            Map.Entry<TopicPartition, List<ProducerBatch>> entry = batchIt.next();
            List<ProducerBatch> partitionInFlightBatches = entry.getValue();
            if (partitionInFlightBatches != null) {
                Iterator<ProducerBatch> iter = partitionInFlightBatches.iterator();
                while (iter.hasNext()) {
                    ProducerBatch batch = iter.next();
                    if (batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now)) {
                        iter.remove();
                        // expireBatches is called in Sender.sendProducerData, before client.poll.
                        // The !batch.isDone() invariant should always hold. An IllegalStateException
                        // exception will be thrown if the invariant is violated.
                        if (!batch.isDone()) {
                            expiredBatches.add(batch);
                        } else {
                            throw new IllegalStateException(batch.topicPartition + " batch created at " +
                                batch.createdMs + " gets unexpected final state " + batch.finalState());
                        }
                    } else {
                        accumulator.maybeUpdateNextBatchExpiryTime(batch);
                        break;
                    }
                }
                if (partitionInFlightBatches.isEmpty()) {
                    batchIt.remove();
                }
            }
        }
        return expiredBatches;
    }

    private void addToInflightBatches(List<ProducerBatch> batches) {
        for (ProducerBatch batch : batches) {
            List<ProducerBatch> inflightBatchList = inFlightBatches.get(batch.topicPartition);
            if (inflightBatchList == null) {
                inflightBatchList = new ArrayList<>();
                inFlightBatches.put(batch.topicPartition, inflightBatchList);
            }
            inflightBatchList.add(batch);
        }
    }

    public void addToInflightBatches(Map<Integer, List<ProducerBatch>> batches) {
        for (List<ProducerBatch> batchList : batches.values()) {
            addToInflightBatches(batchList);
        }
    }

    /**
     * The main run loop for the sender thread
     */
    public void run() {
        log.debug("Starting Kafka producer I/O thread.");

        // main loop, runs until close is called
        while (running) {
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

        // okay we stopped accepting requests but there may still be
        // requests in the accumulator or waiting for acknowledgment,
        // wait until these are completed.
        while (!forceClose && (this.accumulator.hasUndrained() || this.client.inFlightRequestCount() > 0)) {
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }
        if (forceClose) {
            // We need to fail all the incomplete batches and wake up the threads waiting on
            // the futures.
            log.debug("Aborting incomplete batches due to forced shutdown");
            this.accumulator.abortIncompleteBatches();
        }
        try {
            this.client.close();
        } catch (Exception e) {
            log.error("Failed to close network client", e);
        }

        log.debug("Shutdown of Kafka producer I/O thread has completed.");
    }

    /**
     * Run a single iteration of sending
     *
     */
    void runOnce() {
        if (transactionManager != null) {
            try {
                if (transactionManager.shouldResetProducerStateAfterResolvingSequences())
                    // Check if the previous run expired batches which requires a reset of the producer state.
                    transactionManager.resetProducerId();

                if (!transactionManager.isTransactional()) {
                    // this is an idempotent producer, so make sure we have a producer id
                    maybeWaitForProducerId();
                } else if (transactionManager.hasUnresolvedSequences() && !transactionManager.hasFatalError()) {
                    transactionManager.transitionToFatalError(
                        new KafkaException("The client hasn't received acknowledgment for " +
                            "some previously sent messages and can no longer retry them. It isn't safe to continue."));
                } else if (transactionManager.hasInFlightTransactionalRequest() || maybeSendTransactionalRequest()) {
                    // as long as there are outstanding transactional requests, we simply wait for them to return
                    client.poll(retryBackoffMs, time.milliseconds());
                    return;
                }

                // do not continue sending if the transaction manager is in a failed state or if there
                // is no producer id (for the idempotent case).
                if (transactionManager.hasFatalError() || !transactionManager.hasProducerId()) {
                    RuntimeException lastError = transactionManager.lastError();
                    if (lastError != null)
                        maybeAbortBatches(lastError);
                    client.poll(retryBackoffMs, time.milliseconds());
                    return;
                } else if (transactionManager.hasAbortableError()) {
                    accumulator.abortUndrainedBatches(transactionManager.lastError());
                }
            } catch (AuthenticationException e) {
                // This is already logged as error, but propagated here to perform any clean ups.
                log.trace("Authentication exception while processing transactional request: {}", e);
                transactionManager.authenticationFailed(e);
            }
        }

        long currentTimeMs = time.milliseconds();
        long currentTimeNanos = time.nanoseconds();


        long pollTimeout = sendProducerData(currentTimeMs, currentTimeNanos);

        long currentTimeNanos2 = time.nanoseconds();

        if (pendingRdma != 0) {
            pollTimeout = 0;
        }

        rdmaClient.poll(0, currentTimeNanos2);
        client.poll(pollTimeout, currentTimeMs);
    }

    private long sendProducerData(long now, long nowNanos) {
        Cluster cluster = metadata.fetch();

        // get the list of partitions with data ready to send
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);

        // if there are any partitions whose leaders are not known yet, force metadata update
        if (!result.unknownLeaderTopics.isEmpty()) {
            // The set of topics with unknown leader contains topics with leader election pending as well as
            // topics which may have expired. Add the topic again to metadata to ensure it is included
            // and request metadata update, since there are messages to send to the topic.
            for (String topic : result.unknownLeaderTopics)
                this.metadata.add(topic);

            log.debug("Requesting metadata update due to unknown leader topics from the batched records: {}",
                result.unknownLeaderTopics);
            this.metadata.requestUpdate();
        }


        // remove any nodes we aren't ready to send to
        Iterator<Node> iter = result.readyNodes.iterator();
        long notReadyTimeout = Long.MAX_VALUE;
        while (iter.hasNext()) {
            Node node = iter.next();
            if (rdmaClient.isContended(node)) {
                iter.remove();
            } else if (!this.client.ready(node, now)) {
                iter.remove();
                notReadyTimeout = Math.min(notReadyTimeout, this.client.pollDelayMs(node, now));
            }
        }


        // create produce requests
        Map<Integer, List<ProducerBatch>> batches = this.accumulator.drain(cluster, result.readyNodes, this.maxRequestSize, now);
        addToInflightBatches(batches);
        if (guaranteeMessageOrder) {
            // Mute all the partitions drained
            for (List<ProducerBatch> batchList : batches.values()) {
                for (ProducerBatch batch : batchList)
                    this.accumulator.mutePartition(batch.topicPartition);
            }
        }


        Map<Node, RecordAccumulator.GetAddressRequestInfo> getAddressRequestInfoMap = new HashMap<>();
        // rdma batches
        Map<Integer, List<ProduceRDMAWriteRequest>> rdmabatches = this.accumulator.drainRdma(cluster, result.readyNodes,  this.maxRequestSize, now, getAddressRequestInfoMap);

        if (!getAddressRequestInfoMap.isEmpty()) {
            requestProduserAddressAsync(getAddressRequestInfoMap, now);
        }

        if (guaranteeMessageOrder) {
            // Mute all the partitions drained
            for (List<ProduceRDMAWriteRequest>  requests: rdmabatches.values()) {
                for (ProduceRDMAWriteRequest req : requests)
                    this.accumulator.mutePartition(req.getBatch().topicPartition);
            }
        }

        accumulator.resetNextBatchExpiryTime();
        List<ProducerBatch> expiredInflightBatches = getExpiredInflightBatches(now);
        List<ProducerBatch> expiredBatches = this.accumulator.expiredBatches(now);
        expiredBatches.addAll(expiredInflightBatches);

        // Reset the producer id if an expired batch has previously been sent to the broker. Also update the metrics
        // for expired batches. see the documentation of @TransactionState.resetProducerId to understand why
        // we need to reset the producer id here.
        if (!expiredBatches.isEmpty()) {
            log.trace("Expired {} batches in accumulator", expiredBatches.size());
        }
        for (ProducerBatch expiredBatch : expiredBatches) {
            String errorMessage = "Expiring " + expiredBatch.recordCount + " record(s) for " + expiredBatch.topicPartition
                + ":" + (now - expiredBatch.createdMs) + " ms has passed since batch creation";
            failBatch(expiredBatch, -1, NO_TIMESTAMP, new TimeoutException(errorMessage), false);
            if (transactionManager != null && expiredBatch.inRetry()) {
                // This ensures that no new batches are drained until the current in flight batches are fully resolved.
                transactionManager.markSequenceUnresolved(expiredBatch.topicPartition);
            }
        }
        sensors.updateProduceRequestMetrics(batches);

        // If we have any nodes that are ready to send + have sendable data, poll with 0 timeout so this can immediately
        // loop and try sending more data. Otherwise, the timeout will be the smaller value between next batch expiry
        // time, and the delay time for checking data availability. Note that the nodes may have data that isn't yet
        // sendable due to lingering, backing off, etc. This specifically does not include nodes with sendable data
        // that aren't ready to send since they would cause busy looping.
        long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
        pollTimeout = Math.min(pollTimeout, this.accumulator.nextExpiryTimeMs() - now);
        pollTimeout = Math.max(pollTimeout, 0);
        if (!result.readyNodes.isEmpty()) {
            log.trace("Nodes with data ready to send: {}", result.readyNodes);
            // if some partitions are already ready to be sent, the select time would be 0;
            // otherwise if some partition already has some data accumulated but not ready yet,
            // the select time will be the time difference between now and its linger expiry time;
            // otherwise the select time will be the time difference between now and the metadata expiry time;
            pollTimeout = 0;
        }

        sendProduceRequests(batches, now);

        if (pendingRdma != 0) {
            rdmaClient.poll(0, nowNanos);
        }

        sendRdmaProduceRequests(rdmabatches, nowNanos);

        return pollTimeout;
    }

    private void requestProduserAddressAsync(Map<Node, RecordAccumulator.GetAddressRequestInfo> getAddressRequestInfoMap, long now) {

        for (Map.Entry<Node, RecordAccumulator.GetAddressRequestInfo> entry : getAddressRequestInfoMap.entrySet()) {

            RecordAccumulator.GetAddressRequestInfo addrInfo = entry.getValue();

            List<TopicPartition> tplist = addrInfo.unknownRdmaTopicPartitions;
            List<TopicPartition> toUpdate = addrInfo.newFileRequired;

            RequestCompletionHandler callback = new RequestCompletionHandler() {
                public void onComplete(ClientResponse response) {
                    handleAddressResponse(response,  time.milliseconds());
                }
            };

            String nodeId = entry.getKey().idString();

            RDMAProduceAddressRequest.Builder requestBuilder = new RDMAProduceAddressRequest.Builder(acks, tplist, toUpdate, requestTimeoutMs);


            ClientRequest clientRequest = client.newClientRequest(nodeId, requestBuilder, now, true,
                    requestTimeoutMs, callback);
            client.send(clientRequest, now);

        }
    }


    private  Map<Node, List<TopicPartition>> groupGetAddressRequests(Cluster cluster, List<TopicPartition> tps) {

        final Map<Node, List<TopicPartition>> getAddressByNode = new HashMap<>();


        for (TopicPartition tp: tps) {

            Node node = cluster.leaderFor(tp);

            List<TopicPartition> topicData =
                    getAddressByNode.computeIfAbsent(node,  n -> new ArrayList<>());

            topicData.add(tp);
        }
        return getAddressByNode;

    }


    void handleAddressResponse(ClientResponse response, long now) {
        RDMAProduceAddressResponse lor = (RDMAProduceAddressResponse) response.responseBody();

        try {
            boolean connected = this.rdmaClient.connect(response.destination(), lor.rdmaHost(), lor.rdmaPort(), Optional.empty());
            if (connected) {
                System.out.println("Connected rdma");
            }
        } catch (Exception e) {

        }
        this.accumulator.updateAddresses(lor.responses());
    }


    private boolean maybeSendTransactionalRequest() {
        if (transactionManager.isCompleting() && accumulator.hasIncomplete()) {
            if (transactionManager.isAborting())
                accumulator.abortUndrainedBatches(new KafkaException("Failing batch since transaction was aborted"));
            // There may still be requests left which are being retried. Since we do not know whether they had
            // been successfully appended to the broker log, we must resend them until their final status is clear.
            // If they had been appended and we did not receive the error, then our sequence number would no longer
            // be correct which would lead to an OutOfSequenceException.
            if (!accumulator.flushInProgress())
                accumulator.beginFlush();
        }

        TransactionManager.TxnRequestHandler nextRequestHandler = transactionManager.nextRequestHandler(accumulator.hasIncomplete());
        if (nextRequestHandler == null)
            return false;

        AbstractRequest.Builder<?> requestBuilder = nextRequestHandler.requestBuilder();
        while (!forceClose) {
            Node targetNode = null;
            try {
                if (nextRequestHandler.needsCoordinator()) {
                    targetNode = transactionManager.coordinator(nextRequestHandler.coordinatorType());
                    if (targetNode == null) {
                        transactionManager.lookupCoordinator(nextRequestHandler);
                        break;
                    }
                    if (!NetworkClientUtils.awaitReady(client, targetNode, time, requestTimeoutMs)) {
                        transactionManager.lookupCoordinator(nextRequestHandler);
                        break;
                    }
                } else {
                    targetNode = awaitLeastLoadedNodeReady(requestTimeoutMs);
                }

                if (targetNode != null) {
                    if (nextRequestHandler.isRetry())
                        time.sleep(nextRequestHandler.retryBackoffMs());
                    long currentTimeMs = time.milliseconds();
                    ClientRequest clientRequest = client.newClientRequest(
                        targetNode.idString(), requestBuilder, currentTimeMs, true, requestTimeoutMs, nextRequestHandler);
                    log.debug("Sending transactional request {} to node {}", requestBuilder, targetNode);
                    client.send(clientRequest, currentTimeMs);
                    transactionManager.setInFlightCorrelationId(clientRequest.correlationId());
                    return true;
                }
            } catch (IOException e) {
                log.debug("Disconnect from {} while trying to send request {}. Going " +
                        "to back off and retry.", targetNode, requestBuilder, e);
                if (nextRequestHandler.needsCoordinator()) {
                    // We break here so that we pick up the FindCoordinator request immediately.
                    transactionManager.lookupCoordinator(nextRequestHandler);
                    break;
                }
            }
            time.sleep(retryBackoffMs);
            metadata.requestUpdate();
        }
        transactionManager.retry(nextRequestHandler);
        return true;
    }

    private void maybeAbortBatches(RuntimeException exception) {
        if (accumulator.hasIncomplete()) {
            log.error("Aborting producer batches due to fatal error", exception);
            accumulator.abortBatches(exception);
        }
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     */
    public void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        this.accumulator.close();
        this.running = false;
        this.wakeup();
    }

    /**
     * Closes the sender without sending out any pending messages.
     */
    public void forceClose() {
        this.forceClose = true;
        initiateClose();
    }

    private ClientResponse sendAndAwaitInitProducerIdRequest(Node node) throws IOException {
        String nodeId = node.idString();
        InitProducerIdRequest.Builder builder = new InitProducerIdRequest.Builder(null);
        ClientRequest request = client.newClientRequest(nodeId, builder, time.milliseconds(), true, requestTimeoutMs, null);
        return NetworkClientUtils.sendAndReceive(client, request, time);
    }

    private Node awaitLeastLoadedNodeReady(long remainingTimeMs) throws IOException {
        Node node = client.leastLoadedNode(time.milliseconds());
        if (node != null && NetworkClientUtils.awaitReady(client, node, time, remainingTimeMs)) {
            return node;
        }
        return null;
    }

    private void maybeWaitForProducerId() {
        while (!forceClose && !transactionManager.hasProducerId() && !transactionManager.hasError()) {
            Node node = null;
            try {
                node = awaitLeastLoadedNodeReady(requestTimeoutMs);
                if (node != null) {
                    ClientResponse response = sendAndAwaitInitProducerIdRequest(node);
                    InitProducerIdResponse initProducerIdResponse = (InitProducerIdResponse) response.responseBody();
                    Errors error = initProducerIdResponse.error();
                    if (error == Errors.NONE) {
                        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(
                                initProducerIdResponse.producerId(), initProducerIdResponse.epoch());
                        transactionManager.setProducerIdAndEpoch(producerIdAndEpoch);
                        return;
                    } else if (error.exception() instanceof RetriableException) {
                        log.debug("Retriable error from InitProducerId response", error.message());
                    } else {
                        transactionManager.transitionToFatalError(error.exception());
                        break;
                    }
                } else {
                    log.debug("Could not find an available broker to send InitProducerIdRequest to. Will back off and retry.");
                }
            } catch (UnsupportedVersionException e) {
                transactionManager.transitionToFatalError(e);
                break;
            } catch (IOException e) {
                log.debug("Broker {} disconnected while awaiting InitProducerId response", node, e);
            }
            log.trace("Retry InitProducerIdRequest in {}ms.", retryBackoffMs);
            time.sleep(retryBackoffMs);
            metadata.requestUpdate();
        }
    }

    /**
     * Handle a produce response
     */
    private void handleProduceResponse(ClientResponse response, Map<TopicPartition, ProducerBatch> batches, long now) {
        RequestHeader requestHeader = response.requestHeader();
        long receivedTimeMs = response.receivedTimeMs();
        int correlationId = requestHeader.correlationId();
        if (response.wasDisconnected()) {
            log.trace("Cancelled request with header {} due to node {} being disconnected",
                requestHeader, response.destination());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NETWORK_EXCEPTION), correlationId, now, 0L);
        } else if (response.versionMismatch() != null) {
            log.warn("Cancelled request {} due to a version mismatch with node {}",
                    response, response.destination(), response.versionMismatch());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.UNSUPPORTED_VERSION), correlationId, now, 0L);
        } else {
            log.trace("Received produce response from node {} with correlation id {}", response.destination(), correlationId);
            // if we have a response, parse it
            if (response.hasResponse()) {
                ProduceResponse produceResponse = (ProduceResponse) response.responseBody();
                for (Map.Entry<TopicPartition, ProduceResponse.PartitionResponse> entry : produceResponse.responses().entrySet()) {
                    TopicPartition tp = entry.getKey();
                    ProduceResponse.PartitionResponse partResp = entry.getValue();
                    ProducerBatch batch = batches.get(tp);
                    completeBatch(batch, partResp, correlationId, now, receivedTimeMs + produceResponse.throttleTimeMs());
                }
                this.sensors.recordLatency(response.destination(), response.requestLatencyMs());
            } else {
                // this is the acks = 0 case, just complete all requests
                for (ProducerBatch batch : batches.values()) {
                    completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NONE), correlationId, now, 0L);
                }
            }
        }
    }

    private void handleRdmaProduceResponse(ClientRDMAResponse response,  ProducerBatch  batch, long now) {

        int correlationId =  -1;
        long receivedTimeMs = response.receivedTimeMs;

        log.trace("Received produce response from node {} with correlation id {}", response.destination(), correlationId);
        // if we have a response, parse it
        if (response.hasResponse()) {

            // get special request
            ProduceRDMAWriteRequest request = (ProduceRDMAWriteRequest) response.getRequest();
            ProduceResponse.PartitionResponse partResp = new ProduceResponse.PartitionResponse(Errors.NONE, request.baseOffset, NO_TIMESTAMP, ProduceResponse.INVALID_OFFSET);

            completeBatch(batch, partResp, correlationId, now, receivedTimeMs);

            this.sensors.recordLatency(response.destination(), response.requestLatencyNanos() / 1000000L);
        } else {
            // this is the acks = 0 case, just complete all requests
            completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NONE), correlationId, now, 0L);
        }

    }


    /**
     * Complete or retry the given batch of records.
     *
     * @param batch The record batch
     * @param response The produce response
     * @param correlationId The correlation id for the request
     * @param now The current POSIX timestamp in milliseconds
     */
    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response, long correlationId,
                               long now, long throttleUntilTimeMs) {
        Errors error = response.error;

        if (error == Errors.MESSAGE_TOO_LARGE && batch.recordCount > 1 && !batch.isDone() &&
                (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || batch.isCompressed())) {
            // If the batch is too large, we split the batch and send the split batches again. We do not decrement
            // the retry attempts in this case.
            log.warn(
                "Got error produce response in correlation id {} on topic-partition {}, splitting and retrying ({} attempts left). Error: {}",
                correlationId,
                batch.topicPartition,
                this.retries - batch.attempts(),
                error);
            if (transactionManager != null)
                transactionManager.removeInFlightBatch(batch);
            this.accumulator.splitAndReenqueue(batch);
            this.accumulator.deallocate(batch);
            this.sensors.recordBatchSplit();
        } else if (error != Errors.NONE) {
            if (canRetry(batch, response, now)) {
                log.warn(
                    "Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                    correlationId,
                    batch.topicPartition,
                    this.retries - batch.attempts() - 1,
                    error);
                if (transactionManager == null) {
                    reenqueueBatch(batch, now);
                } else if (transactionManager.hasProducerIdAndEpoch(batch.producerId(), batch.producerEpoch())) {
                    // If idempotence is enabled only retry the request if the current producer id is the same as
                    // the producer id of the batch.
                    log.debug("Retrying batch to topic-partition {}. ProducerId: {}; Sequence number : {}",
                            batch.topicPartition, batch.producerId(), batch.baseSequence());
                    reenqueueBatch(batch, now);
                } else {
                    failBatch(batch, response, new OutOfOrderSequenceException("Attempted to retry sending a " +
                            "batch but the producer id changed from " + batch.producerId() + " to " +
                            transactionManager.producerIdAndEpoch().producerId + " in the mean time. This batch will be dropped."), false);
                }
            } else if (error == Errors.DUPLICATE_SEQUENCE_NUMBER) {
                // If we have received a duplicate sequence error, it means that the sequence number has advanced beyond
                // the sequence of the current batch, and we haven't retained batch metadata on the broker to return
                // the correct offset and timestamp.
                //
                // The only thing we can do is to return success to the user and not return a valid offset and timestamp.
                completeBatch(batch, response);
            } else {
                final RuntimeException exception;
                if (error == Errors.TOPIC_AUTHORIZATION_FAILED)
                    exception = new TopicAuthorizationException(batch.topicPartition.topic());
                else if (error == Errors.CLUSTER_AUTHORIZATION_FAILED)
                    exception = new ClusterAuthorizationException("The producer is not authorized to do idempotent sends");
                else
                    exception = error.exception();
                // tell the user the result of their request. We only adjust sequence numbers if the batch didn't exhaust
                // its retries -- if it did, we don't know whether the sequence number was accepted or not, and
                // thus it is not safe to reassign the sequence.
                failBatch(batch, response, exception, batch.attempts() < this.retries);
            }
            if (error.exception() instanceof InvalidMetadataException) {
                if (error.exception() instanceof UnknownTopicOrPartitionException) {
                    log.warn("Received unknown topic or partition error in produce request on partition {}. The " +
                            "topic-partition may not exist or the user may not have Describe access to it",
                        batch.topicPartition);
                } else {
                    log.warn("Received invalid metadata error in produce request on partition {} due to {}. Going " +
                            "to request metadata update now", batch.topicPartition, error.exception().toString());
                }
                metadata.requestUpdate();
            }
        } else {
            completeBatch(batch, response);
        }

        // Unmute the completed partition.
        if (guaranteeMessageOrder)
            this.accumulator.unmutePartition(batch.topicPartition, throttleUntilTimeMs);
    }

    private void reenqueueBatch(ProducerBatch batch, long currentTimeMs) {
        this.accumulator.reenqueue(batch, currentTimeMs);
        maybeRemoveFromInflightBatches(batch);
        this.sensors.recordRetries(batch.topicPartition.topic(), batch.recordCount);
    }

    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
        if (transactionManager != null) {
            if (transactionManager.hasProducerIdAndEpoch(batch.producerId(), batch.producerEpoch())) {
                transactionManager
                    .maybeUpdateLastAckedSequence(batch.topicPartition, batch.baseSequence() + batch.recordCount - 1);
                log.debug("ProducerId: {}; Set last ack'd sequence number for topic-partition {} to {}",
                    batch.producerId(),
                    batch.topicPartition,
                    transactionManager.lastAckedSequence(batch.topicPartition));
            }
            transactionManager.updateLastAckedOffset(response, batch);
            transactionManager.removeInFlightBatch(batch);
        }

        if (batch.done(response.baseOffset, response.logAppendTime, null)) {
            maybeRemoveFromInflightBatches(batch);
            this.accumulator.deallocate(batch);
        }
    }

    private void failBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response, RuntimeException exception,
                           boolean adjustSequenceNumbers) {
        failBatch(batch, response.baseOffset, response.logAppendTime, exception, adjustSequenceNumbers);
    }

    private void failBatch(ProducerBatch batch, long baseOffset, long logAppendTime, RuntimeException exception,
        boolean adjustSequenceNumbers) {
        if (transactionManager != null) {
            if (exception instanceof OutOfOrderSequenceException
                    && !transactionManager.isTransactional()
                    && transactionManager.hasProducerId(batch.producerId())) {
                log.error("The broker returned {} for topic-partition " +
                            "{} at offset {}. This indicates data loss on the broker, and should be investigated.",
                        exception, batch.topicPartition, baseOffset);

                // Reset the transaction state since we have hit an irrecoverable exception and cannot make any guarantees
                // about the previously committed message. Note that this will discard the producer id and sequence
                // numbers for all existing partitions.
                transactionManager.resetProducerId();
            } else if (exception instanceof ClusterAuthorizationException
                    || exception instanceof TransactionalIdAuthorizationException
                    || exception instanceof ProducerFencedException
                    || exception instanceof UnsupportedVersionException) {
                transactionManager.transitionToFatalError(exception);
            } else if (transactionManager.isTransactional()) {
                transactionManager.transitionToAbortableError(exception);
            }
            transactionManager.removeInFlightBatch(batch);
            if (adjustSequenceNumbers)
                transactionManager.adjustSequencesDueToFailedBatch(batch);
        }

        this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);

        if (batch.done(baseOffset, logAppendTime, exception)) {
            maybeRemoveFromInflightBatches(batch);
            this.accumulator.deallocate(batch);
        }
    }

    /**
     * We can retry a send if the error is transient and the number of attempts taken is fewer than the maximum allowed.
     * We can also retry OutOfOrderSequence exceptions for future batches, since if the first batch has failed, the
     * future batches are certain to fail with an OutOfOrderSequence exception.
     */
    private boolean canRetry(ProducerBatch batch, ProduceResponse.PartitionResponse response, long now) {
        return !batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now) &&
            batch.attempts() < this.retries &&
            !batch.isDone() &&
            ((response.error.exception() instanceof RetriableException) ||
                (transactionManager != null && transactionManager.canRetry(response, batch)));
    }

    /**
     * Transfer the record batches into a list of produce requests on a per-node basis
     */
    private void sendProduceRequests(Map<Integer, List<ProducerBatch>> collated, long now) {
        for (Map.Entry<Integer, List<ProducerBatch>> entry : collated.entrySet())
            sendProduceRequest(now, entry.getKey(), acks, requestTimeoutMs, entry.getValue());
    }

    /**
     * Transfer the record batches into a list of produce requests on a per-node basis
     */
    private void sendRdmaProduceRequests(Map<Integer, List<ProduceRDMAWriteRequest>> collated, long nowNanos) {
        for (Map.Entry<Integer, List<ProduceRDMAWriteRequest>> entry : collated.entrySet())
            sendRdmaProduceRequest(nowNanos, entry.getKey(), entry.getValue());
    }

    /**
     * Create a produce request from the given record batches
     */
    private void sendProduceRequest(long now, int destination, short acks, int timeout, List<ProducerBatch> batches) {
        if (batches.isEmpty())
            return;

        Map<TopicPartition, MemoryRecords> produceRecordsByPartition = new HashMap<>(batches.size());
        final Map<TopicPartition, ProducerBatch> recordsByPartition = new HashMap<>(batches.size());

        // find the minimum magic version used when creating the record sets
        byte minUsedMagic = apiVersions.maxUsableProduceMagic();
        for (ProducerBatch batch : batches) {
            if (batch.magic() < minUsedMagic)
                minUsedMagic = batch.magic();
        }

        for (ProducerBatch batch : batches) {
            TopicPartition tp = batch.topicPartition;
            MemoryRecords records = batch.records();

            // down convert if necessary to the minimum magic used. In general, there can be a delay between the time
            // that the producer starts building the batch and the time that we send the request, and we may have
            // chosen the message format based on out-dated metadata. In the worst case, we optimistically chose to use
            // the new message format, but found that the broker didn't support it, so we need to down-convert on the
            // client before sending. This is intended to handle edge cases around cluster upgrades where brokers may
            // not all support the same message format version. For example, if a partition migrates from a broker
            // which is supporting the new magic version to one which doesn't, then we will need to convert.
            if (!records.hasMatchingMagic(minUsedMagic))
                records = batch.records().downConvert(minUsedMagic, 0, time).records();
            produceRecordsByPartition.put(tp, records);
            recordsByPartition.put(tp, batch);
        }

        String transactionalId = null;
        if (transactionManager != null && transactionManager.isTransactional()) {
            transactionalId = transactionManager.transactionalId();
        }
        ProduceRequest.Builder requestBuilder = ProduceRequest.Builder.forMagic(minUsedMagic, acks, timeout,
                produceRecordsByPartition, transactionalId);
        RequestCompletionHandler callback = new RequestCompletionHandler() {
            public void onComplete(ClientResponse response) {
                handleProduceResponse(response, recordsByPartition, time.milliseconds());
            }
        };

        String nodeId = Integer.toString(destination);

        ClientRequest clientRequest = client.newClientRequest(nodeId, requestBuilder, now, acks != 0,
                requestTimeoutMs, callback);
        client.send(clientRequest, now);
        log.trace("Sent produce request to {}: {}", nodeId, requestBuilder);
    }

    private void sendRdmaProduceRequest(long nowNanos, int destination, List<ProduceRDMAWriteRequest> requests) {
        // todo fix now
        if (requests.isEmpty())
            return;

       /* // find the minimum magic version used when creating the record sets
        byte minUsedMagic = apiVersions.maxUsableProduceMagic();
        for (ProduceRDMAWriteRequest request : requests) {
            ProducerBatch batch = request.getBatch();
            if (batch.magic() < minUsedMagic)
                minUsedMagic = batch.magic();
        }


        String transactionalId = null;
        if (transactionManager != null && transactionManager.isTransactional()) {
            transactionalId = transactionManager.transactionalId();
        }*/

        String nodeId = Integer.toString(destination);

        for (ProduceRDMAWriteRequest request : requests) {
            ProducerBatch batch = request.getBatch();

        /*    if (!batch.records().hasMatchingMagic(minUsedMagic)){
                // request.updateBuffer(batch.records().downConvert(minUsedMagic, 0, time).records() );
                log.info("Unexpected conversion!  {}: {}", nodeId, requests.size());
            }*/

            RDMARequestCompletionHandler callback = new RDMARequestCompletionHandler() {
                public void onComplete(ClientRDMAResponse response) {
                    pendingRdma--;
                    handleRdmaProduceResponse(response, batch, time.milliseconds());
                }
            };

            boolean completeOnSend = acks == 0; // fire and forget option.
            boolean completeOnRecv = acks != 0;

            ClientRDMARequest clientRequest =  rdmaClient.newClientRdmaRequest(nodeId, request, nowNanos, requestTimeoutMs, completeOnSend, completeOnRecv, callback);

            rdmaClient.send(clientRequest, nowNanos);
            pendingRdma++;
        }

       // log.trace("Rdma Sent produce request to {}: {}", nodeId, requests.size());
    }


    /**
     * Wake up the selector associated with this send thread
     */
    public void wakeup() {
        this.client.wakeup();
    }

    public static Sensor throttleTimeSensor(SenderMetricsRegistry metrics) {
        Sensor produceThrottleTimeSensor = metrics.sensor("produce-throttle-time");
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeAvg, new Avg());
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeMax, new Max());
        return produceThrottleTimeSensor;
    }

    /**
     * A collection of sensors for the sender
     */
    private static class SenderMetrics {
        public final Sensor retrySensor;
        public final Sensor errorSensor;
        public final Sensor queueTimeSensor;
        public final Sensor requestTimeSensor;
        public final Sensor recordsPerRequestSensor;
        public final Sensor batchSizeSensor;
        public final Sensor compressionRateSensor;
        public final Sensor maxRecordSizeSensor;
        public final Sensor batchSplitSensor;
        private final SenderMetricsRegistry metrics;
        private final Time time;

        public SenderMetrics(SenderMetricsRegistry metrics, Metadata metadata, KafkaClient client, Time time) {
            this.metrics = metrics;
            this.time = time;

            this.batchSizeSensor = metrics.sensor("batch-size");
            this.batchSizeSensor.add(metrics.batchSizeAvg, new Avg());
            this.batchSizeSensor.add(metrics.batchSizeMax, new Max());

            this.compressionRateSensor = metrics.sensor("compression-rate");
            this.compressionRateSensor.add(metrics.compressionRateAvg, new Avg());

            this.queueTimeSensor = metrics.sensor("queue-time");
            this.queueTimeSensor.add(metrics.recordQueueTimeAvg, new Avg());
            this.queueTimeSensor.add(metrics.recordQueueTimeMax, new Max());

            this.requestTimeSensor = metrics.sensor("request-time");
            this.requestTimeSensor.add(metrics.requestLatencyAvg, new Avg());
            this.requestTimeSensor.add(metrics.requestLatencyMax, new Max());

            this.recordsPerRequestSensor = metrics.sensor("records-per-request");
            this.recordsPerRequestSensor.add(new Meter(metrics.recordSendRate, metrics.recordSendTotal));
            this.recordsPerRequestSensor.add(metrics.recordsPerRequestAvg, new Avg());

            this.retrySensor = metrics.sensor("record-retries");
            this.retrySensor.add(new Meter(metrics.recordRetryRate, metrics.recordRetryTotal));

            this.errorSensor = metrics.sensor("errors");
            this.errorSensor.add(new Meter(metrics.recordErrorRate, metrics.recordErrorTotal));

            this.maxRecordSizeSensor = metrics.sensor("record-size");
            this.maxRecordSizeSensor.add(metrics.recordSizeMax, new Max());
            this.maxRecordSizeSensor.add(metrics.recordSizeAvg, new Avg());

            this.metrics.addMetric(metrics.requestsInFlight, (config, now) -> client.inFlightRequestCount());
            this.metrics.addMetric(metrics.metadataAge,
                (config, now) -> (now - metadata.lastSuccessfulUpdate()) / 1000.0);

            this.batchSplitSensor = metrics.sensor("batch-split-rate");
            this.batchSplitSensor.add(new Meter(metrics.batchSplitRate, metrics.batchSplitTotal));
        }

        private void maybeRegisterTopicMetrics(String topic) {
            // if one sensor of the metrics has been registered for the topic,
            // then all other sensors should have been registered; and vice versa
            String topicRecordsCountName = "topic." + topic + ".records-per-batch";
            Sensor topicRecordCount = this.metrics.getSensor(topicRecordsCountName);
            if (topicRecordCount == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic);

                topicRecordCount = this.metrics.sensor(topicRecordsCountName);
                MetricName rateMetricName = this.metrics.topicRecordSendRate(metricTags);
                MetricName totalMetricName = this.metrics.topicRecordSendTotal(metricTags);
                topicRecordCount.add(new Meter(rateMetricName, totalMetricName));

                String topicByteRateName = "topic." + topic + ".bytes";
                Sensor topicByteRate = this.metrics.sensor(topicByteRateName);
                rateMetricName = this.metrics.topicByteRate(metricTags);
                totalMetricName = this.metrics.topicByteTotal(metricTags);
                topicByteRate.add(new Meter(rateMetricName, totalMetricName));

                String topicCompressionRateName = "topic." + topic + ".compression-rate";
                Sensor topicCompressionRate = this.metrics.sensor(topicCompressionRateName);
                MetricName m = this.metrics.topicCompressionRate(metricTags);
                topicCompressionRate.add(m, new Avg());

                String topicRetryName = "topic." + topic + ".record-retries";
                Sensor topicRetrySensor = this.metrics.sensor(topicRetryName);
                rateMetricName = this.metrics.topicRecordRetryRate(metricTags);
                totalMetricName = this.metrics.topicRecordRetryTotal(metricTags);
                topicRetrySensor.add(new Meter(rateMetricName, totalMetricName));

                String topicErrorName = "topic." + topic + ".record-errors";
                Sensor topicErrorSensor = this.metrics.sensor(topicErrorName);
                rateMetricName = this.metrics.topicRecordErrorRate(metricTags);
                totalMetricName = this.metrics.topicRecordErrorTotal(metricTags);
                topicErrorSensor.add(new Meter(rateMetricName, totalMetricName));
            }
        }

        public void updateProduceRequestMetrics(Map<Integer, List<ProducerBatch>> batches) {
            long now = time.milliseconds();
            for (List<ProducerBatch> nodeBatch : batches.values()) {
                int records = 0;
                for (ProducerBatch batch : nodeBatch) {
                    // register all per-topic metrics at once
                    String topic = batch.topicPartition.topic();
                    maybeRegisterTopicMetrics(topic);

                    // per-topic record send rate
                    String topicRecordsCountName = "topic." + topic + ".records-per-batch";
                    Sensor topicRecordCount = Utils.notNull(this.metrics.getSensor(topicRecordsCountName));
                    topicRecordCount.record(batch.recordCount);

                    // per-topic bytes send rate
                    String topicByteRateName = "topic." + topic + ".bytes";
                    Sensor topicByteRate = Utils.notNull(this.metrics.getSensor(topicByteRateName));
                    topicByteRate.record(batch.estimatedSizeInBytes());

                    // per-topic compression rate
                    String topicCompressionRateName = "topic." + topic + ".compression-rate";
                    Sensor topicCompressionRate = Utils.notNull(this.metrics.getSensor(topicCompressionRateName));
                    topicCompressionRate.record(batch.compressionRatio());

                    // global metrics
                    this.batchSizeSensor.record(batch.estimatedSizeInBytes(), now);
                    this.queueTimeSensor.record(batch.queueTimeMs(), now);
                    this.compressionRateSensor.record(batch.compressionRatio());
                    this.maxRecordSizeSensor.record(batch.maxRecordSize, now);
                    records += batch.recordCount;
                }
                this.recordsPerRequestSensor.record(records, now);
            }
        }

        public void recordRetries(String topic, int count) {
            long now = time.milliseconds();
            this.retrySensor.record(count, now);
            String topicRetryName = "topic." + topic + ".record-retries";
            Sensor topicRetrySensor = this.metrics.getSensor(topicRetryName);
            if (topicRetrySensor != null)
                topicRetrySensor.record(count, now);
        }

        public void recordErrors(String topic, int count) {
            long now = time.milliseconds();
            this.errorSensor.record(count, now);
            String topicErrorName = "topic." + topic + ".record-errors";
            Sensor topicErrorSensor = this.metrics.getSensor(topicErrorName);
            if (topicErrorSensor != null)
                topicErrorSensor.record(count, now);
        }

        public void recordLatency(String node, long latency) {
            long now = time.milliseconds();
            this.requestTimeSensor.record(latency, now);
            if (!node.isEmpty()) {
                String nodeTimeName = "node-" + node + ".latency";
                Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
                if (nodeRequestTime != null)
                    nodeRequestTime.record(latency, now);
            }
        }

        void recordBatchSplit() {
            this.batchSplitSensor.record();
        }
    }

}
