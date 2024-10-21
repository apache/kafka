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
package org.apache.kafka.server;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AllocateProducerIdsRequest;
import org.apache.kafka.common.requests.AllocateProducerIdsResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.ProducerIdsBlock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class RPCProducerIdManager extends ProducerIdManager {

    private static final Logger log = LoggerFactory.getLogger(RPCProducerIdManager.class);
    private final String logPrefix;

    private final int brokerId;
    private final Time time;
    private final Supplier<Long> brokerEpochSupplier;
    private final NodeToControllerChannelManager controllerChannel;

    // Visible for testing
    public AtomicReference<ProducerIdsBlock> nextProducerIdBlock = new AtomicReference<>(null);
    private final AtomicReference<ProducerIdsBlock> currentProducerIdBlock = new AtomicReference<>(ProducerIdsBlock.EMPTY);
    private final AtomicBoolean requestInFlight = new AtomicBoolean(false);
    private final AtomicLong backoffDeadlineMs = new AtomicLong(NO_RETRY);

    public RPCProducerIdManager(int brokerId,
                                Time time,
                                Supplier<Long> brokerEpochSupplier,
                                NodeToControllerChannelManager controllerChannel
    ) {
        this.brokerId = brokerId;
        this.time = time;
        this.brokerEpochSupplier = brokerEpochSupplier;
        this.controllerChannel = controllerChannel;
        this.logPrefix = "[RPC ProducerId Manager " + brokerId + "]: ";
    }


    @Override
    public Long generateProducerId() throws Exception {
        Long result = null;
        var iteration = 0;
        while (result == null) {
            var claimNextId = currentProducerIdBlock.get().claimNextId();
            if (claimNextId.isPresent()) {
                var nextProducerId = claimNextId.get();
                // Check if we need to prefetch the next block
                long prefetchTarget = currentProducerIdBlock.get().firstProducerId() +
                        (long) (currentProducerIdBlock.get().size() * PID_PREFETCH_THRESHOLD);
                if (nextProducerId == prefetchTarget) {
                    maybePrefetchNextBlock();
                }
                result = nextProducerId;
            } else {
                // Check the next block if current block is full
                var block = nextProducerIdBlock.getAndSet(null);
                if (block == null) {
                    // Return COORDINATOR_LOAD_IN_PROGRESS rather than REQUEST_TIMED_OUT since older clients treat the error as fatal
                    // when it should be retriable like COORDINATOR_LOAD_IN_PROGRESS.
                    maybeRequestNextBlock();
                    throw  Errors.COORDINATOR_LOAD_IN_PROGRESS.exception("Producer ID block is full. Waiting for next block");
                } else {
                    currentProducerIdBlock.set(block);
                    requestInFlight.set(false);
                    iteration++;
                }
            }
            if (iteration == ITERATION_LIMIT) {
                throw Errors.COORDINATOR_LOAD_IN_PROGRESS.exception("Producer ID block is full. Waiting for next block");
            }
        }
        return result;
    }

    @Override
    public boolean hasValidBlock() {
        return nextProducerIdBlock.get() != null;
    }

    private void maybePrefetchNextBlock() {
        var retryTimestamp = backoffDeadlineMs.get();
        if (retryTimestamp == NO_RETRY || time.milliseconds() >= retryTimestamp) {
            // Send a request only if we reached the retry deadline, or if no deadline was set.
            if (nextProducerIdBlock.get() == null &&
                    requestInFlight.compareAndSet(false, true)) {
                backoffDeadlineMs.set(NO_RETRY);
                sendRequest();
                // Reset backoff after a successful send.
                backoffDeadlineMs.set(NO_RETRY);
            }
        }
    }

    private void maybeRequestNextBlock() {
        var retryTimestamp = backoffDeadlineMs.get();
        if (retryTimestamp == NO_RETRY || time.milliseconds() >= retryTimestamp) {
            // Send a request only if we reached the retry deadline, or if no deadline was set.
            if (nextProducerIdBlock.get() == null &&
                    requestInFlight.compareAndSet(false, true)) {
                sendRequest();
                // Reset backoff after a successful send.
                backoffDeadlineMs.set(NO_RETRY);
            }
        }
    }

    protected void sendRequest() {
        var message = new AllocateProducerIdsRequestData()
                .setBrokerEpoch(brokerEpochSupplier.get())
                .setBrokerId(brokerId);
        var request = new AllocateProducerIdsRequest.Builder(message);
        controllerChannel.sendRequest(request, new ControllerRequestCompletionHandler() {

            @Override
            public void onComplete(ClientResponse response) {
                if (response.responseBody() instanceof AllocateProducerIdsResponse) {
                    handleAllocateProducerIdsResponse((AllocateProducerIdsResponse) response.responseBody());
                }
            }

            @Override
            public void onTimeout() {
                handleTimeout();
            }
        });
    }

    protected void handleAllocateProducerIdsResponse(AllocateProducerIdsResponse response) {
        var data = response.data();
        var successfulResponse = false;
        var errors = Errors.forCode(data.errorCode());
        switch (errors) {
            case NONE:
                log.debug("{} Got next producer ID block from controller {}", logPrefix, data);
                successfulResponse = sanityCheckResponse(data);
                break;
            case STALE_BROKER_EPOCH:
                log.warn("{} Our broker currentBlockCount was stale, trying again.", logPrefix);
                break;
            case BROKER_ID_NOT_REGISTERED:
                log.warn("{} Our broker ID is not yet known by the controller, trying again.", logPrefix);
                break;
            default :
                log.error("{} Received error code {} from the controller.", logPrefix, errors);
        }
        if (!successfulResponse) {
            // There is no need to compare and set because only one thread
            // handles the AllocateProducerIds response.
            backoffDeadlineMs.set(time.milliseconds() + RETRY_BACKOFF_MS);
            requestInFlight.set(false);
        }
    }

    private boolean sanityCheckResponse(AllocateProducerIdsResponseData data) {
        if (data.producerIdStart() < currentProducerIdBlock.get().lastProducerId()) {
            log.error("{} Producer ID block is not monotonic with current block: current={} response={}", logPrefix, currentProducerIdBlock.get(), data);
        } else if (data.producerIdStart() < 0 || data.producerIdLen() < 0 || data.producerIdStart() > Long.MAX_VALUE - data.producerIdLen()) {
            log.error("{} Producer ID block includes invalid ID range: {}", logPrefix, data);
        } else {
            nextProducerIdBlock.set(new ProducerIdsBlock(brokerId, data.producerIdStart(), data.producerIdLen()));
            return true;
        }
        return false;
    }

    private void handleTimeout() {
        log.warn("{} Timed out when requesting AllocateProducerIds from the controller.", logPrefix);
        requestInFlight.set(false);
    }
}
