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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AllocateProducerIdsRequest;
import org.apache.kafka.common.requests.AllocateProducerIdsResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.common.ProducerIdsBlock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * RPCProducerIdManager allocates producer id blocks asynchronously and will immediately fail requests
 * for producers to retry if it does not have an available producer id and is waiting on a new block.
 */
public class RPCProducerIdManager implements ProducerIdManager {

    static final int RETRY_BACKOFF_MS = 50;
    // Once we reach this percentage of PIDs consumed from the current block, trigger a fetch of the next block
    private static final double PID_PREFETCH_THRESHOLD = 0.90;
    private static final int ITERATION_LIMIT = 3;
    private static final long NO_RETRY = -1L;

    private static final Logger log = LoggerFactory.getLogger(RPCProducerIdManager.class);
    private final String logPrefix;

    private final int brokerId;
    private final Time time;
    private final Supplier<Long> brokerEpochSupplier;
    private final NodeToControllerChannelManager controllerChannel;

    // Visible for testing
    final AtomicReference<ProducerIdsBlock> nextProducerIdBlock = new AtomicReference<>(null);
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
    public long generateProducerId() {
        var iteration = 0;
        while (iteration <= ITERATION_LIMIT) {
            var claimNextId = currentProducerIdBlock.get().claimNextId();
            if (claimNextId.isPresent()) {
                long nextProducerId = claimNextId.get();
                // Check if we need to prefetch the next block
                var prefetchTarget = currentProducerIdBlock.get().firstProducerId() +
                        (long) (currentProducerIdBlock.get().size() * PID_PREFETCH_THRESHOLD);
                if (nextProducerId == prefetchTarget) {
                    maybeRequestNextBlock();
                }
                return nextProducerId;
            } else {
                // Check the next block if current block is full
                var block = nextProducerIdBlock.getAndSet(null);
                if (block == null) {
                    // Return COORDINATOR_LOAD_IN_PROGRESS rather than REQUEST_TIMED_OUT since older clients treat the error as fatal
                    // when it should be retriable like COORDINATOR_LOAD_IN_PROGRESS.
                    maybeRequestNextBlock();
                    throw Errors.COORDINATOR_LOAD_IN_PROGRESS.exception("Producer ID block is full. Waiting for next block");
                } else {
                    currentProducerIdBlock.set(block);
                    requestInFlight.set(false);
                    iteration++;
                }
            }
        }
        throw Errors.COORDINATOR_LOAD_IN_PROGRESS.exception("Producer ID block is full. Waiting for next block");
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
        log.debug("{} Requesting next Producer ID block", logPrefix);
        controllerChannel.sendRequest(request, new ControllerRequestCompletionHandler() {

            @Override
            public void onComplete(ClientResponse response) {
                if (response.responseBody() instanceof AllocateProducerIdsResponse) {
                    handleAllocateProducerIdsResponse((AllocateProducerIdsResponse) response.responseBody());
                }
            }

            @Override
            public void onTimeout() {
                log.warn("{} Timed out when requesting AllocateProducerIds from the controller.", logPrefix);
                requestInFlight.set(false);
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
}
