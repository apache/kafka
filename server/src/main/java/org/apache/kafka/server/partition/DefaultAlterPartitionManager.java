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

package org.apache.kafka.server.partition;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.errors.OperationNotAttemptedException;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AlterPartitionRequest;
import org.apache.kafka.common.requests.AlterPartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.server.ControllerInformation;
import org.apache.kafka.server.NodeToControllerChannelManagerImpl;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.common.TopicIdPartition;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.util.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DefaultAlterPartitionManager implements AlterPartitionManager {
    private static final Logger log = LoggerFactory.getLogger(DefaultAlterPartitionManager.class);
    private final NodeToControllerChannelManager controllerChannelManager;
    private final Scheduler scheduler;
    private final int brokerId;
    private final Supplier<Long> brokerEpochSupplier;

    // Used to allow only one pending ISR update per partition (visible for testing)
    final ConcurrentHashMap<TopicIdPartition, AlterPartitionItem> unsentIsrUpdates = new ConcurrentHashMap<>();

    // Used to allow only one in-flight request at a time
    private final AtomicBoolean inflightRequest = new AtomicBoolean(false);

    public DefaultAlterPartitionManager(NodeToControllerChannelManager controllerChannelManager, Scheduler scheduler, int brokerId, Supplier<Long> brokerEpochSupplier) {
        this.controllerChannelManager = controllerChannelManager;
        this.scheduler = scheduler;
        this.brokerId = brokerId;
        this.brokerEpochSupplier = brokerEpochSupplier;
    }

    public static DefaultAlterPartitionManager create(AbstractKafkaConfig config,
                                                      Scheduler scheduler,
                                                      Supplier<ControllerInformation> controllerNodeProvider,
                                                      Time time,
                                                      Metrics metrics,
                                                      String threadNamePrefix,
                                                      Supplier<Long> brokerEpochSupplier) {
        NodeToControllerChannelManager channelManager = new NodeToControllerChannelManagerImpl(
                controllerNodeProvider,
                time,
                metrics,
                config,
                "alter-partition",
                threadNamePrefix,
                Long.MAX_VALUE
        );

        return new DefaultAlterPartitionManager(channelManager, scheduler, config.brokerId(), brokerEpochSupplier);
    }

    @Override
    public void start() {
        controllerChannelManager.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        controllerChannelManager.shutdown();
    }

    @Override
    public CompletableFuture<LeaderAndIsr> submit(TopicIdPartition topicIdPartition,
                                                  LeaderAndIsr leaderAndIsr) {
        CompletableFuture<LeaderAndIsr> future = new CompletableFuture<>();
        AlterPartitionItem alterPartitionItem = new AlterPartitionItem(topicIdPartition, leaderAndIsr, future);
        boolean enqueued = unsentIsrUpdates.putIfAbsent(alterPartitionItem.topicIdPartition(), alterPartitionItem) == null;
        if (enqueued) {
            maybePropagateIsrChanges();
        } else {
            future.completeExceptionally(new OperationNotAttemptedException(String.format(
                    "Failed to enqueue ISR change state %s for partition %s", leaderAndIsr, topicIdPartition)));
        }
        return future;
    }

    void maybePropagateIsrChanges() {
        // Send all pending items if there is not already a request in-flight.
        if (!unsentIsrUpdates.isEmpty() && inflightRequest.compareAndSet(false, true)) {
            // Copy current unsent ISRs but don't remove from the map, they get cleared in the response handler
            List<AlterPartitionItem> inflightAlterPartitionItems = new ArrayList<>(unsentIsrUpdates.values());
            sendRequest(inflightAlterPartitionItems);
        }
    }

    void clearInFlightRequest() {
        if (!inflightRequest.compareAndSet(true, false)) {
            log.warn("Attempting to clear AlterPartition in-flight flag when no apparent request is in-flight");
        }
    }

    private void sendRequest(List<AlterPartitionItem> inflightAlterPartitionItems) {
        long brokerEpoch = brokerEpochSupplier.get();
        AlterPartitionRequest.Builder request = buildRequest(inflightAlterPartitionItems, brokerEpoch);
        log.debug("Sending AlterPartition to controller {}", request);

        // We will not time out AlterPartition request, instead letting it retry indefinitely
        // until a response is received, or a new LeaderAndIsr overwrites the existing isrState
        // which causes the response for those partitions to be ignored.
        controllerChannelManager.sendRequest(request,
                new ControllerRequestCompletionHandler() {
                    @Override
                    public void onComplete(ClientResponse response) {
                        log.debug("Received AlterPartition response {}", response);
                        Errors error;
                        try {
                            if (response.authenticationException() != null) {
                                // For now, we treat authentication errors as retriable. We use the
                                // `NETWORK_EXCEPTION` error code for lack of a good alternative.
                                // Note that `NodeToControllerChannelManager` will still log the
                                // authentication errors so that users have a chance to fix the problem.
                                error = Errors.NETWORK_EXCEPTION;
                            } else if (response.versionMismatch() != null) {
                                error = Errors.UNSUPPORTED_VERSION;
                            } else {
                                error = handleAlterPartitionResponse(
                                        (AlterPartitionResponse) response.responseBody(),
                                        brokerEpoch,
                                        inflightAlterPartitionItems
                                );
                            }
                        } finally {
                            // clear the flag so future requests can proceed
                            clearInFlightRequest();
                        }

                        // check if we need to send another request right away
                        if (error == Errors.NONE) {
                            // In the normal case, check for pending updates to send immediately
                            maybePropagateIsrChanges();
                        } else {
                            // If we received a top-level error from the controller, retry the request in the near future
                            scheduler.scheduleOnce("send-alter-partition", () -> maybePropagateIsrChanges(), 50);
                        }
                    }

                    @Override
                    public void onTimeout() {
                        throw new IllegalStateException("Encountered unexpected timeout when sending AlterPartition to the controller");
                    }
                });
    }

    /**
     * Builds an AlterPartition request.
     * <p>
     * While building the request, we don't know which version of the AlterPartition API is
     * supported by the controller. The final decision is taken when the AlterPartitionRequest
     * is built in the network client based on the advertised api versions of the controller.
     *
     * @return an AlterPartitionRequest.Builder with the provided parameters.
     */
    private AlterPartitionRequest.Builder buildRequest(
            List<AlterPartitionItem> inflightAlterPartitionItems,
            long brokerEpoch) {
        AlterPartitionRequestData message = new AlterPartitionRequestData()
                .setBrokerId(brokerId)
                .setBrokerEpoch(brokerEpoch);

        inflightAlterPartitionItems.stream()
                .collect(Collectors.groupingBy(item -> item.topicIdPartition().topicId()))
                .forEach((topicId, items) -> {
                    AlterPartitionRequestData.TopicData topicData = new AlterPartitionRequestData.TopicData().setTopicId(topicId);
                    message.topics().add(topicData);

                    items.forEach(item -> {
                        AlterPartitionRequestData.PartitionData partitionData =
                                new AlterPartitionRequestData.PartitionData()
                                        .setPartitionIndex(item.topicIdPartition().partitionId())
                                        .setLeaderEpoch(item.leaderAndIsr().leaderEpoch())
                                        .setNewIsrWithEpochs(item.leaderAndIsr().isrWithBrokerEpoch())
                                        .setPartitionEpoch(item.leaderAndIsr().partitionEpoch());

                        partitionData.setLeaderRecoveryState(item.leaderAndIsr().leaderRecoveryState().value());

                        topicData.partitions().add(partitionData);
                    });

                });

        return new AlterPartitionRequest.Builder(message);
    }

    private Errors handleAlterPartitionResponse(AlterPartitionResponse alterPartitionResponse,
                                                long sentBrokerEpoch,
                                                List<AlterPartitionItem> inflightAlterPartitionItems) {
        AlterPartitionResponseData data = alterPartitionResponse.data();
        Errors error = Errors.forCode(data.errorCode());
        switch (error) {
            case STALE_BROKER_EPOCH -> log.warn("Broker had a stale broker epoch ({}), retrying.", sentBrokerEpoch);
            case CLUSTER_AUTHORIZATION_FAILED -> log.error("Broker is not authorized to send AlterPartition to controller",
                    Errors.CLUSTER_AUTHORIZATION_FAILED.exception("Broker is not authorized to send AlterPartition to controller"));
            case NONE -> {
                // Collect partition-level responses to pass to the callbacks
                Map<TopicIdPartition, LeaderAndIsr> successResponses = new HashMap<>();
                Map<TopicIdPartition, Errors> errorResponses = new HashMap<>();
                data.topics().forEach(topic ->
                        topic.partitions().forEach(partition -> {
                            TopicIdPartition tp = new TopicIdPartition(topic.topicId(), partition.partitionIndex());
                            Errors apiError = Errors.forCode(partition.errorCode());
                            log.debug("Controller successfully handled AlterPartition request for {}: {}", tp, partition);
                            if (apiError == Errors.NONE) {
                                Optional<LeaderRecoveryState> leaderRecoveryStateOpt =
                                        LeaderRecoveryState.optionalOf(partition.leaderRecoveryState());
                                if (leaderRecoveryStateOpt.isPresent()) {
                                    successResponses.put(tp, new LeaderAndIsr(
                                            partition.leaderId(),
                                            partition.leaderEpoch(),
                                            partition.isr(),
                                            leaderRecoveryStateOpt.get(),
                                            partition.partitionEpoch()
                                    ));
                                } else {
                                    log.error("Controller returned an invalid leader recovery state ({}) for {}: {}", partition.leaderRecoveryState(), tp, partition);
                                    errorResponses.put(tp, Errors.UNKNOWN_SERVER_ERROR);
                                }
                            } else {
                                errorResponses.put(tp, apiError);
                            }
                        }
                        ));
                // Iterate across the items we sent rather than what we received to ensure we run the callback even if a
                // partition was somehow erroneously excluded from the response. Note that these callbacks are run from
                // the leaderIsrUpdateLock write lock in Partition#sendAlterPartitionRequest
                inflightAlterPartitionItems.forEach(inflightAlterPartition -> {
                    if (successResponses.containsKey(inflightAlterPartition.topicIdPartition())) {
                        // Regardless of callback outcome, we need to clear from the unsent updates map to unblock further
                        // updates. We clear it now to allow the callback to submit a new update if needed.
                        unsentIsrUpdates.remove(inflightAlterPartition.topicIdPartition());
                        inflightAlterPartition.future().complete(successResponses.get(inflightAlterPartition.topicIdPartition()));
                    } else if (errorResponses.containsKey(inflightAlterPartition.topicIdPartition())) {
                        unsentIsrUpdates.remove(inflightAlterPartition.topicIdPartition());
                        inflightAlterPartition.future().completeExceptionally(errorResponses.get(inflightAlterPartition.topicIdPartition()).exception());
                    } else {
                        // Don't remove this partition from the update map so it will get re-sent
                        log.warn("Partition {} was sent but not included in the response", inflightAlterPartition.topicIdPartition());
                    }
                });
            }
            default -> log.warn("Controller returned an unexpected top-level error when handling AlterPartition request: {}", error);
        }

        return error;
    }

}
