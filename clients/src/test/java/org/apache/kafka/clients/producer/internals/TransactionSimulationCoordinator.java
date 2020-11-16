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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
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
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import static org.apache.kafka.common.record.RecordBatch.NO_PRODUCER_EPOCH;
import static org.apache.kafka.common.record.RecordBatch.NO_PRODUCER_ID;

/**
 * A reduced functionality of a combination of transaction coordinator and group coordinator.
 * It provides basic event handling from {@link Sender} with transaction turned on.
 *
 * Random fault injection is supported as well, which will return a retriable error to
 * the client.
 */
class TransactionSimulationCoordinator {

    private final Map<TopicPartition, List<Record>> pendingPartitionData;
    private final Map<TopicPartition, Long> pendingOffsets;
    private boolean offsetsAddedToTxn = false;

    private long currentProducerId = 0L;
    private short currentEpoch = 0;
    private Random faultInjectRandom = new Random();

    public Map<TopicPartition, List<Record>> persistentPartitionData() {
        return persistentPartitionData;
    }

    public Map<TopicPartition, Long> committedOffsets() {
        return committedOffsets;
    }

    private final Map<TopicPartition, List<Record>> persistentPartitionData;
    private final Map<TopicPartition, Long> committedOffsets;

    private final MockClient networkClient;
    private final int throttleTimeMs = 10;

    TransactionSimulationCoordinator(MockClient networkClient) {
        this.networkClient = networkClient;
        this.pendingPartitionData = new HashMap<>();
        this.pendingOffsets = new HashMap<>();
        this.persistentPartitionData = new HashMap<>();
        this.committedOffsets = new HashMap<>();
    }

    void runOnce(boolean dropMessage) {
        Queue<ClientRequest> incomingRequests = networkClient.requests();

        final boolean faultInject = faultInjectRandom.nextBoolean();

        if (incomingRequests.peek() == null) {
            return;
        }

        final AbstractResponse response;
        AbstractRequest nextRequest = incomingRequests.peek().requestBuilder().build();
        if (nextRequest instanceof FindCoordinatorRequest) {
            response = handleFindCoordinator(faultInject);
        } else if (nextRequest instanceof InitProducerIdRequest) {
            response = handleInitProducerId((InitProducerIdRequest) nextRequest, faultInject);
        } else if (nextRequest instanceof AddPartitionsToTxnRequest) {
            response = handleAddPartitionToTxn((AddPartitionsToTxnRequest) nextRequest, faultInject);
        } else if (nextRequest instanceof AddOffsetsToTxnRequest) {
            response = handleAddOffsetsToTxn((AddOffsetsToTxnRequest) nextRequest, faultInject);
        } else if (nextRequest instanceof TxnOffsetCommitRequest) {
            response = handleTxnCommit((TxnOffsetCommitRequest) nextRequest, faultInject);
        } else if (nextRequest instanceof ProduceRequest) {
            response = handleProduce((ProduceRequest) nextRequest, faultInject);
        } else if (nextRequest instanceof EndTxnRequest) {
            response = handleEndTxn((EndTxnRequest) nextRequest, faultInject);
        } else {
            throw new IllegalArgumentException("Unknown request: " + nextRequest);
        }

        networkClient.respond(response, dropMessage);
    }

    private FindCoordinatorResponse handleFindCoordinator(final boolean faultInject) {
        if (faultInject) {
            return new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()));
        } else {
            return new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setHost("localhost")
                    .setNodeId(0)
                    .setPort(2211)
            );
        }
    }

    private InitProducerIdResponse handleInitProducerId(InitProducerIdRequest request,
                                                        final boolean faultInject) {
        if (faultInject) {
            return new InitProducerIdResponse(
                new InitProducerIdResponseData()
                    .setErrorCode(Errors.NOT_COORDINATOR.code())
            );
        } else if (request.data.producerId() != NO_PRODUCER_ID &&
                    request.data.producerId() != currentProducerId) {
            return new InitProducerIdResponse(
                new InitProducerIdResponseData()
                    .setErrorCode(Errors.UNKNOWN_PRODUCER_ID.code())
                    .setThrottleTimeMs(throttleTimeMs)
            );
        } else if (request.data.producerEpoch() != NO_PRODUCER_EPOCH &&
                    request.data.producerEpoch() != currentEpoch) {
            return new InitProducerIdResponse(
                new InitProducerIdResponseData()
                    .setErrorCode(Errors.INVALID_PRODUCER_EPOCH.code())
                    .setThrottleTimeMs(throttleTimeMs)
            );
        } else {
            currentProducerId += 1;
            currentEpoch += 1;
            return new InitProducerIdResponse(
                new InitProducerIdResponseData()
                    .setProducerId(currentProducerId)
                    .setProducerEpoch(currentEpoch)
                    .setErrorCode(Errors.NONE.code())
            );
        }
    }

    private AddPartitionsToTxnResponse handleAddPartitionToTxn(AddPartitionsToTxnRequest request,
                                                               final boolean faultInject) {
        Map<TopicPartition, Errors> errors = new HashMap<>();
        request.partitions().forEach(topicPartition -> {
            if (faultInject) {
                errors.put(topicPartition, Errors.COORDINATOR_NOT_AVAILABLE);
            } else if (request.data.producerId() != currentProducerId) {
                errors.put(topicPartition, Errors.UNKNOWN_PRODUCER_ID);
            } else if (request.data.producerEpoch() != currentEpoch) {
                errors.put(topicPartition, Errors.INVALID_PRODUCER_EPOCH);
            } else {
                errors.put(topicPartition, Errors.NONE);
            }
        });

        return new AddPartitionsToTxnResponse(
            throttleTimeMs,
            errors
        );
    }

    private AddOffsetsToTxnResponse handleAddOffsetsToTxn(AddOffsetsToTxnRequest request,
                                                          final boolean faultInject) {
        if (faultInject) {
            return new AddOffsetsToTxnResponse(
                new AddOffsetsToTxnResponseData()
                    .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
                    .setThrottleTimeMs(throttleTimeMs)
            );
        } else if (request.data.producerId() != currentProducerId) {
            return new AddOffsetsToTxnResponse(
                new AddOffsetsToTxnResponseData()
                    .setErrorCode(Errors.UNKNOWN_PRODUCER_ID.code())
                    .setThrottleTimeMs(throttleTimeMs)
            );
        } else if (request.data.producerEpoch() != currentEpoch) {
            return new AddOffsetsToTxnResponse(
                new AddOffsetsToTxnResponseData()
                    .setErrorCode(Errors.INVALID_PRODUCER_EPOCH.code())
                    .setThrottleTimeMs(throttleTimeMs)
            );
        } else {
            offsetsAddedToTxn = true;
            return new AddOffsetsToTxnResponse(
                new AddOffsetsToTxnResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setThrottleTimeMs(throttleTimeMs)
            );
        }
    }

    private AbstractResponse handleTxnCommit(TxnOffsetCommitRequest request,
                                             final boolean faultInject) {
        Map<TopicPartition, Errors> errors = new HashMap<>();
        request.data.topics().forEach(topic -> topic.partitions().forEach(partition -> {
            TopicPartition key = new TopicPartition(topic.name(), partition.partitionIndex());
            if (faultInject) {
                errors.put(key, Errors.COORDINATOR_LOAD_IN_PROGRESS);
            } else if (request.data.producerId() != currentProducerId) {
                errors.put(key, Errors.UNKNOWN_PRODUCER_ID);
            } else if (request.data.producerEpoch() != currentEpoch) {
                errors.put(key, Errors.INVALID_PRODUCER_EPOCH);
            } else if (offsetsAddedToTxn) {
                pendingOffsets.put(key, partition.committedOffset());
                errors.put(key, Errors.NONE);
            } else {
                errors.put(key, Errors.UNKNOWN_TOPIC_OR_PARTITION);
            }
        }));

        return new TxnOffsetCommitResponse(
            throttleTimeMs,
            errors
        );
    }

    private AbstractResponse handleProduce(ProduceRequest request,
                                           final boolean faultInject) {
        Map<TopicPartition, PartitionResponse> errors = new HashMap<>();
        Map<TopicPartition, MemoryRecords> partitionRecords = request.partitionRecordsOrFail();

        partitionRecords.forEach((topicPartition, records) -> {
            if (faultInject) {
                // Trigger KIP-360 path.
                errors.put(topicPartition, new PartitionResponse(Errors.UNKNOWN_PRODUCER_ID));
            } else {
                List<Record> sentRecords = pendingPartitionData.getOrDefault(topicPartition, new ArrayList<>());
                for (Record partitionRecord  : records.records()) {
                    sentRecords.add(partitionRecord);
                }

                pendingPartitionData.put(topicPartition, sentRecords);
                errors.put(topicPartition, new PartitionResponse(Errors.NONE));
            }
        });

        return new ProduceResponse(errors, throttleTimeMs);
    }

    private EndTxnResponse handleEndTxn(EndTxnRequest request, final boolean faultInject) {
        if (faultInject) {
            return new EndTxnResponse(
                new EndTxnResponseData()
                    .setErrorCode(Errors.NOT_COORDINATOR.code())
                    .setThrottleTimeMs(throttleTimeMs)
            );
        } else if (request.data.producerId() != currentProducerId) {
            return new EndTxnResponse(
            new EndTxnResponseData()
                .setErrorCode(Errors.UNKNOWN_PRODUCER_ID.code())
                .setThrottleTimeMs(throttleTimeMs)
            );
        } else if (request.data.producerEpoch() != currentEpoch) {
            return new EndTxnResponse(
                new EndTxnResponseData()
                    .setErrorCode(Errors.INVALID_PRODUCER_EPOCH.code())
                    .setThrottleTimeMs(throttleTimeMs)
            );
        }

        if (request.result().equals(TransactionResult.COMMIT)) {
            for (Map.Entry<TopicPartition, List<Record>> entry : pendingPartitionData.entrySet()) {
                List<Record> materializedRecords = persistentPartitionData.getOrDefault(entry.getKey(), new ArrayList<>());
                materializedRecords.addAll(entry.getValue());
                persistentPartitionData.put(entry.getKey(), materializedRecords);
            }
            committedOffsets.putAll(pendingOffsets);
        }
        pendingPartitionData.clear();
        pendingOffsets.clear();
        offsetsAddedToTxn = false;

        return new EndTxnResponse(
            new EndTxnResponseData()
                .setErrorCode(Errors.NONE.code())
                .setThrottleTimeMs(throttleTimeMs)
        );
    }
}
