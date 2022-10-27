package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.Function;

class ConnectionModel {
    final long connectionId;
    final MockTime time;
    final Random random;
    final ClusterModel cluster;
    private final int maxInflights;

    private boolean isConnected = true;
    private Deque<ClientRequest> undeliveredRequests = new ArrayDeque<>();
    private Deque<UndeliveredResponse> undeliveredResponses = new ArrayDeque<>();

    ConnectionModel(
        long connectionId,
        MockTime time,
        Random random,
        ClusterModel cluster,
        int maxInflights
    ) {
        this.connectionId = connectionId;
        this.time = time;
        this.random = random;
        this.cluster = cluster;
        this.maxInflights = maxInflights;
    }

    ClientResponse clientReceive() {
        UndeliveredResponse undelivered = undeliveredResponses.poll();
        if (undelivered == null) {
            return null;
        } else {
            emitResponseReceivedEvent(undelivered);
            return undelivered.response;
        }
    }

    void maybeDropUndeliveredRequests() {
        if (!undeliveredRequests.isEmpty()) {
            int numRequestsToDrop = random.nextInt(undeliveredRequests.size());
            for (int i = 0; i < numRequestsToDrop; i++) {
                ClientRequest dropped = undeliveredRequests.removeLast();
                if (dropped.requestBuilder() instanceof ProduceRequest.Builder) {
                    ProduceRequest.Builder produceBldr = (ProduceRequest.Builder) dropped.requestBuilder();
                    emitProduceRequestEvent(produceBldr, batchEntry -> new ProduceRequestDropped(
                        batchEntry.getValue(),
                        batchEntry.getKey().partition(),
                        connectionId
                    ));
                }
            }
        }
    }

    void clientSend(ClientRequest request) {
        if (!isReady()) {
            throw new IllegalStateException("Attempt to send before the client is ready");
        }

        // We need to copy Produce requests since underlying buffers may get re-allocated
        // after a disconnect before the "server" has processed them.
        if (request.requestBuilder() instanceof ProduceRequest.Builder) {
            ProduceRequest.Builder bldr = (ProduceRequest.Builder) request.requestBuilder();
            ProduceRequest.Builder bldrCopy = deepCopy(bldr);

            undeliveredRequests.add(new ClientRequest(
                request.destination(),
                bldrCopy,
                request.correlationId(),
                request.clientId(),
                request.createdTimeMs(),
                request.expectResponse(),
                request.requestTimeoutMs(),
                request.callback()
            ));

            emitProduceRequestEvent(bldrCopy, batchEntry -> new ProduceRequestSent(
                batchEntry.getValue(),
                batchEntry.getKey().partition(),
                connectionId
            ));
        } else {
            undeliveredRequests.add(request);
        }
    }

    private void emitProduceRequestEvent(
        ProduceRequest.Builder bldr,
        Function<Map.Entry<TopicPartition, RecordBatch>, SimulationEvent> eventBuilder
    ) {
        Map<TopicPartition, RecordBatch> batches = collectBatches(bldr.build().data());
        for (Map.Entry<TopicPartition, RecordBatch> batchEntry : batches.entrySet()) {
            SimulationEvent event = eventBuilder.apply(batchEntry);
            cluster.events.add(event);
        }
    }

    void maybeHandleRequest(Function<AbstractRequest, AbstractResponse> func) {
        ClientRequest request = undeliveredRequests.poll();
        if (request != null) {
            AbstractRequest requestBody = request.requestBuilder().build();
            AbstractResponse responseBody = func.apply(requestBody);

            RequestHeader requestHeader = new RequestHeader(
                requestBody.apiKey(),
                requestBody.version(),
                "",
                request.correlationId()
            );

            ClientResponse response = new ClientResponse(
                requestHeader,
                request.callback(),
                request.destination(),
                request.createdTimeMs(),
                time.milliseconds(),
                false,
                null,
                null,
                responseBody
            );

            undeliveredResponses.add(new UndeliveredResponse(request, response));
        }
    }

    private void emitResponseReceivedEvent(UndeliveredResponse undelivered) {
        if (undelivered.request.requestBuilder() instanceof ProduceRequest.Builder) {
            ProduceRequest.Builder requestBldr = (ProduceRequest.Builder) undelivered.request.requestBuilder();
            Map<TopicPartition, RecordBatch> batches = collectBatches(requestBldr.build().data());

            ProduceResponse response = (ProduceResponse) undelivered.response.responseBody();
            Map<TopicPartition, Errors> errors = collectErrors(response.data());

            for (Map.Entry<TopicPartition, RecordBatch> batchEntry : batches.entrySet()) {
                Errors error = Objects.requireNonNull(errors.get(batchEntry.getKey()));
                cluster.events.add(new ProduceResponseReceived(
                    batchEntry.getValue(),
                    batchEntry.getKey().partition(),
                    connectionId,
                    error
                ));
            }
        }
    }

    List<ClientResponse> disconnect() {
        List<ClientResponse> responses = new ArrayList<>();
        for (UndeliveredResponse undelivered : undeliveredResponses) {
            ClientResponse response = undelivered.response;
            responses.add(new ClientResponse(
                response.requestHeader(),
                response.callback(),
                response.destination(),
                response.createdTimeMs(),
                time.milliseconds(),
                true,
                null,
                null,
                null
            ));
        }

        for (ClientRequest undelivered : undeliveredRequests) {
            AbstractRequest requestBody = undelivered.requestBuilder().build();
            RequestHeader requestHeader = new RequestHeader(
                requestBody.apiKey(),
                requestBody.version(),
                "",
                undelivered.correlationId()
            );
            responses.add(new ClientResponse(
                requestHeader,
                undelivered.callback(),
                undelivered.destination(),
                undelivered.createdTimeMs(),
                time.milliseconds(),
                true,
                null,
                null,
                null
            ));
        }

        this.isConnected = false;
        return responses;
    }

    boolean hasUndeliveredRequests() {
        return !undeliveredRequests.isEmpty();
    }

    boolean isReady() {
        return isConnected && numInflights() < maxInflights;
    }

    int numInflights() {
        return undeliveredRequests.size() + undeliveredResponses.size();
    }

    private static class UndeliveredResponse {
        private final ClientRequest request;
        private final ClientResponse response;

        private UndeliveredResponse(
            ClientRequest request,
            ClientResponse response
        ) {
            this.request = request;
            this.response = response;
        }
    }

    private static ProduceRequest.Builder deepCopy(ProduceRequest.Builder bldr) {
        ProduceRequestData copy = bldr.data().duplicate();
        for (ProduceRequestData.TopicProduceData topicData : copy.topicData()) {
            for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                MemoryRecords records = (MemoryRecords) partitionData.records();
                ByteBuffer bufferCopy = ByteBuffer.wrap(Utils.toArray(records.buffer().duplicate()));
                MemoryRecords recordsCopy = MemoryRecords.readableRecords(bufferCopy);
                partitionData.setRecords(recordsCopy);
            }
        }
        return new ProduceRequest.Builder(
            bldr.oldestAllowedVersion(),
            bldr.latestAllowedVersion(),
            copy
        );
    }

    public static Map<TopicPartition, RecordBatch> collectBatches(
        ProduceRequestData produceRequest
    ) {
        Map<TopicPartition, RecordBatch> batches = new HashMap<>();
        for (ProduceRequestData.TopicProduceData topicData : produceRequest.topicData()) {
            for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                MemoryRecords records = (MemoryRecords) partitionData.records();

                TopicPartition topicPartition = new TopicPartition(
                    topicData.name(),
                    partitionData.index()
                );

                DefaultRecordBatch batch = (DefaultRecordBatch) records.batchIterator().next();
                batches.put(topicPartition, batch);
            }
        }
        return batches;
    }

    public static Map<TopicPartition, Errors> collectErrors(
        ProduceResponseData produceResponse
    ) {
        Map<TopicPartition, Errors> errors = new HashMap<>();
        for (ProduceResponseData.TopicProduceResponse topicData : produceResponse.responses()) {
            for (ProduceResponseData.PartitionProduceResponse partitionData : topicData.partitionResponses()) {
                Errors error = Errors.forCode(partitionData.errorCode());
                TopicPartition topicPartition = new TopicPartition(
                    topicData.name(),
                    partitionData.index()
                );
                errors.put(topicPartition, error);
            }
        }
        return errors;
    }
}
