package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.MockTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

class ClusterModel {
    final List<SimulationEvent> events = new ArrayList<>();
    final MockTime time;
    final Random random;
    final String topic = "test-topic";
    private final Map<Integer, BrokerModel> brokers;
    private final Map<Integer, PartitionModel> partitions;
    private long nextProducerId = 0;

    ClusterModel(
        MockTime time,
        Random random,
        List<Node> brokers,
        int numPartitions,
        boolean enableStrictValidation
    ) {
        this.time = time;
        this.random = random;
        this.brokers = brokers.stream().collect(Collectors.toMap(
            node -> node.id(),
            node -> new BrokerModel(node, this)
        ));

        this.partitions = new HashMap<>(numPartitions);

        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            PartitionModel partitionModel = new PartitionModel(partitionId, enableStrictValidation);
            int randomLeaderId = randomBroker().node.id();
            partitionModel.setNewLeader(randomLeaderId);
            this.partitions.put(partitionId, partitionModel);
        }
    }

    void assertNoDuplicates() {
        partitions.values().forEach(PartitionModel::assertNoDuplicates);
    }

    void assertNoReordering() {
        partitions.values().forEach(PartitionModel::assertNoReordering);
    }

    void enableUnderMinIsr(int partitionId, long durationMs) {
        PartitionModel partitionModel = partitions.get(partitionId);
        partitionModel.enableUnderMinIsrUntil(time.milliseconds() + durationMs);
    }

    void enableInvalidRecordError(int partitionId, long durationMs) {
        PartitionModel partitionModel = partitions.get(partitionId);
        partitionModel.enableInvalidRecordErrorsUntil(time.milliseconds() + durationMs);
    }

    boolean isLeader(BrokerModel broker, int partitionId) {
        return partitions.get(partitionId).leaderId == broker.node.id();
    }

    InitProducerIdResponse handleInitProducerIdRequest(InitProducerIdRequest request) {
        return new InitProducerIdResponse(
            new InitProducerIdResponseData()
                .setErrorCode(Errors.NONE.code())
                .setProducerId(nextProducerId++)
                .setProducerEpoch((short) 0)
        );
    }

    MetadataResponse handleMetadataRequest(MetadataRequest request) {
        MetadataResponseData responseData = new MetadataResponseData();
        for (BrokerModel broker : brokers.values()) {
            responseData.brokers().add(
                new MetadataResponseData.MetadataResponseBroker()
                    .setHost(broker.node.host())
                    .setNodeId(broker.node.id())
                    .setPort(broker.node.port())
                    .setRack((broker.node.rack()))
            );
        }

        for (MetadataRequestData.MetadataRequestTopic requestTopic : request.data().topics()) {
            if (!requestTopic.name().equals(topic)) {
                throw new IllegalArgumentException("Unexpected topic " + requestTopic.name());
            }
            MetadataResponseData.MetadataResponseTopic responseTopic =
                new MetadataResponseData.MetadataResponseTopic()
                    .setName(topic)
                    .setErrorCode(Errors.NONE.code());

            for (PartitionModel partition : partitions.values()) {
                responseTopic.partitions().add(
                    new MetadataResponseData.MetadataResponsePartition()
                        .setPartitionIndex(partition.partitionId)
                        .setLeaderId(partition.leaderId)
                        .setLeaderEpoch(partition.leaderEpoch)
                        .setReplicaNodes(Collections.singletonList(partition.leaderId))
                        .setIsrNodes(Collections.singletonList(partition.leaderId))
                );
            }

            responseData.topics().add(responseTopic);
        }

        return new MetadataResponse(responseData, request.version());
    }

    private ProduceResponse handleProduceRequest(
        BrokerModel broker,
        ProduceRequest request,
        long connectionId
    ) {
        ProduceResponseData responseData = new ProduceResponseData();

        for (ProduceRequestData.TopicProduceData requestTopic : request.data().topicData()) {
            if (!this.topic.equals(requestTopic.name())) {
                throw new IllegalArgumentException("Unexpected topic " + requestTopic.name());
            }

            ProduceResponseData.TopicProduceResponse responseTopic =
                new ProduceResponseData.TopicProduceResponse()
                    .setName(this.topic);

            for (ProduceRequestData.PartitionProduceData requestPartition : requestTopic.partitionData()) {
                int partitionId = requestPartition.index();
                PartitionModel partition = partitions.get(partitionId);

                MemoryRecords records = (MemoryRecords) requestPartition.records();
                DefaultRecordBatch batch = ((DefaultRecordBatch) records.batches().iterator().next());

                final ProduceResponseData.PartitionProduceResponse responsePartition;
                if (partition == null) {
                    responsePartition = new ProduceResponseData.PartitionProduceResponse()
                        .setIndex(partitionId)
                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
                } else if (!isLeader(broker, partitionId)) {
                    responsePartition = new ProduceResponseData.PartitionProduceResponse()
                        .setIndex(partitionId)
                        .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code());
                } else if (partition.isUnderMinIsr(time.milliseconds())) {
                    responsePartition = new ProduceResponseData.PartitionProduceResponse()
                        .setIndex(partitionId)
                        .setErrorCode(Errors.NOT_ENOUGH_REPLICAS.code());
                } else if (partition.hasInvalidRecord(time.milliseconds(), batch)) {
                    responsePartition = new ProduceResponseData.PartitionProduceResponse()
                        .setIndex(partitionId)
                        .setErrorCode(Errors.INVALID_RECORD.code());
                } else {
                    responsePartition = partition.tryProduce(batch);
                }

                this.events.add(new ProduceRequestHandled(
                    batch,
                    partitionId,
                    connectionId,
                    Errors.forCode(responsePartition.errorCode())
                ));
                responseTopic.partitionResponses().add(responsePartition);
            }

            responseData.responses().add(responseTopic);
        }

        return new ProduceResponse(responseData);
    }

    private AbstractResponse handle(
        BrokerModel broker,
        AbstractRequest request,
        long connectionId
    ) {
        if (request instanceof InitProducerIdRequest) {
            return handleInitProducerIdRequest((InitProducerIdRequest) request);
        } else if (request instanceof MetadataRequest) {
            return handleMetadataRequest((MetadataRequest) request);
        } else if (request instanceof ProduceRequest) {
            return handleProduceRequest(broker, (ProduceRequest) request, connectionId);
        } else {
            throw new IllegalArgumentException("Unexpected request type: " + request.getClass());
        }
    }

    void poll() {
        BrokerModel broker = randomBroker();
        ConnectionModel connection = broker.randomConnection(random);
        if (connection != null) {
            connection.maybeHandleRequest(request ->
                this.handle(broker, request, connection.connectionId)
            );
        }
    }

    BrokerModel broker(int brokerId) {
        return brokers.get(brokerId);
    }

    BrokerModel randomBroker() {
        return SimulationUtils.randomEntry(random, brokers).getValue();
    }

}
