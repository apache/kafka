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

package org.apache.kafka.jmh.metadata;

import kafka.controller.KafkaController;
import kafka.coordinator.group.GroupCoordinator;
import kafka.coordinator.transaction.TransactionCoordinator;
import kafka.network.RequestChannel;
import kafka.network.RequestConvertToJson;
import kafka.server.AdminManager;
import kafka.server.BrokerFeatures;
import kafka.server.BrokerTopicStats;
import kafka.server.ClientQuotaManager;
import kafka.server.ClientRequestQuotaManager;
import kafka.server.ControllerMutationQuotaManager;
import kafka.server.FetchManager;
import kafka.server.FinalizedFeatureCache;
import kafka.server.ForwardingManager;
import kafka.server.KafkaApis;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.MetadataCache;
import kafka.server.QuotaFactory;
import kafka.server.ReplicaManager;
import kafka.server.ReplicationQuotaManager;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataBroker;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataEndpoint;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SystemTime;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import scala.Option;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)

public class MetadataRequestBenchmark {
    @Param({"500", "1000", "5000"})
    private int topicCount;
    @Param({"10", "20", "50"})
    private int partitionCount;

    private RequestChannel requestChannel = Mockito.mock(RequestChannel.class, Mockito.withSettings().stubOnly());
    private RequestChannel.Metrics requestChannelMetrics = Mockito.mock(RequestChannel.Metrics.class);
    private ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);
    private GroupCoordinator groupCoordinator = Mockito.mock(GroupCoordinator.class);
    private AdminManager adminManager = Mockito.mock(AdminManager.class);
    private TransactionCoordinator transactionCoordinator = Mockito.mock(TransactionCoordinator.class);
    private KafkaController kafkaController = Mockito.mock(KafkaController.class);
    private ForwardingManager forwardingManager = Mockito.mock(ForwardingManager.class);
    private KafkaZkClient kafkaZkClient = Mockito.mock(KafkaZkClient.class);
    private Metrics metrics = new Metrics();
    private int brokerId = 1;
    private MetadataCache metadataCache = new MetadataCache(brokerId);
    private ClientQuotaManager clientQuotaManager = Mockito.mock(ClientQuotaManager.class);
    private ClientRequestQuotaManager clientRequestQuotaManager = Mockito.mock(ClientRequestQuotaManager.class);
    private ControllerMutationQuotaManager controllerMutationQuotaManager = Mockito.mock(ControllerMutationQuotaManager.class);
    private ReplicationQuotaManager replicaQuotaManager = Mockito.mock(ReplicationQuotaManager.class);
    private QuotaFactory.QuotaManagers quotaManagers = new QuotaFactory.QuotaManagers(clientQuotaManager,
        clientQuotaManager, clientRequestQuotaManager, controllerMutationQuotaManager, replicaQuotaManager,
        replicaQuotaManager, replicaQuotaManager, Option.empty());
    private FetchManager fetchManager = Mockito.mock(FetchManager.class);
    private BrokerTopicStats brokerTopicStats = new BrokerTopicStats();
    private KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user");
    private KafkaApis kafkaApis;
    private RequestChannel.Request allTopicMetadataRequest;

    @Setup(Level.Trial)
    public void setup() {
        initializeMetadataCache();
        kafkaApis = createKafkaApis();
        allTopicMetadataRequest = buildAllTopicMetadataRequest();
    }

    private void initializeMetadataCache() {
        List<UpdateMetadataBroker> liveBrokers = new LinkedList<>();
        List<UpdateMetadataPartitionState> partitionStates = new LinkedList<>();

        IntStream.range(0, 5).forEach(brokerId -> liveBrokers.add(
            new UpdateMetadataBroker().setId(brokerId)
                .setEndpoints(endpoints(brokerId))
                .setRack("rack1")));

        IntStream.range(0, topicCount).forEach(topicId -> {
            String topicName = "topic-" + topicId;

            IntStream.range(0, partitionCount).forEach(partitionId -> {
                partitionStates.add(
                    new UpdateMetadataPartitionState().setTopicName(topicName)
                        .setPartitionIndex(partitionId)
                        .setControllerEpoch(1)
                        .setLeader(partitionCount % 5)
                        .setLeaderEpoch(0)
                        .setIsr(Arrays.asList(0, 1, 3))
                        .setZkVersion(1)
                        .setReplicas(Arrays.asList(0, 1, 3)));
            });
        });

        UpdateMetadataRequest updateMetadataRequest = new UpdateMetadataRequest.Builder(
            ApiKeys.UPDATE_METADATA.latestVersion(),
            1, 1, 1,
            partitionStates, liveBrokers, Collections.emptyMap()).build();
        metadataCache.updateMetadata(100, updateMetadataRequest);
    }

    private List<UpdateMetadataEndpoint> endpoints(final int brokerId) {
        return Collections.singletonList(
            new UpdateMetadataEndpoint()
                .setHost("host_" + brokerId)
                .setPort(9092)
                .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
                .setListener(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT).value()));
    }

    private KafkaApis createKafkaApis() {
        Properties kafkaProps =  new Properties();
        kafkaProps.put(KafkaConfig$.MODULE$.ZkConnectProp(), "zk");
        kafkaProps.put(KafkaConfig$.MODULE$.BrokerIdProp(), brokerId + "");
        BrokerFeatures brokerFeatures = BrokerFeatures.createDefault();
        return new KafkaApis(requestChannel,
            replicaManager,
            adminManager,
            groupCoordinator,
            transactionCoordinator,
            kafkaController,
            forwardingManager,
            kafkaZkClient,
            brokerId,
            new KafkaConfig(kafkaProps),
            metadataCache,
            metrics,
            Option.empty(),
            quotaManagers,
            fetchManager,
            brokerTopicStats,
            "clusterId",
            new SystemTime(),
            null,
            brokerFeatures,
            new FinalizedFeatureCache(brokerFeatures));
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        kafkaApis.close();
        metrics.close();
    }

    private RequestChannel.Request buildAllTopicMetadataRequest() {
        MetadataRequest metadataRequest = MetadataRequest.Builder.allTopics().build();
        RequestHeader header = new RequestHeader(metadataRequest.apiKey(), metadataRequest.version(), "", 0);
        ByteBuffer bodyBuffer = metadataRequest.serialize();

        RequestContext context = new RequestContext(header, "1", null, principal,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);
        return new RequestChannel.Request(1, context, 0, MemoryPool.NONE, bodyBuffer, requestChannelMetrics, Option.empty());
    }

    @Benchmark
    public void testMetadataRequestForAllTopics() {
        kafkaApis.handleTopicMetadataRequest(allTopicMetadataRequest);
    }

    @Benchmark
    public String testRequestToJson() {
        return RequestConvertToJson.requestDesc(allTopicMetadataRequest.header(), allTopicMetadataRequest.requestLog(), allTopicMetadataRequest.isForwarded()).toString();
    }
}
