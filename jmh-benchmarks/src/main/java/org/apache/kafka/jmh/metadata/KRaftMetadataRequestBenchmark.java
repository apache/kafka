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

import kafka.coordinator.transaction.TransactionCoordinator;
import kafka.network.RequestChannel;
import kafka.network.RequestConvertToJson;
import kafka.server.AutoTopicCreationManager;
import kafka.server.BrokerTopicStats;
import kafka.server.ClientQuotaManager;
import kafka.server.ClientRequestQuotaManager;
import kafka.server.ControllerMutationQuotaManager;
import kafka.server.FetchManager;
import kafka.server.ForwardingManager;
import kafka.server.KafkaApis;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.MetadataCache;
import kafka.server.QuotaFactory;
import kafka.server.RaftSupport;
import kafka.server.ReplicaManager;
import kafka.server.ReplicationQuotaManager;
import kafka.server.SimpleApiVersionManager;
import kafka.server.builders.KafkaApisBuilder;
import kafka.server.metadata.KRaftMetadataCache;
import kafka.server.metadata.MockConfigRepository;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataEndpoint;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupCoordinator;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.common.Features;
import org.apache.kafka.server.common.MetadataVersion;
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
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)

public class KRaftMetadataRequestBenchmark {
    private final RequestChannel requestChannel = Mockito.mock(RequestChannel.class, Mockito.withSettings().stubOnly());
    private final RequestChannel.Metrics requestChannelMetrics = Mockito.mock(RequestChannel.Metrics.class);
    private final ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);
    private final GroupCoordinator groupCoordinator = Mockito.mock(GroupCoordinator.class);
    private final TransactionCoordinator transactionCoordinator = Mockito.mock(TransactionCoordinator.class);
    private final AutoTopicCreationManager autoTopicCreationManager = Mockito.mock(AutoTopicCreationManager.class);
    private final Metrics metrics = new Metrics();
    private final int brokerId = 1;
    private final ForwardingManager forwardingManager = Mockito.mock(ForwardingManager.class);
    private final KRaftMetadataCache metadataCache = MetadataCache.kRaftMetadataCache(brokerId);
    private final ClientQuotaManager clientQuotaManager = Mockito.mock(ClientQuotaManager.class);
    private final ClientRequestQuotaManager clientRequestQuotaManager = Mockito.mock(ClientRequestQuotaManager.class);
    private final ControllerMutationQuotaManager controllerMutationQuotaManager = Mockito.mock(ControllerMutationQuotaManager.class);
    private final ReplicationQuotaManager replicaQuotaManager = Mockito.mock(ReplicationQuotaManager.class);
    private final QuotaFactory.QuotaManagers quotaManagers = new QuotaFactory.QuotaManagers(clientQuotaManager,
            clientQuotaManager, clientRequestQuotaManager, controllerMutationQuotaManager, replicaQuotaManager,
            replicaQuotaManager, replicaQuotaManager, Option.empty());
    private final FetchManager fetchManager = Mockito.mock(FetchManager.class);
    private final BrokerTopicStats brokerTopicStats = new BrokerTopicStats(Optional.empty());
    private final KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user");
    @Param({"500", "1000",  "5000"})
    private int topicCount;
    @Param({"10", "20", "50"})
    private int partitionCount;
    private KafkaApis kafkaApis;
    private RequestChannel.Request allTopicMetadataRequest;

    @Setup(Level.Trial)
    public void setup() {
        initializeMetadataCache();
        kafkaApis = createKafkaApis();
        allTopicMetadataRequest = buildAllTopicMetadataRequest();
    }

    private void initializeMetadataCache() {
        MetadataDelta buildupMetadataDelta = new MetadataDelta(MetadataImage.EMPTY);
        IntStream.range(0, 5).forEach(brokerId -> {
            RegisterBrokerRecord.BrokerEndpointCollection endpoints = new RegisterBrokerRecord.BrokerEndpointCollection();
            endpoints(brokerId).forEach(endpoint ->
                endpoints.add(new RegisterBrokerRecord.BrokerEndpoint().
                    setHost(endpoint.host()).
                    setPort(endpoint.port()).
                    setName(endpoint.listener()).
                    setSecurityProtocol(endpoint.securityProtocol())));
            buildupMetadataDelta.replay(new RegisterBrokerRecord().
                setBrokerId(brokerId).
                setBrokerEpoch(100L).
                setFenced(false).
                setRack(null).
                setEndPoints(endpoints).
                setIncarnationId(Uuid.fromString(Uuid.randomUuid().toString())));
        });
        IntStream.range(0, topicCount).forEach(topicNum -> {
            Uuid topicId = Uuid.randomUuid();
            buildupMetadataDelta.replay(new TopicRecord().setName("topic-" + topicNum).setTopicId(topicId));
            IntStream.range(0, partitionCount).forEach(partitionId ->
                buildupMetadataDelta.replay(new PartitionRecord().
                    setPartitionId(partitionId).
                    setTopicId(topicId).
                    setReplicas(Arrays.asList(0, 1, 3)).
                    setIsr(Arrays.asList(0, 1, 3)).
                    setRemovingReplicas(Collections.emptyList()).
                    setAddingReplicas(Collections.emptyList()).
                    setLeader(partitionCount % 5).
                    setLeaderEpoch(0)));
        });
        metadataCache.setImage(buildupMetadataDelta.apply(MetadataProvenance.EMPTY));
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
        kafkaProps.put(KafkaConfig$.MODULE$.NodeIdProp(), brokerId + "");
        kafkaProps.put(KafkaConfig$.MODULE$.ProcessRolesProp(), "broker");
        kafkaProps.put(KafkaConfig$.MODULE$.QuorumVotersProp(), "9000@foo:8092");
        kafkaProps.put(KafkaConfig$.MODULE$.ControllerListenerNamesProp(), "CONTROLLER");
        KafkaConfig config = new KafkaConfig(kafkaProps);
        return new KafkaApisBuilder().
                setRequestChannel(requestChannel).
                setMetadataSupport(new RaftSupport(forwardingManager, metadataCache)).
                setReplicaManager(replicaManager).
                setGroupCoordinator(groupCoordinator).
                setTxnCoordinator(transactionCoordinator).
                setAutoTopicCreationManager(autoTopicCreationManager).
                setBrokerId(brokerId).
                setConfig(config).
                setConfigRepository(new MockConfigRepository()).
                setMetadataCache(metadataCache).
                setMetrics(metrics).
                setAuthorizer(Optional.empty()).
                setQuotas(quotaManagers).
                setFetchManager(fetchManager).
                setBrokerTopicStats(brokerTopicStats).
                setClusterId("clusterId").
                setTime(Time.SYSTEM).
                setTokenManager(null).
                setApiVersionManager(new SimpleApiVersionManager(
                        ApiMessageType.ListenerType.BROKER,
                        false,
                        false,
                        () -> Features.fromKRaftVersion(MetadataVersion.latestTesting()))).
                build();
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

    @Benchmark
    public void testTopicIdInfo() {
        metadataCache.topicIdInfo();
    }
}
