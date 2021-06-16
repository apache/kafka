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

package org.apache.kafka.jmh.common;

import kafka.controller.KafkaController;
import kafka.coordinator.group.GroupCoordinator;
import kafka.coordinator.transaction.TransactionCoordinator;
import kafka.log.CleanerConfig;
import kafka.log.LogConfig;
import kafka.log.LogManager;
import kafka.network.RequestChannel;
import kafka.server.AlterIsrManager;
import kafka.server.AutoTopicCreationManager;
import kafka.server.BrokerTopicStats;
import kafka.server.ClientQuotaManager;
import kafka.server.ClientQuotaManagerConfig;
import kafka.server.ClientRequestQuotaManager;
import kafka.server.ControllerMutationQuotaManager;
import kafka.server.FetchManager;
import kafka.server.FetchSessionCache;
import kafka.server.KafkaApis;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.LogDirFailureChannel;
import kafka.server.MetadataCache;
import kafka.server.QuotaFactory;
import kafka.server.QuotaType;
import kafka.server.ReplicaManager;
import kafka.server.ReplicationQuotaManager;
import kafka.server.SimpleApiVersionManager;
import kafka.server.ZkAdminManager;
import kafka.server.ZkMetadataCache;
import kafka.server.ZkSupport;
import kafka.server.metadata.CachedConfigRepository;
import kafka.utils.KafkaScheduler;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.LeaderAndIsrRequest;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
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
import scala.collection.JavaConverters;
import scala.compat.java8.OptionConverters;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)

public class KafkaApisFetchBenchmark {
    @Param({"10", "20", "100"})
    private int topicCount;
    @Param({"10", "20", "50"})
    private int partitionCount;


    private final int brokerId = 1;
    private MockTime time = new MockTime();
    private List<ApiKeys> keys = Arrays.asList(ApiKeys.FETCH, ApiKeys.LEADER_AND_ISR);
    @SuppressWarnings("deprecation")
    private RequestChannel.Metrics requestChannelMetrics = new RequestChannel.Metrics(JavaConverters.collectionAsScalaIterable(keys));
    private RequestChannel requestChannel = new RequestChannel(20, "", time, requestChannelMetrics);

    private GroupCoordinator groupCoordinator = Mockito.mock(GroupCoordinator.class);
    private ZkAdminManager adminManager = Mockito.mock(ZkAdminManager.class);
    private TransactionCoordinator transactionCoordinator = Mockito.mock(TransactionCoordinator.class);
    private KafkaController kafkaController = Mockito.mock(KafkaController.class);
    private AutoTopicCreationManager autoTopicCreationManager = Mockito.mock(AutoTopicCreationManager.class);
    private KafkaZkClient kafkaZkClient = Mockito.mock(KafkaZkClient.class);
    private Metrics metrics = new Metrics();
    private ZkMetadataCache metadataCache = MetadataCache.zkMetadataCache(brokerId);

    private ClientQuotaManager clientQuotaManager = new ClientQuotaManager(new ClientQuotaManagerConfig(Long.MAX_VALUE, 11, 1),
            metrics, QuotaType.Fetch$.MODULE$, time, "", Option.empty());
    private ClientRequestQuotaManager clientRequestQuotaManager = new ClientRequestQuotaManager(new ClientQuotaManagerConfig(Long.MAX_VALUE, 11, 1),
            metrics, time, "", Option.empty());
    private ControllerMutationQuotaManager controllerMutationQuotaManager = Mockito.mock(ControllerMutationQuotaManager.class);
    private ReplicationQuotaManager replicaQuotaManager = Mockito.mock(ReplicationQuotaManager.class);
    private QuotaFactory.QuotaManagers quotaManagers = new QuotaFactory.QuotaManagers(clientQuotaManager,
            clientQuotaManager, clientRequestQuotaManager, controllerMutationQuotaManager, replicaQuotaManager,
            replicaQuotaManager, replicaQuotaManager, Option.empty());

    private FetchManager fetchManager =  new FetchManager(time, new FetchSessionCache(1000, 120000));
    private BrokerTopicStats brokerTopicStats = new BrokerTopicStats();
    private KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user");
    private final CachedConfigRepository configRepository = new CachedConfigRepository();
    private KafkaScheduler scheduler =  new KafkaScheduler(1, "scheduler-thread", true);
    private AlterIsrManager alterIsrManager = TestUtils.createAlterIsrManager();
    private KafkaConfig brokerProperties =  KafkaConfig.fromProps(TestUtils.createBrokerConfig(
            0, TestUtils.MockZkConnect(), true, true, 9092, Option.empty(), Option.empty(),
            Option.empty(), true, false, 0, false, 0, false, 0, Option.empty(), 1, true, 1,
            (short) 1));
    private LogDirFailureChannel failureChannel = new LogDirFailureChannel(brokerProperties.logDirs().size());
    private List<String> topics = new ArrayList<>();
    private Map<String, Uuid> topicIds = new HashMap<>();

    private LogManager logManager;
    private ReplicaManager replicaManager;
    private KafkaApis kafkaApis;
    private RequestChannel.Request request;

    @SuppressWarnings("deprecation")
    @Setup(Level.Trial)
    public void setup() {
        final List<File> files =
                JavaConverters.seqAsJavaList(brokerProperties.logDirs()).stream().map(File::new).collect(Collectors.toList());
        logManager = TestUtils.createLogManager(JavaConverters.asScalaBuffer(files),
                LogConfig.apply(), configRepository, CleanerConfig.apply(1, 4 * 1024 * 1024L, 0.9d,
                        1024 * 1024, 32 * 1024 * 1024,
                        Double.MAX_VALUE, 15 * 1000, true, "MD5"), time);

        scheduler.startup();
        for (int topicIdx = 0; topicIdx < topicCount; topicIdx++) {
            String topicName = Uuid.randomUuid().toString();
            topics.add(topicName);
            topicIds.put(topicName, Uuid.randomUuid());
        }
        initializeMetadataCache();
        replicaManager = createReplicaManager();
        kafkaApis = createKafkaApis();

        // Put LISR data into ReplicaManager
        kafkaApis.handleLeaderAndIsrRequest(buildLeaderAndIsrRequest());
        request = buildFetchRequest();

    }

    private ReplicaManager createReplicaManager() {
        final BrokerTopicStats brokerTopicStats = new BrokerTopicStats();
        KafkaZkClient zkClient = new KafkaZkClient(null, false, Time.SYSTEM) {
            @Override
            public Properties getEntityConfigs(String rootEntityType, String sanitizedEntityName) {
                return new Properties();
            }
        };
        ReplicaManager replicaManager = new ReplicaManager(
                brokerProperties,
                metrics,
                time,
                OptionConverters.toScala(Optional.of(zkClient)),
                scheduler,
                logManager,
                new AtomicBoolean(false),
                quotaManagers,
                brokerTopicStats,
                metadataCache,
                failureChannel,
                alterIsrManager,
                configRepository,
                Option.empty());
        replicaManager.startup();
        replicaManager.checkpointHighWatermarks();
        return replicaManager;
    }

    private KafkaApis createKafkaApis() {
        Properties kafkaProps =  new Properties();
        kafkaProps.put(KafkaConfig$.MODULE$.ZkConnectProp(), "zk");
        kafkaProps.put(KafkaConfig$.MODULE$.BrokerIdProp(), brokerId + "");
        return new KafkaApis(requestChannel,
                new ZkSupport(adminManager, kafkaController, kafkaZkClient, Option.empty(), metadataCache),
                replicaManager,
                groupCoordinator,
                transactionCoordinator,
                autoTopicCreationManager,
                brokerId,
                new KafkaConfig(kafkaProps),
                configRepository,
                metadataCache,
                metrics,
                Option.empty(),
                quotaManagers,
                fetchManager,
                brokerTopicStats,
                "clusterId",
                new SystemTime(),
                null,
                new SimpleApiVersionManager(ApiMessageType.ListenerType.ZK_BROKER));
    }

    private void initializeMetadataCache() {
        List<UpdateMetadataRequestData.UpdateMetadataBroker> liveBrokers = new LinkedList<>();
        List<UpdateMetadataRequestData.UpdateMetadataPartitionState> partitionStates = new LinkedList<>();

        IntStream.range(0, 5).forEach(brokerId -> liveBrokers.add(
                new UpdateMetadataRequestData.UpdateMetadataBroker().setId(brokerId)
                        .setEndpoints(endpoints(brokerId))
                        .setRack("rack1")));

        topics.forEach(topicName -> {

            IntStream.range(0, partitionCount).forEach(partitionId -> {
                partitionStates.add(
                        new UpdateMetadataRequestData.UpdateMetadataPartitionState().setTopicName(topicName)
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
                partitionStates, liveBrokers, topicIds).build();
        metadataCache.updateMetadata(100, updateMetadataRequest);
    }

    private List<UpdateMetadataRequestData.UpdateMetadataEndpoint> endpoints(final int brokerId) {
        return Collections.singletonList(
                new UpdateMetadataRequestData.UpdateMetadataEndpoint()
                        .setHost("host_" + brokerId)
                        .setPort(9092)
                        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
                        .setListener(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT).value()));
    }


    private RequestChannel.Request buildLeaderAndIsrRequest() {
        List<LeaderAndIsrPartitionState> partitionStates = new LinkedList<>();
        List<Node> liveLeaders = new LinkedList<>();
        Map<String, Uuid> topicIds = new HashMap<>();


        IntStream.range(0, 5).forEach(brokerId -> liveLeaders.add(
                new Node(brokerId, "host_" + brokerId, 9092, "rack1")));

        topics.forEach(topicName -> {

            IntStream.range(0, partitionCount).forEach(partitionId -> {
                partitionStates.add(
                        new LeaderAndIsrPartitionState().setTopicName(topicName)
                                .setPartitionIndex(partitionId)
                                .setControllerEpoch(1)
                                .setLeader(partitionCount % 5)
                                .setLeaderEpoch(0)
                                .setIsr(Arrays.asList(0, 1, 3))
                                .setZkVersion(1)
                                .setReplicas(Arrays.asList(0, 1, 3)));
            });
        });

        LeaderAndIsrRequest leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(
                ApiKeys.LEADER_AND_ISR.latestVersion(),
                1, 1, 1,
                partitionStates, topicIds, liveLeaders).build();
        RequestHeader header = new RequestHeader(leaderAndIsrRequest.apiKey(), leaderAndIsrRequest.version(), "", 0);
        ByteBuffer bodyBuffer = leaderAndIsrRequest.serialize();

        RequestContext context = new RequestContext(header, "1", null, principal,
                ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);
        return new RequestChannel.Request(1, context, 0, MemoryPool.NONE, bodyBuffer, requestChannelMetrics, Option.empty());
    }

    private RequestChannel.Request buildFetchRequest() {
        Map<TopicPartition, FetchRequest.PartitionData> fetchData = new HashMap<>();
        topics.forEach(topic -> {
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                        0, 0, 4096, Optional.empty());
                fetchData.put(new TopicPartition(topic, partitionId), partitionData);
            }
        });
        FetchRequest fetchRequest = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion(), ApiKeys.FETCH.latestVersion(),
                -1, 0, 0, fetchData).build();
        RequestHeader header = new RequestHeader(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion(), "jmh-benchmark", 100);
        ByteBuffer bodyBuffer = fetchRequest.serialize();

        RequestContext context = new RequestContext(header, "1", null, principal,
                ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);
        return new RequestChannel.Request(1, context, 0, MemoryPool.NONE, bodyBuffer, requestChannelMetrics, Option.empty());
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        kafkaApis.close();
        metrics.close();
        replicaManager.shutdown(false);
        scheduler.shutdown();
        quotaManagers.shutdown();
    }

    @Benchmark
    public void testHandleFetchRequest() {
        kafkaApis.handleFetchRequest(request);
    }

}