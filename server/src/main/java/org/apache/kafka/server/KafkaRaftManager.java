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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.BootstrapConfiguration;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.raft.Endpoints;
import org.apache.kafka.raft.ExternalKRaftMetrics;
import org.apache.kafka.raft.FileQuorumStateStore;
import org.apache.kafka.raft.KafkaNetworkChannel;
import org.apache.kafka.raft.KafkaRaftClient;
import org.apache.kafka.raft.KafkaRaftClientDriver;
import org.apache.kafka.raft.MetadataLogConfig;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.raft.RaftLog;
import org.apache.kafka.raft.RaftManager;
import org.apache.kafka.raft.TimingWheelExpirationService;
import org.apache.kafka.raft.internals.KafkaRaftLog;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.server.util.FileLock;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.storage.internals.log.LogManager;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

/**
 * Manages the lifecycle of a Raft client, its network channel, and the
 * metadata log directory used by a broker or controller node.
 */
public class KafkaRaftManager<T> implements RaftManager<T> {

    private final AbstractKafkaConfig config;
    private final RecordSerde<T> serde;
    private final Time time;
    private final Metrics metrics;
    private final ExternalKRaftMetrics externalKRaftMetrics;
    private final String threadNamePrefix;
    private final CompletableFuture<Map<Integer, InetSocketAddress>> controllerQuorumVotersFuture;
    private final QuorumConfig raftConfig;
    private final LogContext logContext;
    private final Logger log;
    private final KafkaScheduler scheduler;
    private final File dataDir;
    private final Optional<FileLock> dataDirLock;
    private final RaftLog raftLog;
    private final KafkaNetworkChannel netChannel;
    private final SystemTimer expirationTimer;
    private final TimingWheelExpirationService expirationService;
    private final KafkaRaftClient<T> client;
    private final KafkaRaftClientDriver<T> clientDriver;

    public KafkaRaftManager(
        String clusterId,
        AbstractKafkaConfig config,
        Uuid metadataLogDirUuid,
        RecordSerde<T> serde,
        TopicPartition topicPartition,
        Uuid topicId,
        Time time,
        Metrics metrics,
        ExternalKRaftMetrics externalKRaftMetrics,
        Optional<String> threadNamePrefixOpt,
        CompletableFuture<Map<Integer, InetSocketAddress>> controllerQuorumVotersFuture,
        Collection<InetSocketAddress> bootstrapServers,
        Endpoints localListeners,
        FaultHandler fatalFaultHandler
    ) throws IOException {
        this.config = config;
        this.serde = serde;
        this.time = time;
        this.metrics = metrics;
        this.externalKRaftMetrics = externalKRaftMetrics;
        this.controllerQuorumVotersFuture = controllerQuorumVotersFuture;

        raftConfig = new QuorumConfig(config);
        threadNamePrefix = threadNamePrefixOpt.orElse("kafka-raft");
        logContext = new LogContext("[RaftManager id=" + config.nodeId() + "] ");
        log = logContext.logger(KafkaRaftManager.class);

        KafkaScheduler scheduler = new KafkaScheduler(1, true, threadNamePrefix + "-scheduler");
        Optional<FileLock> dataDirLock = Optional.empty();
        RaftLog raftLog = null;
        KafkaNetworkChannel netChannel = null;
        SystemTimer expirationTimer = null;
        TimingWheelExpirationService expirationService = null;
        try {
            scheduler.startup();
            ApiVersions apiVersions = new ApiVersions();
            dataDir = createDataDir(topicPartition);
            dataDirLock = createFileLock();
            raftLog = buildMetadataLog(topicPartition, topicId, dataDir, scheduler);
            netChannel = buildNetworkChannel(apiVersions);
            expirationTimer = new SystemTimer("raft-expiration-executor");
            expirationService = new TimingWheelExpirationService(expirationTimer);
            client = buildRaftClient(
                clusterId, metadataLogDirUuid, bootstrapServers, localListeners, netChannel, raftLog, expirationService);
            clientDriver = new KafkaRaftClientDriver<>(
                client, threadNamePrefix, fatalFaultHandler, logContext);
        } catch (Throwable t) {
            closeResources(log, expirationService, expirationTimer, null, scheduler, netChannel, raftLog, dataDirLock);
            throw t;
        }
        this.scheduler = scheduler;
        this.dataDirLock = dataDirLock;
        this.raftLog = raftLog;
        this.netChannel = netChannel;
        this.expirationTimer = expirationTimer;
        this.expirationService = expirationService;
    }

    @Override
    public RaftLog raftLog() {
        return raftLog;
    }

    @Override
    public KafkaRaftClient<T> client() {
        return client;
    }

    @Override
    public RecordSerde<T> recordSerde() {
        return serde;
    }

    public void startup() throws ExecutionException, InterruptedException {
        client.initialize(
            controllerQuorumVotersFuture.get(),
            new FileQuorumStateStore(new File(dataDir, FileQuorumStateStore.DEFAULT_FILE_NAME)),
            metrics,
            externalKRaftMetrics
        );
        netChannel.start();
        clientDriver.start();
    }

    public void shutdown() {
        closeResources(log, expirationService, expirationTimer, clientDriver, scheduler, netChannel, raftLog, dataDirLock);
    }

    @Override
    public CompletionStage<ApiMessage> handleRequest(
        RequestContext context,
        RequestHeader header,
        ApiMessage request,
        long createdTimeMs
    ) {
        return clientDriver.handleRequest(context, header, request, createdTimeMs);
    }

    private static void closeResources(
        Logger log,
        TimingWheelExpirationService expirationService,
        SystemTimer expirationTimer,
        KafkaRaftClientDriver<?> clientDriver,
        KafkaScheduler scheduler,
        KafkaNetworkChannel netChannel,
        RaftLog raftLog,
        Optional<FileLock> dataDirLock
    ) {
        if (expirationService != null) {
            Utils.swallow(log, expirationService::shutdown);
        }
        Utils.closeQuietly(expirationTimer, "expiration timer");
        if (clientDriver != null) {
            Utils.swallow(log, clientDriver::shutdown);
        }
        if (scheduler != null) {
            Utils.swallow(log, scheduler::shutdown);
        }
        Utils.closeQuietly(netChannel, "net channel");
        Utils.closeQuietly(raftLog, "raft log");
        Utils.swallow(log, () -> {
            if (dataDirLock.isPresent()) {
                dataDirLock.get().destroy();
            }
        });
    }

    private static File createLogDirectory(File logDir, String logDirName) throws IOException {
        String logDirPath = logDir.getAbsolutePath();
        File dir = new File(logDirPath, logDirName);
        Files.createDirectories(dir.toPath());
        return dir;
    }

    private static FileLock lockDataDir(File dataDir) throws IOException {
        FileLock lock = new FileLock(new File(dataDir, LogManager.LOCK_FILE_NAME));
        if (!lock.tryLock()) {
            throw new KafkaException(
                "Failed to acquire lock on file .lock in " + lock.file().getParent() +
                ". A Kafka instance in another process or thread is using this directory."
            );
        }
        return lock;
    }

    /**
     * Returns true if the configured metadata log dir is not one of the data log dirs.
     */
    private static boolean hasDifferentLogDir(AbstractKafkaConfig config) {
        Path metadataPath = Paths.get(config.metadataLogDir()).toAbsolutePath();
        return config.logDirs().stream()
            .map(d -> Paths.get(d).toAbsolutePath())
            .noneMatch(metadataPath::equals);
    }

    private File createDataDir(TopicPartition topicPartition) throws IOException {
        String logDirName = UnifiedLog.logDirName(topicPartition);
        return createLogDirectory(new File(config.metadataLogDir()), logDirName);
    }

    private Optional<FileLock> createFileLock() throws IOException {
        // Acquire the log dir lock if the metadata log dir is different from the log dirs
        boolean differentMetadataLogDir = hasDifferentLogDir(config);
        // Or this node is only a controller
        boolean isOnlyController = config.processRoles().equals(Set.of(ProcessRole.ControllerRole));

        if (differentMetadataLogDir || isOnlyController) {
            FileLock fileLock = lockDataDir(new File(config.metadataLogDir()));
            return Optional.of(fileLock);
        }
        return Optional.empty();
    }

    private KafkaRaftLog buildMetadataLog(
        TopicPartition topicPartition,
        Uuid topicId,
        File dataDir,
        KafkaScheduler scheduler
    ) throws IOException {
        return KafkaRaftLog.createLog(
            topicPartition,
            topicId,
            dataDir,
            time,
            scheduler,
            new MetadataLogConfig(config),
            config.nodeId()
        );
    }

    private KafkaNetworkChannel buildNetworkChannel(ApiVersions apiVersions) {
        ListenerName listenerName = buildControllerListenerName();
        NetworkClient netClient = buildNetworkClient(listenerName, apiVersions);
        return new KafkaNetworkChannel(time, listenerName, netClient, raftConfig.requestTimeoutMs(), threadNamePrefix);
    }

    private ListenerName buildControllerListenerName() {
        return new ListenerName(config.controllerListenerNames().get(0));
    }

    private NetworkClient buildNetworkClient(
        ListenerName controllerListenerName,
        ApiVersions apiVersions
    ) {
        SecurityProtocol controllerSecurityProtocol =
            Optional.ofNullable(config.effectiveListenerSecurityProtocolMap().get(controllerListenerName))
                .orElseGet(() -> SecurityProtocol.forName(controllerListenerName.value()));
        ChannelBuilder channelBuilder = ChannelBuilders.clientChannelBuilder(
            controllerSecurityProtocol,
            JaasContext.Type.SERVER,
            config,
            controllerListenerName,
            config.saslMechanismControllerProtocol(),
            time,
            logContext);

        if (channelBuilder instanceof Reconfigurable reconfigurable) {
            config.addReconfigurable(reconfigurable);
        }

        String metricGroupPrefix = "raft-channel";
        boolean collectPerConnectionMetrics = false;

        Selector selector = new Selector(
            NetworkReceive.UNLIMITED,
            config.connectionsMaxIdleMs(),
            metrics,
            time,
            metricGroupPrefix,
            Map.of(),
            collectPerConnectionMetrics,
            channelBuilder,
            logContext);

        String clientId = "raft-client-" + config.nodeId();
        int maxInflightRequestsPerConnection = 1;
        int reconnectBackoffMs = 50;
        int reconnectBackoffMaxMs = 500;
        boolean discoverBrokerVersions = true;

        return new NetworkClient(
            selector,
            new ManualMetadataUpdater(),
            clientId,
            maxInflightRequestsPerConnection,
            reconnectBackoffMs,
            reconnectBackoffMaxMs,
            Selectable.USE_DEFAULT_BUFFER_SIZE,
            config.socketReceiveBufferBytes(),
            raftConfig.requestTimeoutMs(),
            config.connectionSetupTimeoutMs(),
            config.connectionSetupTimeoutMaxMs(),
            time,
            discoverBrokerVersions,
            apiVersions,
            logContext,
            MetadataRecoveryStrategy.NONE,
            BootstrapConfiguration.DISABLED,
            false
        );
    }

    private KafkaRaftClient<T> buildRaftClient(
        String clusterId,
        Uuid metadataLogDirUuid,
        Collection<InetSocketAddress> bootstrapServers,
        Endpoints localListeners,
        KafkaNetworkChannel netChannel,
        RaftLog raftLog,
        TimingWheelExpirationService expirationService
    ) {
        return new KafkaRaftClient<>(
            OptionalInt.of(config.nodeId()),
            metadataLogDirUuid,
            serde,
            netChannel,
            raftLog,
            time,
            expirationService,
            logContext,
            // Controllers should always flush the log on replication because they may become voters
            config.processRoles().contains(ProcessRole.ControllerRole),
            clusterId,
            bootstrapServers,
            localListeners,
            Feature.KRAFT_VERSION.supportedVersionRange(config.unstableFeatureVersionsEnabled()),
            raftConfig
        );
    }
}
