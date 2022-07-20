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

package kafka.shell;

import kafka.raft.KafkaRaftManager;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import kafka.server.MetaProperties;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.metadata.util.ClusterMetadataSource;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.raft.RaftConfig;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.shell.TrackingListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import scala.compat.java8.OptionConverters;


/**
 * The MetadataShellObserver attaches to a running Raft quorum as an observer.
 *
 * Note: nearly all of the code for the metadata shell is in the "shell" gradle module. However, this file and
 * MetadataShellTool.java are in the core module so that they can access KafkaRaftManager, which is also part of the
 * core gradle module.
 */
public class MetadataShellObserver implements ClusterMetadataSource {
    private static final Logger log = LoggerFactory.getLogger(MetadataShellObserver.class);
    private final CompletableFuture<Void> caughtUpFuture = new CompletableFuture<>();
    private final String quorumVoters;
    private final String clusterId;
    private final KafkaRaftManager<ApiMessageAndVersion> raftManager;
    private final Path tempDir;

    static MetadataShellObserver create(
        String quorumVoters,
        String clusterId
    ) throws Exception {
        Path tempDir = Files.createTempDirectory("MetadataShell");
        Exit.addShutdownHook("delete-metadata-shell-temp-dir", () -> cleanup(null, tempDir));
        KafkaRaftManager<ApiMessageAndVersion> raftManager = null;

        try {
            // In a few places we have to set a made-up node ID here, because the configuration code assumes that such
            // an ID will always be set. This made-up ID will not ever be sent over the wire, so using MAX_INT is fine.
            MetaProperties metaProperties = new MetaProperties(clusterId, Integer.MAX_VALUE);
            HashMap<String, Object> configMap = new HashMap<>();
            configMap.put(RaftConfig.QUORUM_VOTERS_CONFIG, quorumVoters);
            configMap.put(KafkaConfig.MetadataLogDirProp(), tempDir.toAbsolutePath().toString());
            configMap.put(KafkaConfig.ProcessRolesProp(), "broker");
            configMap.put(KafkaConfig.NodeIdProp(), String.valueOf(Integer.MAX_VALUE));
            configMap.put(KafkaConfig.ControllerListenerNamesProp(), "CONTROLLER");
            KafkaConfig config = new KafkaConfig(configMap);
            CompletableFuture<Map<Integer, RaftConfig.AddressSpec>> votersFuture = CompletableFuture.completedFuture(
                    RaftConfig.parseVoterConnections(config.quorumVoters()));
            raftManager = new KafkaRaftManager<>(
                    metaProperties,
                    config,
                    MetadataRecordSerde.INSTANCE,
                    KafkaRaftServer.MetadataPartition(),
                    KafkaRaftServer.MetadataTopicId(),
                    Time.SYSTEM,
                    new Metrics(),
                    OptionConverters.toScala(Optional.of("MetadataShellObserver")),
                    votersFuture);
            return new MetadataShellObserver(quorumVoters, clusterId, raftManager, tempDir);
        } catch (Throwable e) {
            cleanup(raftManager, tempDir);
            throw e;
        }
    }

    static void cleanup(
        KafkaRaftManager<ApiMessageAndVersion> raftManager,
        Path tempDir
    ) {
        if (raftManager != null) {
            try {
                raftManager.shutdown();
            } catch (Exception e) {
                log.error("Got exception while shutting down raftManager", e);
            }
        }
        if (tempDir != null) {
            try {
                Utils.delete(tempDir.toFile());
            } catch (Exception e) {
                log.error("Got exception while removing temporary directory {}", e);
            }
        }
    }

    public MetadataShellObserver(
        String quorumVoters,
        String clusterId,
        KafkaRaftManager<ApiMessageAndVersion> raftManager,
        Path tempDir
    ) {
        this.quorumVoters = quorumVoters;
        this.clusterId = clusterId;
        this.raftManager = raftManager;
        this.tempDir = tempDir;
    }

    @Override
    public void start(RaftClient.Listener<ApiMessageAndVersion> listener) throws Exception {
        TrackingListener<ApiMessageAndVersion> trackingListener = new TrackingListener<>(
                caughtUpFuture,
                () -> raftManager.client().highWatermark(),
                listener);
        raftManager.register(trackingListener);
        raftManager.startup();
    }

    @Override
    public CompletableFuture<Void> caughtUpFuture() {
        return caughtUpFuture;
    }

    @Override
    public void close() throws Exception {
        cleanup(raftManager, tempDir);
    }

    @Override
    public String toString() {
        return "MetadataShellObserver(quorumVoters=" + quorumVoters + ", clusterId=" + clusterId + ")";
    }
}
