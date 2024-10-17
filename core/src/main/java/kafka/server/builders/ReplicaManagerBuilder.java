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

package kafka.server.builders;

import kafka.log.LogManager;
import kafka.log.remote.RemoteLogManager;
import kafka.server.AddPartitionsToTxnManager;
import kafka.server.AlterPartitionManager;
import kafka.server.DelayedActionQueue;
import kafka.server.DelayedDeleteRecords;
import kafka.server.DelayedElectLeader;
import kafka.server.DelayedFetch;
import kafka.server.DelayedOperationPurgatory;
import kafka.server.DelayedProduce;
import kafka.server.DelayedRemoteFetch;
import kafka.server.DelayedRemoteListOffsets;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.QuotaFactory.QuotaManagers;
import kafka.server.ReplicaManager;
import kafka.zk.KafkaZkClient;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.DirectoryEventHandler;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.jdk.javaapi.OptionConverters;



public class ReplicaManagerBuilder {
    private KafkaConfig config = null;
    private Metrics metrics = null;
    private Time time = Time.SYSTEM;
    private Scheduler scheduler = null;
    private LogManager logManager = null;
    private QuotaManagers quotaManagers = null;
    private MetadataCache metadataCache = null;
    private LogDirFailureChannel logDirFailureChannel = null;
    private AlterPartitionManager alterPartitionManager = null;
    private BrokerTopicStats brokerTopicStats = null;
    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private Optional<RemoteLogManager> remoteLogManager = Optional.empty();
    private Optional<KafkaZkClient> zkClient = Optional.empty();
    private Optional<DelayedOperationPurgatory<DelayedProduce>> delayedProducePurgatory = Optional.empty();
    private Optional<DelayedOperationPurgatory<DelayedFetch>> delayedFetchPurgatory = Optional.empty();
    private Optional<DelayedOperationPurgatory<DelayedDeleteRecords>> delayedDeleteRecordsPurgatory = Optional.empty();
    private Optional<DelayedOperationPurgatory<DelayedElectLeader>> delayedElectLeaderPurgatory = Optional.empty();
    private Optional<DelayedOperationPurgatory<DelayedRemoteFetch>> delayedRemoteFetchPurgatory = Optional.empty();
    private Optional<DelayedOperationPurgatory<DelayedRemoteListOffsets>> delayedRemoteListOffsetsPurgatory = Optional.empty();
    private Optional<String> threadNamePrefix = Optional.empty();
    private Long brokerEpoch = -1L;
    private Optional<AddPartitionsToTxnManager> addPartitionsToTxnManager = Optional.empty();
    private DirectoryEventHandler directoryEventHandler = DirectoryEventHandler.NOOP;

    public ReplicaManagerBuilder setConfig(KafkaConfig config) {
        this.config = config;
        return this;
    }

    public ReplicaManagerBuilder setMetrics(Metrics metrics) {
        this.metrics = metrics;
        return this;
    }

    public ReplicaManagerBuilder setTime(Time time) {
        this.time = time;
        return this;
    }

    public ReplicaManagerBuilder setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    public ReplicaManagerBuilder setLogManager(LogManager logManager) {
        this.logManager = logManager;
        return this;
    }

    public ReplicaManagerBuilder setRemoteLogManager(RemoteLogManager remoteLogManager) {
        this.remoteLogManager = Optional.ofNullable(remoteLogManager);
        return this;
    }

    public ReplicaManagerBuilder setQuotaManagers(QuotaManagers quotaManagers) {
        this.quotaManagers = quotaManagers;
        return this;
    }

    public ReplicaManagerBuilder setMetadataCache(MetadataCache metadataCache) {
        this.metadataCache = metadataCache;
        return this;
    }

    public ReplicaManagerBuilder setLogDirFailureChannel(LogDirFailureChannel logDirFailureChannel) {
        this.logDirFailureChannel = logDirFailureChannel;
        return this;
    }

    public ReplicaManagerBuilder setAlterPartitionManager(AlterPartitionManager alterPartitionManager) {
        this.alterPartitionManager = alterPartitionManager;
        return this;
    }

    public ReplicaManagerBuilder setBrokerTopicStats(BrokerTopicStats brokerTopicStats) {
        this.brokerTopicStats = brokerTopicStats;
        return this;
    }

    public ReplicaManagerBuilder setIsShuttingDown(AtomicBoolean isShuttingDown) {
        this.isShuttingDown = isShuttingDown;
        return this;
    }

    public ReplicaManagerBuilder setZkClient(KafkaZkClient zkClient) {
        this.zkClient = Optional.of(zkClient);
        return this;
    }

    public ReplicaManagerBuilder setDelayedProducePurgatory(DelayedOperationPurgatory<DelayedProduce> delayedProducePurgatory) {
        this.delayedProducePurgatory = Optional.of(delayedProducePurgatory);
        return this;
    }

    public ReplicaManagerBuilder setDelayedFetchPurgatory(DelayedOperationPurgatory<DelayedFetch> delayedFetchPurgatory) {
        this.delayedFetchPurgatory = Optional.of(delayedFetchPurgatory);
        return this;
    }

    public ReplicaManagerBuilder setDelayedRemoteFetchPurgatory(DelayedOperationPurgatory<DelayedRemoteFetch> delayedRemoteFetchPurgatory) {
        this.delayedRemoteFetchPurgatory = Optional.of(delayedRemoteFetchPurgatory);
        return this;
    }

    public ReplicaManagerBuilder setDelayedDeleteRecordsPurgatory(DelayedOperationPurgatory<DelayedDeleteRecords> delayedDeleteRecordsPurgatory) {
        this.delayedDeleteRecordsPurgatory = Optional.of(delayedDeleteRecordsPurgatory);
        return this;
    }

    public ReplicaManagerBuilder setDelayedElectLeaderPurgatoryParam(DelayedOperationPurgatory<DelayedElectLeader> delayedElectLeaderPurgatory) {
        this.delayedElectLeaderPurgatory = Optional.of(delayedElectLeaderPurgatory);
        return this;
    }

    public ReplicaManagerBuilder setThreadNamePrefix(String threadNamePrefix) {
        this.threadNamePrefix = Optional.of(threadNamePrefix);
        return this;
    }

    public ReplicaManagerBuilder setBrokerEpoch(long brokerEpoch) {
        this.brokerEpoch = brokerEpoch;
        return this;
    }

    public ReplicaManagerBuilder setAddPartitionsToTransactionManager(AddPartitionsToTxnManager addPartitionsToTxnManager) {
        this.addPartitionsToTxnManager = Optional.of(addPartitionsToTxnManager);
        return this;
    }

    public ReplicaManagerBuilder setDirectoryEventHandler(DirectoryEventHandler directoryEventHandler) {
        this.directoryEventHandler = directoryEventHandler;
        return this;
    }

    public ReplicaManager build() {
        if (config == null) config = new KafkaConfig(Collections.emptyMap());
        if (logManager == null) throw new RuntimeException("You must set logManager");
        if (metadataCache == null) throw new RuntimeException("You must set metadataCache");
        if (logDirFailureChannel == null) throw new RuntimeException("You must set logDirFailureChannel");
        if (alterPartitionManager == null) throw new RuntimeException("You must set alterIsrManager");
        if (brokerTopicStats == null) brokerTopicStats = new BrokerTopicStats(config.remoteLogManagerConfig().isRemoteStorageSystemEnabled());
        // Initialize metrics in the end just before passing it to ReplicaManager to ensure ReplicaManager closes the
        // metrics correctly. There might be a resource leak if it is initialized and an exception occurs between
        // its initialization and creation of ReplicaManager.
        if (metrics == null) metrics = new Metrics();
        return new ReplicaManager(config,
                             metrics,
                             time,
                             scheduler,
                             logManager,
                             OptionConverters.toScala(remoteLogManager),
                             quotaManagers,
                             metadataCache,
                             logDirFailureChannel,
                             alterPartitionManager,
                             brokerTopicStats,
                             isShuttingDown,
                             OptionConverters.toScala(zkClient),
                             OptionConverters.toScala(delayedProducePurgatory),
                             OptionConverters.toScala(delayedFetchPurgatory),
                             OptionConverters.toScala(delayedDeleteRecordsPurgatory),
                             OptionConverters.toScala(delayedElectLeaderPurgatory),
                             OptionConverters.toScala(delayedRemoteFetchPurgatory),
                             OptionConverters.toScala(delayedRemoteListOffsetsPurgatory),
                             OptionConverters.toScala(threadNamePrefix),
                             () -> brokerEpoch,
                             OptionConverters.toScala(addPartitionsToTxnManager),
                             directoryEventHandler,
                             new DelayedActionQueue());
    }
}
