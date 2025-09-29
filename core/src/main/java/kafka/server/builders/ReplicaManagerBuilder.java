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
import kafka.server.AlterPartitionManager;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.QuotaFactory.QuotaManagers;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.DelayedActionQueue;
import org.apache.kafka.server.common.DirectoryEventHandler;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.Option;



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
                             Option.empty(),
                             quotaManagers,
                             metadataCache,
                             logDirFailureChannel,
                             alterPartitionManager,
                             brokerTopicStats,
                             new AtomicBoolean(false),
                             Option.empty(),
                             Option.empty(),
                             Option.empty(),
                             Option.empty(),
                             Option.empty(),
                             Option.empty(),
                             Option.empty(),
                             () ->  -1L,
                             Option.empty(),
                             DirectoryEventHandler.NOOP,
                             new DelayedActionQueue());
    }
}
