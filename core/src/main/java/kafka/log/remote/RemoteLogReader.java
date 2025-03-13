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
package kafka.log.remote;

import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.server.log.remote.quota.RLMQuotaManager;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class RemoteLogReader implements Callable<Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteLogReader.class);
    private final RemoteStorageFetchInfo fetchInfo;
    private final RemoteLogManager rlm;
    private final BrokerTopicStats brokerTopicStats;
    private final Consumer<RemoteLogReadResult> callback;
    private final RLMQuotaManager quotaManager;
    private final Timer remoteReadTimer;

    private volatile boolean cancelled = false;

    public RemoteLogReader(RemoteStorageFetchInfo fetchInfo,
                           RemoteLogManager rlm,
                           Consumer<RemoteLogReadResult> callback,
                           BrokerTopicStats brokerTopicStats,
                           RLMQuotaManager quotaManager,
                           Timer remoteReadTimer) {
        this.fetchInfo = fetchInfo;
        this.rlm = rlm;
        this.brokerTopicStats = brokerTopicStats;
        this.callback = callback;
        this.brokerTopicStats.topicStats(fetchInfo.topicPartition.topic()).remoteFetchRequestRate().mark();
        this.brokerTopicStats.allTopicsStats().remoteFetchRequestRate().mark();
        this.quotaManager = quotaManager;
        this.remoteReadTimer = remoteReadTimer;
    }

    public void cancel() {
        LOGGER.debug("Cancelling remote log reader for topic partition {}", fetchInfo.topicPartition);
        callback.accept(new RemoteLogReadResult(Optional.empty(), Optional.of(new InterruptedException("Cancelled remote log reader"))));
        this.cancelled = true;
    }

    @Override
    public Void call() {
        if (cancelled) {
            LOGGER.debug("Skipping reading records from remote storage for topic partition {} as it has been cancelled", fetchInfo.topicPartition);
            return null;
        }
        RemoteLogReadResult result;
        try {
            LOGGER.debug("Reading records from remote storage for topic partition {}", fetchInfo.topicPartition);
            FetchDataInfo fetchDataInfo = remoteReadTimer.time(() -> rlm.read(fetchInfo));
            brokerTopicStats.topicStats(fetchInfo.topicPartition.topic()).remoteFetchBytesRate().mark(fetchDataInfo.records.sizeInBytes());
            brokerTopicStats.allTopicsStats().remoteFetchBytesRate().mark(fetchDataInfo.records.sizeInBytes());
            result = new RemoteLogReadResult(Optional.of(fetchDataInfo), Optional.empty());
        } catch (OffsetOutOfRangeException e) {
            result = new RemoteLogReadResult(Optional.empty(), Optional.of(e));
        } catch (Exception e) {
            brokerTopicStats.topicStats(fetchInfo.topicPartition.topic()).failedRemoteFetchRequestRate().mark();
            brokerTopicStats.allTopicsStats().failedRemoteFetchRequestRate().mark();
            LOGGER.error("Error occurred while reading the remote data for {}", fetchInfo.topicPartition, e);
            result = new RemoteLogReadResult(Optional.empty(), Optional.of(e));
        }
        LOGGER.debug("Finished reading records from remote storage for topic partition {}", fetchInfo.topicPartition);
        quotaManager.record(result.fetchDataInfo.map(fetchDataInfo -> fetchDataInfo.records.sizeInBytes()).orElse(0));
        callback.accept(result);
        return null;
    }
}
