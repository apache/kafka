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
package kafka.server.share;

import kafka.server.QuotaFactory;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.log.remote.storage.RemoteLogManager;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;
import scala.jdk.javaapi.OptionConverters;
import scala.runtime.BoxedUnit;

/**
 * Implementation of {@link LogReader} that reads records from the local log
 * via {@link ReplicaManager#readFromLog}.
 */
public class ReplicaManagerLogReader implements LogReader {

    private static final Logger log = LoggerFactory.getLogger(ReplicaManagerLogReader.class);

    private final ReplicaManager replicaManager;

    public ReplicaManagerLogReader(ReplicaManager replicaManager) {
        this.replicaManager = replicaManager;
    }

    @Override
    public LinkedHashMap<TopicIdPartition, LogReadResult> read(
            FetchParams fetchParams,
            Set<TopicIdPartition> partitionsToFetch,
            LinkedHashMap<TopicIdPartition, Long> topicPartitionFetchOffsets,
            LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes) {

        if (partitionsToFetch.isEmpty()) {
            return new LinkedHashMap<>();
        }

        LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData = new LinkedHashMap<>();
        topicPartitionFetchOffsets.forEach((topicIdPartition, fetchOffset) ->
            topicPartitionData.put(topicIdPartition,
                new FetchRequest.PartitionData(
                    topicIdPartition.topicId(),
                    fetchOffset,
                    0,
                    partitionMaxBytes.get(topicIdPartition),
                    Optional.empty())
            ));

        Seq<Tuple2<TopicIdPartition, LogReadResult>> responseLogResult = replicaManager.readFromLog(
            fetchParams,
            CollectionConverters.asScala(
                partitionsToFetch.stream().map(topicIdPartition ->
                    new Tuple2<>(topicIdPartition, topicPartitionData.get(topicIdPartition))).collect(Collectors.toList())
            ),
            QuotaFactory.UNBOUNDED_QUOTA,
            true);

        LinkedHashMap<TopicIdPartition, LogReadResult> responseData = new LinkedHashMap<>();
        responseLogResult.foreach(tpLogResult -> {
            responseData.put(tpLogResult._1(), tpLogResult._2());
            return BoxedUnit.UNIT;
        });

        log.trace("Data successfully retrieved by replica manager: {}", responseData);
        return responseData;
    }

    // Reads asynchronously from the remote tier for an offset tiered off the local log. The
    // RemoteStorageFetchInfo is the descriptor surfaced by a prior local read as
    // FetchDataInfo#delayedRemoteStorageFetch. The read runs on the remote storage reader pool so the
    // caller's thread is not blocked; the future completes exceptionally when remote storage is not
    // configured or the read could not be completed. Used internally by readAsync (package-private so
    // it remains unit-testable).
    CompletableFuture<FetchDataInfo> readRemote(RemoteStorageFetchInfo remoteStorageFetchInfo) {
        CompletableFuture<FetchDataInfo> future = new CompletableFuture<>();

        Optional<RemoteLogManager> remoteLogManager = OptionConverters.toJava(replicaManager.remoteLogManager());
        if (remoteLogManager.isEmpty()) {
            future.completeExceptionally(new IllegalStateException(
                "Cannot read " + remoteStorageFetchInfo + " from remote storage as remote log manager is not configured."));
            return future;
        }

        try {
            // The read runs on the remote storage reader thread pool; the callback completes the
            // future on that pool's thread, so the caller's thread is never blocked on remote IO.
            remoteLogManager.get().asyncRead(remoteStorageFetchInfo, result -> {
                if (result.error().isPresent()) {
                    future.completeExceptionally(result.error().get());
                } else if (result.fetchDataInfo().isPresent()) {
                    future.complete(result.fetchDataInfo().get());
                } else {
                    future.completeExceptionally(new IllegalStateException(
                        "Remote read for " + remoteStorageFetchInfo + " returned neither data nor error."));
                }
            });
        } catch (Exception e) {
            // e.g. RejectedExecutionException if the reader pool is shutting down.
            log.warn("Unable to schedule remote read for {}.", remoteStorageFetchInfo, e);
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public Map<TopicIdPartition, CompletableFuture<AsyncReadResult>> readAsync(
            FetchParams fetchParams,
            Set<TopicIdPartition> partitionsToFetch,
            LinkedHashMap<TopicIdPartition, Long> topicPartitionFetchOffsets,
            LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes,
            boolean readRemote) {

        LinkedHashMap<TopicIdPartition, CompletableFuture<AsyncReadResult>> result = new LinkedHashMap<>();
        if (partitionsToFetch.isEmpty()) {
            return result;
        }

        // Perform the local read for all partitions once; remote follow-ups (if any) are issued per partition.
        LinkedHashMap<TopicIdPartition, LogReadResult> localReadResults =
            read(fetchParams, partitionsToFetch, topicPartitionFetchOffsets, partitionMaxBytes);

        for (TopicIdPartition topicIdPartition : partitionsToFetch) {
            LogReadResult logReadResult = localReadResults.get(topicIdPartition);
            if (logReadResult == null) {
                result.put(topicIdPartition, CompletableFuture.completedFuture(new AsyncReadResult(
                    FetchDataInfo.empty(topicPartitionFetchOffsets.getOrDefault(topicIdPartition, 0L)),
                    Errors.UNKNOWN_SERVER_ERROR)));
                continue;
            }

            FetchDataInfo localFetchDataInfo = logReadResult.info();
            Errors error = logReadResult.error();
            Optional<RemoteStorageFetchInfo> remoteStorageFetchInfo = localFetchDataInfo.delayedRemoteStorageFetch;

            // Return the local read directly when it carries data, when it failed, or when the data is tiered
            // but the caller does not want remote reads (those offsets are simply skipped).
            if (error != Errors.NONE || remoteStorageFetchInfo.isEmpty() || !readRemote) {
                result.put(topicIdPartition, CompletableFuture.completedFuture(
                    new AsyncReadResult(localFetchDataInfo, error)));
                continue;
            }

            // Tiered data - follow it to the remote tier asynchronously.
            result.put(topicIdPartition, readRemote(remoteStorageFetchInfo.get()).handle((remoteFetchDataInfo, exception) -> {
                if (exception != null) {
                    Throwable cause = exception instanceof CompletionException && exception.getCause() != null
                        ? exception.getCause() : exception;
                    log.warn("Unable to read partition {} from remote storage.", topicIdPartition, cause);
                    return new AsyncReadResult(localFetchDataInfo, Errors.forException(cause));
                }
                if (remoteFetchDataInfo == null) {
                    return new AsyncReadResult(localFetchDataInfo, Errors.UNKNOWN_SERVER_ERROR);
                }
                return new AsyncReadResult(remoteFetchDataInfo, Errors.NONE);
            }));
        }

        return result;
    }
}
