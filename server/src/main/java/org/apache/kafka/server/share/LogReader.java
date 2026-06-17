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
package org.apache.kafka.server.share;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;

import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Abstraction for reading records from log.
 */
public interface LogReader {

    /**
     * Read records for the given partitions starting at the specified offsets.
     *
     * @param fetchParams             The fetch parameters (isolation level, maxBytes, etc.)
     * @param partitionsToFetch       The set of partitions to actually fetch (after filtering erroneous ones)
     * @param topicPartitionFetchOffsets The fetch offset per partition
     * @param partitionMaxBytes       The max bytes per partition
     * @return A map of partition to log read result
     */
    LinkedHashMap<TopicIdPartition, LogReadResult> read(
        FetchParams fetchParams,
        Set<TopicIdPartition> partitionsToFetch,
        LinkedHashMap<TopicIdPartition, Long> topicPartitionFetchOffsets,
        LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes);

    /**
     * Read records asynchronously from the remote tier for an offset that has been tiered off the
     * local log. The {@link RemoteStorageFetchInfo} is the descriptor surfaced by
     * {@link LogReadResult#info()} as {@link FetchDataInfo#delayedRemoteStorageFetch} when a
     * preceding {@link #read} determined that the requested data resides in remote storage.
     *
     * <p>The read is performed off-thread (on the remote storage reader pool) so that the caller's
     * thread is not blocked on remote storage IO. It is intended for low volume, best-effort reads
     * (e.g. copying records to a DLQ topic). The returned future completes exceptionally when remote
     * storage is not configured on the broker or the read could not be completed, allowing callers
     * to gracefully skip the data instead of failing.
     *
     * @param remoteStorageFetchInfo The remote fetch descriptor obtained from a prior local read.
     * @return A future that completes with the fetched data, or completes exceptionally if it could
     *         not be read remotely.
     */
    CompletableFuture<FetchDataInfo> readRemote(RemoteStorageFetchInfo remoteStorageFetchInfo);
}
