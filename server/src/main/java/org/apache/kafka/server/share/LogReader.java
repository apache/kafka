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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogReadResult;

import java.util.LinkedHashMap;
import java.util.Map;
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
     * The outcome of an asynchronous read for a single partition. Partial-data tolerant: {@code fetchDataInfo}
     * holds whatever data could be read (its {@link FetchDataInfo#records}), and {@code error} is
     * {@link Errors#NONE} on success or the failure reason otherwise.
     *
     * @param fetchDataInfo The data read for the partition.
     * @param error         {@link Errors#NONE} on success, otherwise the read failure.
     */
    record AsyncReadResult(FetchDataInfo fetchDataInfo, Errors error) {
    }

    /**
     * Read records for the given partitions starting at the specified offsets, combining the local read
     * and - when {@code readRemote} is true and the requested data has been tiered off the local log - the
     * follow-up remote read into a single call.
     *
     * <p>This is the asynchronous, remote-aware counterpart to {@link #read}: it returns one future per
     * requested partition. Partitions whose data is available locally (or whose local read failed) complete
     * immediately; partitions whose data is in remote storage complete later, once the remote read finishes
     * on the remote storage reader pool, so the caller's thread is never blocked on remote IO. When
     * {@code readRemote} is false, tiered offsets are simply omitted from the result rather than fetched.
     *
     * <p>Each per-partition result is partial-data tolerant (see {@link AsyncReadResult}); the read never
     * fails as a whole, allowing callers to use whatever records were retrieved and skip the rest.
     *
     * @param fetchParams                The fetch parameters (isolation level, maxBytes, etc.)
     * @param partitionsToFetch          The set of partitions to fetch
     * @param topicPartitionFetchOffsets The fetch offset per partition
     * @param partitionMaxBytes          The max bytes per partition
     * @param readRemote                 Whether to follow tiered offsets to the remote tier; when false,
     *                                   tiered offsets are skipped.
     * @return A map from partition to a future of that partition's {@link AsyncReadResult}.
     */
    Map<TopicIdPartition, CompletableFuture<AsyncReadResult>> readAsync(
        FetchParams fetchParams,
        Set<TopicIdPartition> partitionsToFetch,
        LinkedHashMap<TopicIdPartition, Long> topicPartitionFetchOffsets,
        LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes,
        boolean readRemote);
}
