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
     * Read records for the given partitions starting at the specified offsets, combining the local read
     * and - when {@code readRemote} is true and the requested data has been tiered off the local log - the
     * follow-up remote read into a single call.
     *
     * <p>The read is asynchronous and remote-aware: it returns a single future holding one {@link LogReadResult}
     * per requested partition. The future completes once every partition has resolved - partitions
     * available locally (or whose local read failed) resolve immediately, while partitions whose data
     * is in remote storage resolve later, once the remote read finishes on the remote storage reader
     * pool, so the caller's thread is never blocked on remote IO. When {@code readRemote} is false,
     * tiered offsets are simply omitted from the result rather than fetched.
     *
     * <p>Each per-partition {@link LogReadResult} is partial-data tolerant: the read never fails as a whole,
     * allowing callers to use whatever records were retrieved (via {@link LogReadResult#info()}) and skip
     * the rest based on {@link LogReadResult#error()}.
     *
     * <p>If the order of the fetched partitions matters to the caller, it should pass order-preserving
     * {@link Set} and {@link Map} implementations (for example {@link java.util.LinkedHashSet} and
     * {@link LinkedHashMap}). LogReader implementations must then honour that iteration order and return
     * the partitions in the same order in the resulting {@link LinkedHashMap}.
     *
     * @param fetchParams                The fetch parameters (isolation level, maxBytes, etc.)
     * @param partitionsToFetch          The set of partitions to fetch
     * @param topicPartitionFetchOffsets The fetch offset per partition
     * @param partitionMaxBytes          The max bytes per partition
     * @param readRemote                 Whether to follow tiered offsets to the remote tier; when false,
     *                                   tiered offsets are skipped.
     * @return A future of a map from partition to that partition's {@link LogReadResult}.
     */
    CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> readAsync(
        FetchParams fetchParams,
        Set<TopicIdPartition> partitionsToFetch,
        Map<TopicIdPartition, Long> topicPartitionFetchOffsets,
        Map<TopicIdPartition, Integer> partitionMaxBytes,
        boolean readRemote);
}
