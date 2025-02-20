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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Interface used to decouple UnifiedLog and RemoteLogManager.
 */
public interface AsyncOffsetReader {

    /**
     * Retrieve the offset for the specified timestamp. UnifiedLog may call this method when handling ListOffsets
     * for segments copied to remote storage.
     * @param topicPartition The topic partition
     * @param timestamp The timestamp
     * @param startingOffset The log start offset
     * @param leaderEpochCache The leader epoch cache
     * @param searchLocalLog A supplier to call in case an offset can't be found in the remote storage
     * @return The AsyncOffsetReadFutureHolder containing the desired offset or an exception
     */
    AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> asyncOffsetRead(
            TopicPartition topicPartition,
            long timestamp,
            long startingOffset,
            LeaderEpochFileCache leaderEpochCache,
            Supplier<Optional<FileRecords.TimestampAndOffset>> searchLocalLog);
}
