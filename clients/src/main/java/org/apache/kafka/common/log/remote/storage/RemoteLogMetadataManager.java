/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.record.FileRecords;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * This interface provides storing and fetching remote log segment metadata with strongly consistent semantics.
 *
 */
@InterfaceStability.Unstable
public interface RemoteLogMetadataManager extends Configurable, Closeable {

    /**
     * Stores RemoteLogSegmentMetadata for the given RemoteLogSegmentMetadata.
     *
     * @param remoteLogSegmentId
     * @param remoteLogSegmentMetadata
     * @throws IOException
     */
    void putRemoteLogSegmentData(RemoteLogSegmentId remoteLogSegmentId, RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws IOException;

    /**
     * Fetches RemoteLogSegmentId for the given topic partition which contains the given offset.
     *
     * @param topicPartition
     * @param offset
     * @return
     * @throws IOException
     */
    RemoteLogSegmentId getRemoteLogSegmentId(TopicPartition topicPartition, long offset) throws IOException;

    /**
     * Fetches RemoteLogSegmentMetadata for the given RemoteLogSegmentId.
     *
     * @param metadata
     * @return
     * @throws IOException
     */
    RemoteLogSegmentMetadata getRemoteLogSegmentMetadata(RemoteLogSegmentId metadata) throws IOException;

    /**
     * Earliest log offset if exists for the given topic partition in the remote storage. Return {@link Optional#empty()}
     * if there are no segments in the remote storage.
     *
     * @param tp
     * @return
     */
    Optional<Long> earliestLogOffset(TopicPartition tp) throws IOException;

    /**
     * List the remote log segment files of the given topicPartition.
     * The RemoteLogManager of a follower uses this method to find out the remote data for the given topic partition.
     *
     * @return List of remote segments, sorted by baseOffset in ascending order.
     */
    default List<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicPartition topicPartition) {
        return listRemoteLogSegments(topicPartition, 0);
    }

    /**
     *
     * @param topicPartition
     * @param minOffset
     * @return
     */
    List<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicPartition topicPartition, long minOffset);

}
