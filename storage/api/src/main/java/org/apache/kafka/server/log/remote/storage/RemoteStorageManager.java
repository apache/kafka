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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;

import java.io.Closeable;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

/**
 * This interface provides the lifecycle of remote log segments that includes copy, fetch, and delete from remote
 * storage.
 * <p>
 * Each upload or copy of a segment is initiated with {@link RemoteLogSegmentMetadata} containing {@link RemoteLogSegmentId}
 * which is universally unique even for the same topic partition and offsets.
 * <p>
 * {@link RemoteLogSegmentMetadata} is stored in {@link RemoteLogMetadataManager} before and after copy/delete operations on
 * {@link RemoteStorageManager} with the respective {@link RemoteLogSegmentState}. {@link RemoteLogMetadataManager} is
 * responsible for storing and fetching metadata about the remote log segments in a strongly consistent manner.
 * This allows {@link RemoteStorageManager} to have eventual consistency on metadata (although the data is stored
 * in strongly consistent semantics).
 * <p>
 * All properties prefixed with the config: "remote.log.storage.manager.impl.prefix"
 * (default value is "rsm.config.") are passed when {@link #configure(Map)} is invoked on this instance.
 */
@InterfaceStability.Evolving
public interface RemoteStorageManager extends Configurable, Closeable {

    /**
     * Type of the index file.
     */
    enum IndexType {
        /**
         * Represents offset index.
         */
        OFFSET,

        /**
         * Represents timestamp index.
         */
        TIMESTAMP,

        /**
         * Represents producer snapshot index.
         */
        PRODUCER_SNAPSHOT,

        /**
         * Represents transaction index.
         */
        TRANSACTION,

        /**
         * Represents leader epoch index.
         */
        LEADER_EPOCH,
    }

    /**
     * Copies the given {@link LogSegmentData} provided for the given {@code remoteLogSegmentMetadata}. This includes
     * log segment and its auxiliary indexes like offset index, time index, transaction index, leader epoch index, and
     * producer snapshot index.
     * <p>
     * Invoker of this API should always send a unique id as part of {@link RemoteLogSegmentMetadata#remoteLogSegmentId()}
     * even when it retries to invoke this method for the same log segment data.
     * <p>
     * This operation is expected to be idempotent. If a copy operation is retried and there is existing content already written,
     * it should be overwritten, and do not throw {@link RemoteStorageException}
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @param logSegmentData           data to be copied to tiered storage.
     * @return custom metadata to be added to the segment metadata after copying.
     * @throws RemoteStorageException if there are any errors in storing the data of the segment.
     */
    Optional<CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                                LogSegmentData logSegmentData)
            throws RemoteStorageException;

    /**
     * Returns the remote log segment data file/object as InputStream for the given {@link RemoteLogSegmentMetadata}
     * starting from the given startPosition. The stream will end at the end of the remote log segment data file/object.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @param startPosition            start position of log segment to be read, inclusive.
     * @return input stream of the requested log segment data.
     * @throws RemoteStorageException          if there are any errors while fetching the desired segment.
     * @throws RemoteResourceNotFoundException the requested log segment is not found in the remote storage.
     */
    InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                int startPosition) throws RemoteStorageException;

    /**
     * Returns the remote log segment data file/object as InputStream for the given {@link RemoteLogSegmentMetadata}
     * starting from the given startPosition. The stream will end at the smaller of endPosition and the end of the
     * remote log segment data file/object.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @param startPosition            start position of log segment to be read, inclusive.
     * @param endPosition              end position of log segment to be read, inclusive.
     * @return input stream of the requested log segment data.
     * @throws RemoteStorageException          if there are any errors while fetching the desired segment.
     * @throws RemoteResourceNotFoundException the requested log segment is not found in the remote storage.
     */
    InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                int startPosition,
                                int endPosition) throws RemoteStorageException;

    /**
     * Returns the index for the respective log segment of {@link RemoteLogSegmentMetadata}.
     * <p>
     * Note: The transaction index may not exist because of no transactional records.
     * In this case, it should throw a RemoteResourceNotFoundException, instead of returning {@code null}.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @param indexType                type of the index to be fetched for the segment.
     * @return input stream of the requested index.
     * @throws RemoteStorageException          if there are any errors while fetching the index.
     * @throws RemoteResourceNotFoundException the requested index is not found in the remote storage
     */
    InputStream fetchIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                           IndexType indexType) throws RemoteStorageException;

    /**
     * Deletes the resources associated with the given {@code remoteLogSegmentMetadata}. Deletion is considered as
     * successful if this call returns successfully without any errors. It will throw {@link RemoteStorageException} if
     * there are any errors in deleting the file.
     * <p>
     * This operation is expected to be idempotent. If resources are not found, it is not expected to
     * throw {@link RemoteResourceNotFoundException} as it may be already removed from a previous attempt.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment to be deleted.
     * @throws RemoteStorageException          if there are any storage related errors occurred.
     */
    void deleteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException;
}