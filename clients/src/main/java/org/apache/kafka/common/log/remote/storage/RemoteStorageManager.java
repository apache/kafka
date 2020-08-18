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
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.InputStream;

/**
 * RemoteStorageManager provides the lifecycle of remote log segments that includes copy, fetch, and delete from remote
 * storage.
 * <p>
 * Each upload or copy of a segment is initiated with {@link RemoteLogSegmentMetadata} containing {@link RemoteLogSegmentId}
 * which is universally unique even for the same topic partition and offsets.
 * <p>
 * RemoteLogSegmentMetadata is stored in {@link RemoteLogMetadataManager} before and after copy/delete operations on
 * RemoteStorageManager with the respective {@link RemoteLogSegmentMetadata.State}. {@link RemoteLogMetadataManager} is
 * responsible for storing and fetching metadata about the remote log segments in a strongly consistent manner.
 * This allows RemoteStorageManager to store segments even in eventually consistent manner as the metadata is already
 * stored in a consistent store.
 * <p>
 * All these APIs are still evolving.
 */
@InterfaceStability.Unstable
public interface RemoteStorageManager extends Configurable, Closeable {
    InputStream EMPTY_INPUT_STREAM = new ByteArrayInputStream(new byte[0]);

    /**
     * Copies LogSegmentData provided for the given {@param remoteLogSegmentMetadata}.
     * <p>
     * Invoker of this API should always send a unique id as part of {@link RemoteLogSegmentMetadata#remoteLogSegmentId()#id()}
     * even when it retries to invoke this method for the same log segment data.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @param logSegmentData           data to be copied to tiered storage.
     * @throws RemoteStorageException if there are any errors in storing the data of the segment.
     */
    void copyLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, LogSegmentData logSegmentData)
            throws RemoteStorageException;

    /**
     * Returns the remote log segment data file/object as InputStream for the given RemoteLogSegmentMetadata starting
     * from the given startPosition. The stream will end at the smaller of endPosition and the end of the remote log
     * segment data file/object.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @param startPosition            start position of log segment to be read, inclusive.
     * @param endPosition              end position of log segment to be read, inclusive.
     * @return input stream of the requested log segment data.
     * @throws RemoteStorageException if there are any errors while fetching the desired segment.
     */
    InputStream fetchLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                    Long startPosition, Long endPosition) throws RemoteStorageException;

    /**
     * Returns the offset index for the respective log segment of {@link RemoteLogSegmentMetadata}.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @return input stream of the requested  offset index.
     * @throws RemoteStorageException if there are any errors while fetching the index.
     */
    InputStream fetchOffsetIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException;

    /**
     * Returns the timestamp index for the respective log segment of {@link RemoteLogSegmentMetadata}.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @return input stream of the requested  timestamp index.
     * @throws RemoteStorageException if there are any errors while fetching the index.
     */
    InputStream fetchTimestampIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException;

    /**
     * Returns the transaction index for the the respective log segment of {@link RemoteLogSegmentMetadata}.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @return input stream of the requested  transaction index.
     * @throws RemoteStorageException if there are any errors while fetching the index.
     */
    default InputStream fetchTransactionIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        return EMPTY_INPUT_STREAM;
    }

    /**
     * Returns the producer snapshot index for the the respective log segment of {@link RemoteLogSegmentMetadata}.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @return input stream of the producer snapshot.
     * @throws RemoteStorageException if there are any errors while fetching the index.
     */
    default InputStream fetchProducerSnapshotIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        return EMPTY_INPUT_STREAM;
    }

    /**
     * Returns the leader epoch index for the the respective log segment of {@link RemoteLogSegmentMetadata}.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @return input stream of the leader epoch index.
     * @throws RemoteStorageException if there are any errors while fetching the index.
     */
    default InputStream fetchLeaderEpochIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata)
            throws RemoteStorageException {
        return EMPTY_INPUT_STREAM;
    }

    /**
     * Deletes the resources associated with the given {@param remoteLogSegmentMetadata}. Deletion is considered as
     * successful if this call returns successfully without any errors. It will throw {@link RemoteStorageException} if
     * there are any errors in deleting the file.
     * <p>
     * {@link RemoteResourceNotFoundException} is thrown when there are no resources associated with the given
     * {@param remoteLogSegmentMetadata}.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment to be deleted.
     * @throws RemoteResourceNotFoundException if the requested resource is not found
     * @throws RemoteStorageException          if there are any storage related errors occurred.
     */
    void deleteLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException;

}
