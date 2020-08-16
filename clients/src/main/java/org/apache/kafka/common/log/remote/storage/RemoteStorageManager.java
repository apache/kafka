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
 * RemoteStorageManager provides the lifecycle of remote log segments which includes copy, fetch, and delete operations.
 * <p>
 * {@link RemoteLogMetadataManager} is responsible for storing and fetching metadata about the remote log segments in a
 * strongly consistent manner.
 * <p>
 * Each upload or copy of a segment is given with a {@link RemoteLogSegmentId} which is universally unique even for the
 * same topic partition and offsets. Once the copy or upload is successful, {@link RemoteLogSegmentMetadata} is
 * created with RemoteLogSegmentId and other log segment information and it is stored in {@link RemoteLogMetadataManager}.
 * This allows RemoteStorageManager to store segments even in eventually consistent manner as the metadata is already
 * stored in a consistent store.
 * <p>
 * All these APIs are still experimental.
 */
@InterfaceStability.Unstable
public interface RemoteStorageManager extends Configurable, Closeable {
    InputStream EMPTY_INPUT_STREAM = new ByteArrayInputStream(new byte[0]);

    /**
     * Copies LogSegmentData provided for the given RemoteLogSegmentId and returns any contextual
     * information about this copy operation. This can include path to the object in the store etc.
     * <p>
     * Invoker of this API should always send a unique id as part of {@link RemoteLogSegmentId#id()} even when it
     * retries to invoke this method for the same log segment data.
     *
     * @param remoteLogSegmentMetadata
     * @param logSegmentData
     * @throws RemoteStorageException
     */
    void copyLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, LogSegmentData logSegmentData)
            throws RemoteStorageException;

    /**
     * Returns the remote log segment data file/object as InputStream for the given RemoteLogSegmentMetadata starting
     * from the given startPosition. The stream will end at the smaller of endPosition and the end of the remote log
     * segment data file/object.
     *
     * @param remoteLogSegmentMetadata
     * @param startPosition
     * @param endPosition
     * @return
     * @throws RemoteStorageException
     */
    InputStream fetchLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                    Long startPosition, Long endPosition) throws RemoteStorageException;

    /**
     * Returns the offset index for the respective log segment of {@link RemoteLogSegmentMetadata}.
     *
     * @param remoteLogSegmentMetadata
     * @return
     * @throws RemoteStorageException
     */
    InputStream fetchOffsetIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException;

    /**
     * Returns the timestamp index for the respective log segment of {@link RemoteLogSegmentMetadata}.
     *
     * @param remoteLogSegmentMetadata
     * @return
     * @throws RemoteStorageException
     */
    InputStream fetchTimestampIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException;

    /**
     * Returns the transaction index for the the respective log segment of {@link RemoteLogSegmentMetadata}.
     *
     * @param remoteLogSegmentMetadata
     * @return
     * @throws RemoteStorageException
     */
    default InputStream fetchTransactionIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        return EMPTY_INPUT_STREAM;
    }

    /**
     * Returns the producer snapshot index for the the respective log segment of {@link RemoteLogSegmentMetadata}.
     *
     * @param remoteLogSegmentMetadata
     * @return
     * @throws RemoteStorageException
     */
    default InputStream fetchProducerSnapshotIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        return EMPTY_INPUT_STREAM;
    }

    /**
     * Returns the leader epoch index for the the respective log segment of {@link RemoteLogSegmentMetadata}.
     *
     * @param remoteLogSegmentMetadata
     * @return
     * @throws RemoteStorageException
     */
    default InputStream fetchLeaderEpochIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        return EMPTY_INPUT_STREAM;
    }

    /**
     * Deletes the remote log segment for the given remoteLogSegmentMetadata. Deletion is considered as successful if
     * this call returns successfully without any exceptions. It will throw {@link RemoteStorageException} if there are
     * any errors in deleting the file.
     * <p>
     * Broker pushes an event to __delete_failed_remote_log_segments topic for failed segment deletions so that users
     * can do the cleanup later.
     *
     * @param remoteLogSegmentMetadata
     * @throws RemoteResourceNotFoundException
     * @throws RemoteStorageException
     */
    void deleteLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteResourceNotFoundException, RemoteStorageException;

}
