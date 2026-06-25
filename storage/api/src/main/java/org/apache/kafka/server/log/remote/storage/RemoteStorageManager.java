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
 *
 * Implement {@link org.apache.kafka.common.metrics.Monitorable} to enable the manager to register metrics.
 * The following tags are automatically added to all metrics registered: <code>config</code> set to
 * <code>remote.log.storage.manager.class.name</code>, and <code>class</code> set to the RemoteStorageManager class name.
 * <p>
 * Plugin implementors of {@link RemoteStorageManager} should throw {@link RetriableRemoteStorageException}
 * for transient errors that can be recovered by retrying. For non-recoverable errors,
 * {@link RemoteStorageException} should be thrown. This distinction allows RemoteLogManager to
 * handle retries gracefully and report metrics accurately.
 */
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
     * @throws RemoteStorageException          if there are any errors in storing the data of the segment.
     * @throws RetriableRemoteStorageException if the error is transient and the operation can be retried.
     */
    Optional<CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                                LogSegmentData logSegmentData) throws RemoteStorageException;

    /**
     * Returns the remote log segment data file/object as InputStream for the given {@link RemoteLogSegmentMetadata}
     * starting from the given startPosition. The stream will end at the end of the remote log segment data file/object.
     *
     * <p><b>Override contract:</b> a {@link RemoteStorageManager} implementation must override either this
     * int-overload OR the long-overload ({@link #fetchLogSegment(RemoteLogSegmentMetadata, long)}). New plugins
     * should override the long-overload to support segments larger than 2GiB (KIP-1333). Failing to override
     * either will cause {@link UnsupportedOperationException} at runtime.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @param startPosition            start position of log segment to be read, inclusive.
     * @return input stream of the requested log segment data.
     * @throws RemoteStorageException          if there are any errors while fetching the desired segment.
     * @throws RemoteResourceNotFoundException the requested log segment is not found in the remote storage.
     * @deprecated Use {@link #fetchLogSegment(RemoteLogSegmentMetadata, long)} instead.
     *             Slated for removal in a future major release.
     */
    @Deprecated(since = "4.4")
    default InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                        int startPosition) throws RemoteStorageException {
        throw new UnsupportedOperationException(
            "RemoteStorageManager " + getClass().getName() + " has not overridden either " +
            "fetchLogSegment(RemoteLogSegmentMetadata, int) or fetchLogSegment(RemoteLogSegmentMetadata, long). " +
            "New plugins must override the long-overload (KIP-1333).");
    }

    /**
     * Returns the remote log segment data file/object as InputStream for the given {@link RemoteLogSegmentMetadata}
     * starting from the given startPosition. The stream will end at the end of the remote log segment data file/object.
     * <p>
     * This overload accepts a {@code long} start position to support segments larger than 2GB.
     * The default implementation delegates to {@link #fetchLogSegment(RemoteLogSegmentMetadata, int)} when the
     * position fits in an int, and throws {@link RemoteStorageException} otherwise. Implementations that support
     * segments larger than 2GiB <b>must</b> override this method to handle the full long range.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @param startPosition            start position of log segment to be read, inclusive.
     * @return input stream of the requested log segment data.
     * @throws RemoteStorageException          if there are any errors while fetching the desired segment, or if
     *                                         {@code startPosition > Integer.MAX_VALUE} and this method has not been
     *                                         overridden to support large segments.
     * @throws RemoteResourceNotFoundException the requested log segment is not found in the remote storage.
     */
    default InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                        long startPosition) throws RemoteStorageException {
        if (startPosition > Integer.MAX_VALUE) {
            throw new RemoteStorageException("Start position " + startPosition +
                " exceeds Integer.MAX_VALUE but this RemoteStorageManager implementation has not overridden " +
                "fetchLogSegment(RemoteLogSegmentMetadata, long). Override the long-overload to support " +
                "segments larger than 2GiB (KIP-1333).");
        }
        return fetchLogSegment(remoteLogSegmentMetadata, (int) startPosition);
    }

    /**
     * Returns the remote log segment data file/object as InputStream for the given {@link RemoteLogSegmentMetadata}
     * starting from the given startPosition. The stream will end at the smaller of endPosition and the end of the
     * remote log segment data file/object.
     *
     * <p><b>Override contract:</b> see {@link #fetchLogSegment(RemoteLogSegmentMetadata, int)}.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @param startPosition            start position of log segment to be read, inclusive.
     * @param endPosition              end position of log segment to be read, inclusive.
     * @return input stream of the requested log segment data.
     * @throws RemoteStorageException          if there are any errors while fetching the desired segment.
     * @throws RemoteResourceNotFoundException the requested log segment is not found in the remote storage.
     * @deprecated Use {@link #fetchLogSegment(RemoteLogSegmentMetadata, long, long)} instead.
     *             Slated for removal in a future major release.
     */
    @Deprecated(since = "4.4")
    default InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                        int startPosition,
                                        int endPosition) throws RemoteStorageException {
        throw new UnsupportedOperationException(
            "RemoteStorageManager " + getClass().getName() + " has not overridden either " +
            "fetchLogSegment(..., int, int) or fetchLogSegment(..., long, long). " +
            "New plugins must override the long-overload (KIP-1333).");
    }

    /**
     * Returns the remote log segment data file/object as InputStream for the given {@link RemoteLogSegmentMetadata}
     * starting from the given startPosition. The stream will end at the smaller of endPosition and the end of the
     * remote log segment data file/object.
     * <p>
     * This overload accepts {@code long} positions to support segments larger than 2GB.
     * The default implementation delegates to {@link #fetchLogSegment(RemoteLogSegmentMetadata, int, int)} when both
     * positions fit in an int, and throws {@link RemoteStorageException} otherwise. Implementations that support
     * segments larger than 2GB <b>must</b> override this method to handle the full long range.
     * <p>
     * <b>Plugin implementation contract:</b> see
     * {@link #fetchLogSegment(RemoteLogSegmentMetadata, long)} for the override contract.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @param startPosition            start position of log segment to be read, inclusive.
     * @param endPosition              end position of log segment to be read, inclusive.
     * @return input stream of the requested log segment data.
     * @throws RemoteStorageException          if there are any errors while fetching the desired segment, or if
     *                                         either position exceeds {@code Integer.MAX_VALUE} and this method has
     *                                         not been overridden to support large segments.
     * @throws RemoteResourceNotFoundException the requested log segment is not found in the remote storage.
     */
    default InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                        long startPosition,
                                        long endPosition) throws RemoteStorageException {
        if (startPosition > Integer.MAX_VALUE || endPosition > Integer.MAX_VALUE) {
            throw new RemoteStorageException("Positions (start=" + startPosition + ", end=" + endPosition +
                ") exceed Integer.MAX_VALUE but this RemoteStorageManager implementation has not overridden " +
                "fetchLogSegment(RemoteLogSegmentMetadata, long, long). Override the long-overload to support " +
                "segments larger than 2GiB (KIP-1333).");
        }
        return fetchLogSegment(remoteLogSegmentMetadata, (int) startPosition, (int) endPosition);
    }

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
     * @throws RetriableRemoteStorageException if the error is transient and the operation can be retried.
     */
    void deleteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException;
}
