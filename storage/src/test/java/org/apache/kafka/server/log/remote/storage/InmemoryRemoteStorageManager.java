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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;

/**
 * This class is an implementation of {@link RemoteStorageManager} backed by in-memory store.
 */
public class InmemoryRemoteStorageManager implements RemoteStorageManager {
    private static final Logger log = LoggerFactory.getLogger(InmemoryRemoteStorageManager.class);

    // Map of key to log data, which can be segment or any of its indexes.
    private Map<String, byte[]> keyToLogData = new ConcurrentHashMap<>();

    static String generateKeyForSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        return remoteLogSegmentMetadata.remoteLogSegmentId().id().toString() + ".segment";
    }

    static String generateKeyForIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                      IndexType indexType) {
        return remoteLogSegmentMetadata.remoteLogSegmentId().id().toString() + "." + indexType.toString();
    }

    // visible for testing.
    boolean containsKey(String key) {
        return keyToLogData.containsKey(key);
    }

    @Override
    public Optional<CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                                       LogSegmentData logSegmentData)
            throws RemoteStorageException {
        log.debug("copying log segment and indexes for : {}", remoteLogSegmentMetadata);
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");
        Objects.requireNonNull(logSegmentData, "logSegmentData can not be null");

        if (keyToLogData.containsKey(generateKeyForSegment(remoteLogSegmentMetadata))) {
            throw new RemoteStorageException("It already contains the segment for the given id: " +
                                             remoteLogSegmentMetadata.remoteLogSegmentId());
        }

        try {
            keyToLogData.put(generateKeyForSegment(remoteLogSegmentMetadata),
                    Files.readAllBytes(logSegmentData.logSegment()));
            if (logSegmentData.transactionIndex().isPresent()) {
                keyToLogData.put(generateKeyForIndex(remoteLogSegmentMetadata, IndexType.TRANSACTION),
                    Files.readAllBytes(logSegmentData.transactionIndex().get()));
            }
            keyToLogData.put(generateKeyForIndex(remoteLogSegmentMetadata, IndexType.LEADER_EPOCH),
                    logSegmentData.leaderEpochIndex().array());
            keyToLogData.put(generateKeyForIndex(remoteLogSegmentMetadata, IndexType.PRODUCER_SNAPSHOT),
                    Files.readAllBytes(logSegmentData.producerSnapshotIndex()));
            keyToLogData.put(generateKeyForIndex(remoteLogSegmentMetadata, IndexType.OFFSET),
                    Files.readAllBytes(logSegmentData.offsetIndex()));
            keyToLogData.put(generateKeyForIndex(remoteLogSegmentMetadata, IndexType.TIMESTAMP),
                    Files.readAllBytes(logSegmentData.timeIndex()));
        } catch (Exception e) {
            throw new RemoteStorageException(e);
        }
        log.debug("copied log segment and indexes for : {} successfully.", remoteLogSegmentMetadata);
        return Optional.empty();
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       int startPosition)
            throws RemoteStorageException {
        log.debug("Received fetch segment request at start position: [{}] for [{}]", startPosition, remoteLogSegmentMetadata);
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");

        return fetchLogSegment(remoteLogSegmentMetadata, startPosition, Integer.MAX_VALUE);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       int startPosition,
                                       int endPosition) throws RemoteStorageException {
        log.debug("Received fetch segment request at start position: [{}] and end position: [{}] for segment [{}]",
                startPosition, endPosition, remoteLogSegmentMetadata);

        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");

        if (startPosition < 0 || endPosition < 0) {
            throw new IllegalArgumentException("Given start position or end position must not be negative.");
        }

        if (endPosition < startPosition) {
            throw new IllegalArgumentException("end position must be greater than or equal to start position");
        }

        String key = generateKeyForSegment(remoteLogSegmentMetadata);
        byte[] segment = keyToLogData.get(key);

        if (segment == null) {
            throw new RemoteResourceNotFoundException("No remote log segment found with start offset:"
                                                      + remoteLogSegmentMetadata.startOffset() + " and id: "
                                                      + remoteLogSegmentMetadata.remoteLogSegmentId());
        }

        if (startPosition >= segment.length) {
            throw new IllegalArgumentException("start position: " + startPosition
                                               + " must be less than the length of the segment: " + segment.length);
        }

        // If the given (endPosition + 1) is more than the segment length then the segment length is taken into account.
        // Computed length should never be more than the existing segment size.
        int length = Math.min(segment.length - 1, endPosition) - startPosition + 1;
        log.debug("Length of the segment to be sent: [{}], for segment: [{}]", length, remoteLogSegmentMetadata);

        return new ByteArrayInputStream(segment, startPosition, length);
    }

    @Override
    public InputStream fetchIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  IndexType indexType) throws RemoteStorageException {
        log.debug("Received fetch request for index type: [{}], segment [{}]", indexType, remoteLogSegmentMetadata);
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");
        Objects.requireNonNull(indexType, "indexType can not be null");

        String key = generateKeyForIndex(remoteLogSegmentMetadata, indexType);
        byte[] index = keyToLogData.get(key);
        if (index == null) {
            throw new RemoteResourceNotFoundException("No remote log segment index found with start offset:"
                                                      + remoteLogSegmentMetadata.startOffset() + " and id: "
                                                      + remoteLogSegmentMetadata.remoteLogSegmentId());
        }

        return new ByteArrayInputStream(index);
    }

    @Override
    public void deleteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        log.info("Deleting log segment for: [{}]", remoteLogSegmentMetadata);
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");
        String segmentKey = generateKeyForSegment(remoteLogSegmentMetadata);
        keyToLogData.remove(segmentKey);
        for (IndexType indexType : IndexType.values()) {
            String key = generateKeyForIndex(remoteLogSegmentMetadata, indexType);
            keyToLogData.remove(key);
        }
        log.info("Deleted log segment successfully for: [{}]", remoteLogSegmentMetadata);
    }

    @Override
    public void close() throws IOException {
        // Clearing the references to the map and assigning empty immutable map.
        // Practically, this instance will not be used once it is closed.
        keyToLogData = Collections.emptyMap();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Intentionally left blank here as nothing to be initialized here.
    }
}
