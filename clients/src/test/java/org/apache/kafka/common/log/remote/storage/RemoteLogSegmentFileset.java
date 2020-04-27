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

import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.OFFSET_INDEX;
import static org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.SEGMENT;
import static org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.TIME_INDEX;
import static org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.getFileType;
import static org.apache.kafka.common.log.remote.storage.RemoteTopicPartitionDirectory.openTopicPartitionDirectory;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Represents the set of files offloaded to the local tiered storage for a single log segment.
 * A {@link RemoteLogSegmentFileset} corresponds to the leaves of the file system structure of
 * the local tiered storage:
 *
 * <code>
 * / storage-directory / topic-partition / 82da091b-84f5-4d72-9ceb-3532a1f3a4c1-segment
 *                                       . 82da091b-84f5-4d72-9ceb-3532a1f3a4c1-offset_index
 *                                       . 82da091b-84f5-4d72-9ceb-3532a1f3a4c1-time_index
 * </code>
 */
public final class RemoteLogSegmentFileset {

    /**
     * Characterises the type of a file in the local tiered storage copied from Apache Kafka's standard storage.
     */
    public enum RemoteLogSegmentFileType {
        SEGMENT,
        OFFSET_INDEX,
        TIME_INDEX;

        /**
         * Provides the name of the file of this type for the given UUID in the local tiered storage,
         * e.g. uuid-segment.
         */
        public String toFilename(final UUID uuid) {
            return format("%s-%s", uuid.toString(), name().toLowerCase());
        }

        /**
         * Separator in the file name of a file offloaded to the local tiered storage.
         * It separates the string representation of the UUID and a suffix which characterizes
         * the nature of the file.
         */
        private static final char separator = '-';

        /**
         * Returns the nature of the data stored in the file with the provided name.
         */
        public static RemoteLogSegmentFileType getFileType(final String filename) {
            final int separatorIndex = filename.lastIndexOf(separator);
            if (separatorIndex == -1) {
                throw new IllegalArgumentException(format("Not a remote log segment file: %s", filename));
            }

            try {
                return RemoteLogSegmentFileType.valueOf(filename.substring(1 + separatorIndex).toUpperCase());

            } catch (final RuntimeException e) {
                throw new IllegalArgumentException(format("Not a remote log segment file: %s", filename), e);
            }
        }

        /**
         * Extract the UUID from the filename. This UUID is that of the remote log segment id which uniquely
         * identify the log segment which filename's data belongs to (not necessarily segment data, but also
         * indexes or other associated files).
         */
        public static UUID getUUID(final String filename) {
            final int separatorIndex = filename.lastIndexOf(separator);
            if (separatorIndex == -1) {
                throw new IllegalArgumentException(format("Not a remote log segment file: %s", filename));
            }

            return UUID.fromString(filename.substring(0, separatorIndex));
        }
    }

    private static final Logger LOGGER = getLogger(RemoteLogSegmentFileset.class);

    private final RemoteTopicPartitionDirectory partitionDirectory;
    private final RemoteLogSegmentId remoteLogSegmentId;
    private final Map<RemoteLogSegmentFileType, File> files;

    /**
     * Creates a new fileset located under the given storage directory for the provided remote log segment id.
     * The topic-partition directory is created if it does not exist yet. However the files corresponding to
     * the log segment offloaded are not created on the file system until transfer happens.
     *
     * @param storageDir The root directory of the local tiered storage.
     * @param id Remote log segment id assigned to a log segment in Kafka.
     * @return A new fileset instance.
     */
    public static RemoteLogSegmentFileset openFileset(final File storageDir, final RemoteLogSegmentId id) {
        final RemoteTopicPartitionDirectory tpDir = openTopicPartitionDirectory(id.topicPartition(), storageDir);
        final File partitionDirectory = tpDir.getDirectory();
        final UUID uuid = id.id();

        final Map<RemoteLogSegmentFileType, File> files = stream(RemoteLogSegmentFileType.values())
                .collect(toMap(identity(), type -> new File(partitionDirectory, type.toFilename(uuid))));

        return new RemoteLogSegmentFileset(tpDir, id, files);
    }

    /**
     * Creates a fileset instance for the physical set of files located under the given topic-partition directory.
     * The fileset MUST exist on the file system with the given uuid.
     *
     * @param tpDirectory The topic-partition directory which this fileset's segment belongs to.
     * @param uuid The expected UUID of the fileset.
     * @return A new fileset instance.
     */
    public static RemoteLogSegmentFileset openExistingFileset(final RemoteTopicPartitionDirectory tpDirectory,
                                                              final UUID uuid) {
        final Map<RemoteLogSegmentFileType, File> files =
                stream(tpDirectory.getDirectory().listFiles())
                        .filter(file -> file.getName().startsWith(uuid.toString()))
                        .collect(toMap(file -> getFileType(file.getName()), identity()));

        final Set<RemoteLogSegmentFileType> expectedTypes = new HashSet<>(asList(RemoteLogSegmentFileType.values()));

        if (!files.keySet().equals(expectedTypes)) {
            expectedTypes.removeAll(files.keySet());
            throw new IllegalStateException(format("Invalid fileset, missing files: %s", expectedTypes));
        }

        final RemoteLogSegmentId id = new RemoteLogSegmentId(tpDirectory.getTopicPartition(), uuid);
        return new RemoteLogSegmentFileset(tpDirectory, id, files);
    }


    public RemoteTopicPartitionDirectory getPartitionDirectory() {
        return partitionDirectory;
    }

    public RemoteLogSegmentId getRemoteLogSegmentId() {
        return remoteLogSegmentId;
    }

    public File getFile(final RemoteLogSegmentFileType type) {
        return files.get(type);
    }

    public boolean delete() {
        return deleteFilesOnly(files.values());
    }

    public List<Record> getRecords() throws IOException {
        return StreamSupport
                .stream(FileRecords.open(files.get(SEGMENT)).records().spliterator(), false)
                .collect(Collectors.toList());
    }

    public void copy(final Transferer transferer, final LogSegmentData data) throws IOException {
        transferer.transfer(data.logSegment(), files.get(SEGMENT));
        transferer.transfer(data.offsetIndex(), files.get(OFFSET_INDEX));
        transferer.transfer(data.timeIndex(), files.get(TIME_INDEX));
    }

    public static boolean deleteFilesOnly(final Collection<File> files) {
        final Optional<File> notAFile = files.stream().filter(f -> !f.isFile()).findAny();

        if (notAFile.isPresent()) {
            LOGGER.warn(format("Found unexpected directory %s. Will not delete.", notAFile.get().getAbsolutePath()));
            return false;
        }

        return files.stream().map(RemoteLogSegmentFileset::deleteQuietly).reduce(true, Boolean::logicalAnd);
    }

    public static boolean deleteQuietly(final File file) {
        try {
            LOGGER.trace("Deleting " + file.getAbsolutePath());
            return file.delete();

        } catch (final Exception e) {
            LOGGER.error(format("Encountered error while deleting %s", file.getAbsolutePath()));
        }

        return false;
    }

    RemoteLogSegmentFileset(final RemoteTopicPartitionDirectory topicPartitionDirectory,
                            final RemoteLogSegmentId remoteLogSegmentId,
                            final Map<RemoteLogSegmentFileType, File> files) {

        this.partitionDirectory = requireNonNull(topicPartitionDirectory);
        this.remoteLogSegmentId = requireNonNull(remoteLogSegmentId);
        this.files = unmodifiableMap(files);
    }
}
