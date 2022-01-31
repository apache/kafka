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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.LEADER_EPOCH_CHECKPOINT;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.OFFSET_INDEX;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.PRODUCER_SNAPSHOT;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.SEGMENT;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.TIME_INDEX;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.TRANSACTION_INDEX;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.getFileType;
import static org.apache.kafka.server.log.remote.storage.RemoteTopicPartitionDirectory.openTopicPartitionDirectory;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Represents the set of files offloaded to the local tiered storage for a single log segment.
 * A {@link RemoteLogSegmentFileset} corresponds to the leaves of the file system structure of
 * the local tiered storage:
 *
 * <code>
 * / storage-directory / uuidBase64-partition-topic / oAtiIQ95REujbuzNd_lkLQ-segment
 *                                                  . oAtiIQ95REujbuzNd_lkLQ-offset_index
 *                                                  . oAtiIQ95REujbuzNd_lkLQ-time_index
 * </code>
 */
public final class RemoteLogSegmentFileset {

    /**
     * The format of a file which belongs to the fileset, i.e. a file which is assigned to a log segment in
     * Kafka's log directory.
     *
     * The name of each of the files under the scope of a log segment (the log file, its indexes, etc.)
     * follows the structure UUID-FileType.
     */
    private static final String UUID_LEGAL_CHARS = "[a-zA-Z0-9_-]{22}";
    private static final Pattern FILENAME_FORMAT = compile("(" + UUID_LEGAL_CHARS + ")-([a-z_]+)");
    private static final int GROUP_UUID = 1;
    private static final int GROUP_FILE_TYPE = 2;

    /**
     * Characterises the type of a file in the local tiered storage copied from Apache Kafka's standard storage.
     */
    public enum RemoteLogSegmentFileType {
        SEGMENT(false),
        OFFSET_INDEX(false),
        TIME_INDEX(false),
        TRANSACTION_INDEX(true),
        LEADER_EPOCH_CHECKPOINT(false),
        PRODUCER_SNAPSHOT(true);

        private final boolean optional;

        RemoteLogSegmentFileType(boolean optional) {
            this.optional = optional;
        }

        /**
         * Provides the name of the file of this type for the given UUID in the local tiered storage,
         * e.g. uuid-segment.
         */
        public String toFilename(final Uuid uuid) {
            return format("%s-%s", uuid.toString(), name().toLowerCase(Locale.ROOT));
        }

        /**
         * Returns the nature of the data stored in the file with the provided name.
         */
        public static RemoteLogSegmentFileType getFileType(final String filename) {
            try {
                return RemoteLogSegmentFileType.valueOf(substr(filename, GROUP_FILE_TYPE).toUpperCase(Locale.ROOT));

            } catch (final RuntimeException e) {
                throw new IllegalArgumentException(format("Not a remote log segment file: %s", filename), e);
            }
        }

        /**
         * Extract the UUID from the filename. This UUID is that of the remote log segment id which uniquely
         * identify the log segment which filename's data belongs to (not necessarily segment data, but also
         * indexes or other associated files).
         */
        public static Uuid getUuid(final String filename) {
            return Uuid.fromString(substr(filename, GROUP_UUID));
        }

        static String substr(final String filename, final int group) {
            final Matcher m = FILENAME_FORMAT.matcher(filename);
            if (!m.matches()) {
                throw new IllegalArgumentException(format("Not a remote log segment file: %s", filename));
            }
            return m.group(group);
        }

        public boolean isOptional() {
            return optional;
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

        final RemoteTopicPartitionDirectory tpDir = openTopicPartitionDirectory(id.topicIdPartition(), storageDir);
        final File partitionDirectory = tpDir.getDirectory();
        final Uuid uuid = id.id();

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
                                                              final Uuid uuid) {
        final Map<RemoteLogSegmentFileType, File> files =
                stream(tpDirectory.getDirectory().listFiles())
                        .filter(file -> file.getName().startsWith(uuid.toString()))
                        .collect(toMap(file -> getFileType(file.getName()), identity()));

        final Set<RemoteLogSegmentFileType> expectedFileTypes = stream(RemoteLogSegmentFileType.values())
                .filter(x -> !x.isOptional()).collect(Collectors.toSet());

        if (!files.keySet().containsAll(expectedFileTypes)) {
            expectedFileTypes.removeAll(files.keySet());
            throw new IllegalStateException(format("Invalid fileset, missing files: %s", expectedFileTypes));
        }

        final RemoteLogSegmentId id = new RemoteLogSegmentId(tpDirectory.getTopicIdPartition(), uuid);
        return new RemoteLogSegmentFileset(tpDirectory, id, files);
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
        transferer.transfer(data.logSegment().toFile(), files.get(SEGMENT));
        transferer.transfer(data.offsetIndex().toFile(), files.get(OFFSET_INDEX));
        transferer.transfer(data.timeIndex().toFile(), files.get(TIME_INDEX));
        if (data.transactionIndex().isPresent()) {
            transferer.transfer(data.transactionIndex().get().toFile(), files.get(TRANSACTION_INDEX));
        }
        transferer.transfer(data.leaderEpochIndex(), files.get(LEADER_EPOCH_CHECKPOINT));
        transferer.transfer(data.producerSnapshotIndex().toFile(), files.get(PRODUCER_SNAPSHOT));
    }

    public String toString() {
        final String ls = files.values().stream()
                .map(file -> "\t" + file.getName() + "\n")
                .reduce("", (s1, s2) -> s1 + s2);

        return format("%s/\n%s", partitionDirectory.getDirectory().getName(), ls);
    }

    public static boolean deleteFilesOnly(final Collection<File> files) {
        final Optional<File> notAFile = files.stream().filter(f -> f.exists() && !f.isFile()).findAny();

        if (notAFile.isPresent()) {
            LOGGER.warn(format("Found unexpected directory %s. Will not delete.", notAFile.get().getAbsolutePath()));
            return false;
        }

        return files.stream().map(RemoteLogSegmentFileset::deleteQuietly).reduce(true, Boolean::logicalAnd);
    }

    public static boolean deleteQuietly(final File file) {
        try {
            LOGGER.trace("Deleting " + file.getAbsolutePath());
            if (!file.exists()) {
                return true;
            }
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
