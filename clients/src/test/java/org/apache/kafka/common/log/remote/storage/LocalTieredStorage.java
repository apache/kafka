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

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageListener.*;
import org.apache.kafka.test.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.StandardOpenOption.READ;
import static org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.*;
import static org.apache.kafka.common.log.remote.storage.RemoteTopicPartitionDirectory.*;
import static org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.*;


import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * An implementation of {@link RemoteStorageManager} which relies on the local file system to store
 * offloaded log segments and associated data.
 * <p>
 * Due to the consistency semantic of POSIX-compliant file systems, this remote storage provides strong
 * read-after-write consistency and a segment's data can be accessed once the copy to the storage succeeded.
 * </p>
 * <p>
 * In order to guarantee isolation, independence, reproducibility and consistency of unit and integration
 * tests, the scope of a storage implemented by this class, and identified via the storage ID provided to
 * the constructor, should be limited to a test or well-defined self-contained use-case.
 * </p>
 * <p>
 * The local tiered storage keeps a simple structure of directories mimicking that of Apache Kafka.
 * Given the root directory of the storage, segments and associated files are organized
 * </p>
 * <code>
 *  / storage-directory  / a-topic-0 / 9b8dd441-28af-4805-936f-f02db37f11b5-segment
 *                       .           . 9b8dd441-28af-4805-936f-f02db37f11b5-offset_index
 *                       .           . 9b8dd441-28af-4805-936f-f02db37f11b5-time_index
 *                       .           . 4674f230-d15b-41f7-a5a6-d874b794de42-segment
 *                       .           . 4674f230-d15b-41f7-a5a6-d874b794de42-offset_index
 *                       .           . 4674f230-d15b-41f7-a5a6-d874b794de42-segment
 *                       .
 *                       / a-topic-1 / 82da091b-84f5-4d72-9ceb-3532a1f3a4c1-segment
 *                       .           . 82da091b-84f5-4d72-9ceb-3532a1f3a4c1-offset_index
 *                       .           . 82da091b-84f5-4d72-9ceb-3532a1f3a4c1-time_index
 *                       .
 *                       / b-topic-3 / df2bbd78-3bfd-438c-a4ff-29a45a4d4e9d-segment
 *                                   . df2bbd78-3bfd-438c-a4ff-29a45a4d4e9d-offset_index
 *                                   . df2bbd78-3bfd-438c-a4ff-29a45a4d4e9d-time_index
 * </code>
 */
public final class LocalTieredStorage implements RemoteStorageManager {
    /**
     * The root directory of this storage.
     */
    public static final String STORAGE_DIR_PROP = "remote.log.storage.local.dir";

    /**
     * Delete all files and directories from this storage on close, substantially removing it
     * entirely from the file system.
     */
    public static final String DELETE_ON_CLOSE_PROP = "remote.log.storage.local.delete.on.close";

    /**
     * The implementation of the transfer of the data of the canonical segment and index files to
     * this storage. The only reason the "transferer" abstraction exists is to be able to simulate
     * file copy errors and exercise the associated failure modes.
     */
    public static final String TRANSFERER_CLASS_PROP = "remote.log.storage.local.transferer";

    /**
     * Whether the deleteLogSegment() implemented by this storage should actually delete data or behave
     * as a no-operation. This allows to simulate non-strongly consistent storage systems which do not
     * guarantee visibility of a successful delete for subsequent read or list operations.
     */
    public static final String ENABLE_DELETE_API_PROP = "remote.log.storage.local.delete.enable";

    private static final String ROOT_STORAGES_DIR_NAME = "kafka-tiered-storage";

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalTieredStorage.class);

    private volatile File storageDirectory;
    private volatile boolean deleteOnClose = true;
    private volatile boolean deleteEnabled = true;
    private volatile Transferer transferer = (from, to) -> Files.copy(from.toPath(), to.toPath());

    /**
     * Used to notify users of this storage of internal updates - new topic-partition recorded (upon directory
     * creation) and segment file written (upon segment file write(2)).
     */
    private final LocalTieredStorageListeners storageListeners = new LocalTieredStorageListeners();

    /**
     * Walks through this storage and notify the traverser of every topic-partition, segment and record discovered.
     *
     * - The order of traversal of the topic-partition is not specified.
     * - The order of traversal of the segments within a topic-partition is in ascending order
     *   of the modified timestamp of the segment file.
     * - The order of traversal of records within a segment corresponds to the insertion
     *   order of these records in the original segment from which the segment in this storage
     *   was transferred from.
     *
     * This method is NOT an atomic operation w.r.t the local tiered storage. This storage may change while
     * being traversed topic-partitions, segments and records are communicated to the traverser. There is
     * no guarantee updates to the storage which happens during traversal will be communicated to the traverser.
     * Especially, in case of concurrent read and write/delete to a topic-partition, a segment or a record,
     * the behaviour depends on the underlying file system.
     *
     * @param traverser User-specified object to which this storage communicates the topic-partitions,
     *                  segments and records as they are discovered.
     */
    public void traverse(final LocalTieredStorageTraverser traverser) {
        Objects.requireNonNull(traverser);

        Arrays.stream(storageDirectory.listFiles())
                .filter(File::isDirectory)
                .forEach(dir ->
                        openExistingTopicPartitionDirectory(dir.getName(), storageDirectory).traverse(traverser));
    }

    public void addListener(final LocalTieredStorageListener listener) {
        this.storageListeners.add(listener);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        if (storageDirectory != null) {
            throw new InvalidConfigurationException(format("This instance of local remote storage" +
                    "is already configured. The existing storage directory is %s. Ensure the method " +
                    "configure() is only called once.", storageDirectory.getAbsolutePath()));
        }

        final String storageDir = (String) configs.get(STORAGE_DIR_PROP);
        final String shouldDeleteOnClose = (String) configs.get(DELETE_ON_CLOSE_PROP);
        final String transfererClass = (String) configs.get(TRANSFERER_CLASS_PROP);
        final String isDeleteEnabled = (String) configs.get(ENABLE_DELETE_API_PROP);

        if (shouldDeleteOnClose != null) {
            deleteOnClose = Boolean.parseBoolean(shouldDeleteOnClose);
        }

        if (isDeleteEnabled != null) {
            deleteEnabled = Boolean.parseBoolean(isDeleteEnabled);
        }

        if (transfererClass != null) {
            try {
                transferer = (Transferer) getClass().getClassLoader().loadClass(transfererClass).newInstance();

            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | ClassCastException e) {
                throw new RuntimeException(format("Cannot create transferer from class '%s'", transfererClass), e);
            }
        }

        if (storageDir == null) {
            storageDirectory = TestUtils.tempDirectory(ROOT_STORAGES_DIR_NAME + "-");

            LOGGER.debug(
                    "No storage directory specified, created temporary directory: {}",
                    storageDirectory.getAbsolutePath());

        } else {
            storageDirectory = new File(new File("."), ROOT_STORAGES_DIR_NAME + "/" + storageDir);
            final boolean existed = storageDirectory.exists();

            if (!existed) {
                LOGGER.info("Creating directory: " + storageDirectory.getAbsolutePath());
                storageDirectory.mkdirs();

            } else {
                LOGGER.warn(format("Remote storage with ID %s already exists on the file system. Any data already " +
                        "in the remote storage will not be deleted and may result in an inconsistent state and/or " +
                        "provide stale data.", storageDir));
            }
        }

        LOGGER.info(format("Created local tiered storage: %s", storageDirectory.getName()));
    }

    @Override
    public RemoteLogSegmentContext copyLogSegment(final RemoteLogSegmentId id, final LogSegmentData data)
            throws RemoteStorageException {

        return wrap(() -> {
            final RemoteLogSegmentFileset remoteSegmentFileset = openFileset(storageDirectory, id);

            try {
                if (!remoteSegmentFileset.getPartitionDirectory().didExist()) {
                    storageListeners.onTopicPartitionCreated(id.topicPartition());
                }

                LOGGER.info(format("Transferring log segment for topic=%s partition=%d from offset=%s",
                        id.topicPartition().topic(),
                        id.topicPartition().partition(),
                        data.logSegment().getName().split("\\.")[0]));

                remoteSegmentFileset.copy(transferer, data);

                storageListeners.onSegmentOffloaded(remoteSegmentFileset);

            } catch (final Exception e) {
                //
                // Keep the storage in a consistent state, i.e. a segment stored should always have with its
                // associated offset and time indexes stored as well. Here, delete any file which was copied
                // before the exception was hit. The exception is re-thrown as no remediation is expected in
                // the current scope.
                //
                remoteSegmentFileset.delete();
                throw e;
            }

            return RemoteLogSegmentContext.EMPTY_CONTEXT;
        });
    }

    @Override
    public InputStream fetchLogSegmentData(final RemoteLogSegmentMetadata metadata,
                                           final Long startPosition,
                                           final Long endPosition) throws RemoteStorageException {
        checkArgument(startPosition >= 0, "Start position must be positive", startPosition);
        if (endPosition != null) {
            checkArgument(endPosition >= startPosition,
                    "End position cannot be less than startPosition", startPosition, endPosition);
        }
        return wrap(() -> {
            final RemoteLogSegmentFileset fileset = openFileset(storageDirectory, metadata.remoteLogSegmentId());
            final InputStream inputStream = newInputStream(fileset.getFile(SEGMENT).toPath(), READ);
            inputStream.skip(startPosition);

            // endPosition is ignored at this stage. A wrapper around the file input stream can implement
            // the upper bound on the stream.

            return inputStream;
        });
    }

    @Override
    public InputStream fetchOffsetIndex(final RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        return wrap(() -> {
            final RemoteLogSegmentFileset fileset = openFileset(storageDirectory, metadata.remoteLogSegmentId());
            return newInputStream(fileset.getFile(OFFSET_INDEX).toPath(), READ);
        });
    }

    @Override
    public InputStream fetchTimestampIndex(final RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        return wrap(() -> {
            final RemoteLogSegmentFileset fileset = openFileset(storageDirectory, metadata.remoteLogSegmentId());
            return newInputStream(fileset.getFile(TIME_INDEX).toPath(), READ);
        });
    }

    @Override
    public void deleteLogSegment(final RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        wrap(() -> {
            if (deleteEnabled) {
                final RemoteLogSegmentFileset remote = openFileset(storageDirectory, metadata.remoteLogSegmentId());
                if (!remote.delete()) {
                    throw new RemoteStorageException("Failed to delete remote log segment with id:" +
                            metadata.remoteLogSegmentId());
                }
            }
            return null;
        });
    }

    @Override
    public void close() {
        if (deleteOnClose) {
            try {
                final File[] files = storageDirectory.listFiles();
                final Optional<File> notADirectory = Arrays.stream(files).filter(f -> !f.isDirectory()).findAny();

                if (notADirectory.isPresent()) {
                    LOGGER.warn("Found file %s which is not a remote topic-partition directory. " +
                            "Stopping the deletion process.", notADirectory.get());
                    //
                    // If an unexpected state is encountered, do not proceed with the delete of the local storage,
                    // keeping it for post-mortem analysis. Do not throw either, in an attempt to keep the close()
                    // method quiet.
                    //
                    return;
                }

                final boolean success = Arrays.stream(files)
                        .map(dir -> openExistingTopicPartitionDirectory(dir.getName(), storageDirectory))
                        .map(RemoteTopicPartitionDirectory::delete)
                        .reduce(true, Boolean::logicalAnd);

                if (success) {
                    storageDirectory.delete();
                }

                File root = new File(ROOT_STORAGES_DIR_NAME);
                if (root.exists() && root.isDirectory() && root.list().length == 0) {
                    root.delete();
                }

            } catch (final Exception e) {
                LOGGER.error("Error while deleting remote storage. Stopping the deletion process.", e);
            }
        }
    }

    String getStorageDirectoryRoot() throws RemoteStorageException {
        return wrap(() -> storageDirectory.getAbsolutePath());
    }

    private <U> U wrap(Callable<U> f) throws RemoteStorageException {
        if (storageDirectory == null) {
            throw new RemoteStorageException(
                    "No storage directory was defined for the local remote storage. Make sure " +
                            "the instance was configured correctly via the configure() method.");
        }

        try {
            return f.call();

        } catch (final RemoteStorageException rse) {
            throw rse;

        } catch (final FileNotFoundException | NoSuchFileException e) {
            throw new RemoteResourceNotFoundException(e);
        }

        catch (final Exception e) {
            throw new RemoteStorageException("Internal error in local remote storage", e);
        }
    }

    private static void checkArgument(final boolean valid, final String message, final Object... args) {
        if (!valid) {
            throw new IllegalArgumentException(message + ": " + Arrays.toString(args));
        }
    }
}
