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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.StandardOpenOption.READ;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * An implementation of {@link RemoteStorageManager} which relies on the local file system to store offloaded
 * log segments and associated data.
 * <p>
 * Due to the consistency semantic of POSIX-compliant file systems, this remote storage provides strong
 * read-after-write consistency and a segment's data can be accessed once the copy to the storage succeeded.
 * <p>
 * In order to guarantee isolation, independence, reproducibility and consistency of unit and integration
 * tests, the scope of a storage implemented by this class, and identified via the storage ID provided to the
 * constructor, should be limited to a test or well-defined self-contained use-case.
 */
public final class LocalRemoteStorageManager implements RemoteStorageManager {
    public static final String STORAGE_ID_PROP = "remote.log.storage.local.id";
    public static final String DELETE_ON_CLOSE_PROP = "remote.log.storage.local.delete.on.close";
    public static final String TRANSFERER_CLASS_PROP = "remote.log.storage.local.transferer";
    public static final String ENABLE_DELETE_API_PROP = "remote.log.storage.local.delete.enable";

    private static final String ROOT_STORAGES_DIR_NAME = "remote-storage";

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalRemoteStorageManager.class);

    /**
     * The ID assigned to this storage and used for reference. Using descriptive, unique ID is
     * recommended to ensure a storage can be unambiguously associated to the test which created it.
     */
    private volatile File storageDirectory;

    /**
     * Delete all files and directories from this storage on close, substantially removing it
     * entirely from the file system.
     */
    private volatile boolean deleteOnClose = true;

    /**
     * Whether the deleteLogSegment() implemented by this storage should actually delete data or behave
     * as a no-operation. This allows to simulate non-strongly consistent storage systems which do not
     * guarantee visibility of a successful delete for subsequent read or list operations.
     */
    private volatile boolean deleteEnabled = true;

    /**
     * The implementation of the transfer of the data of the canonical segment and index files to
     * this storage.
     */
    private volatile Transferer transferer = (from, to) -> Files.copy(from.toPath(), to.toPath());

    @Override
    public void configure(Map<String, ?> configs) {
        if (storageDirectory != null) {
            throw new InvalidConfigurationException(format("This instance of local remote storage" +
                    "is already configured. The existing storage directory is %s. Ensure the method " +
                    "configure() is only called once.", storageDirectory.getAbsolutePath()));
        }

        final String storageId = (String) configs.get(STORAGE_ID_PROP);
        final String shouldDeleteOnClose = (String) configs.get(DELETE_ON_CLOSE_PROP);
        final String transfererClass = (String) configs.get(TRANSFERER_CLASS_PROP);
        final String isDeleteEnabled = (String) configs.get(ENABLE_DELETE_API_PROP);

        if (storageId == null) {
            throw new InvalidConfigurationException("No storage id provided: " + STORAGE_ID_PROP);
        }

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

        Directory directory = openDirectory(ROOT_STORAGES_DIR_NAME + "/" + storageId, new File("."));
        storageDirectory = directory.directory;

        if (directory.existed) {
            LOGGER.warn(format("Remote storage with ID %s already exists on the file system. Any data already in " +
                    "the remote storage will not be deleted and may result in an inconsistent state and/or provide" +
                    "stale data.", storageId));
        }

        LOGGER.info(format("Created local remote storage: %s", storageId));
    }

    @Override
    public RemoteLogSegmentContext copyLogSegment(final RemoteLogSegmentId id, final LogSegmentData data)
            throws RemoteStorageException {

        return wrap(() -> {
            final RemoteLogSegmentFiles remote = new RemoteLogSegmentFiles(id, false);
            try {
                final TopicPartition topicPartition = id.topicPartition();

                LOGGER.info(format("Transferring log segment for topic=%s partition=%d from offset=%s",
                        topicPartition.topic(),
                        topicPartition.partition(),
                        data.logSegment().getName().split("\\.")[0]));

                remote.copyAll(data);
            } catch (final Exception e) {
                //
                // Keep the storage in a consistent state, i.e. a segment stored should always have with its
                // associated offset and time indexes stored as well. Here, delete any file which was copied
                // before the exception was hit. The exception is re-thrown as no remediation is expected in
                // the current scope.
                //
                remote.deleteAll();
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
            final RemoteLogSegmentFiles remote = new RemoteLogSegmentFiles(metadata.remoteLogSegmentId(), true);
            final InputStream inputStream = newInputStream(remote.logSegment.toPath(), READ);
            inputStream.skip(startPosition);

            // endPosition is ignored at this stage. A wrapper around the file input stream can implement
            // the upper bound on the stream.

            return inputStream;
        });
    }

    @Override
    public InputStream fetchOffsetIndex(final RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        return wrap(() -> {
            final RemoteLogSegmentFiles remote = new RemoteLogSegmentFiles(metadata.remoteLogSegmentId(), true);
            return newInputStream(remote.offsetIndex.toPath(), READ);
        });
    }

    @Override
    public InputStream fetchTimestampIndex(final RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        return wrap(() -> {
            final RemoteLogSegmentFiles remote = new RemoteLogSegmentFiles(metadata.remoteLogSegmentId(), true);
            return newInputStream(remote.timeIndex.toPath(), READ);
        });
    }

    @Override
    public void deleteLogSegment(final RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        wrap(() -> {
            if (deleteEnabled) {
                final RemoteLogSegmentFiles remote = new RemoteLogSegmentFiles(metadata.remoteLogSegmentId(), true);
                if (!remote.deleteAll()) {
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

                final boolean success = Arrays.stream(files).map(topicPartitionDirectory -> {
                    //
                    // The topic-partition directory is deleted only if all files inside it have been deleted
                    // successfully thanks to the short-circuit operand. Yes, this is bad to rely on that to
                    // drive the execution flow.
                    //
                    return deleteFilesOnly(asList(topicPartitionDirectory.listFiles()))
                            && deleteQuietly(topicPartitionDirectory);

                }).reduce(true, Boolean::logicalAnd);

                if (success) {
                    deleteQuietly(storageDirectory);
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
                            "the instance was configured correctly via the configure(configs: util.Map[String, _]) method.");
        }

        try {
            return f.call();

        } catch (final RemoteStorageException rse) {
            throw rse;

        } catch (final Exception e) {
            throw new RemoteStorageException("Internal error in local remote storage", e);
        }
    }

    interface Transferer {

        void transfer(File from, File to) throws IOException;

    }

    private final class RemoteLogSegmentFiles {
        private static final String SEGMENT_SUFFIX = "segment";
        private static final String OFFSET_SUFFIX = "offset";
        private static final String TIME_SUFFIX = "time";

        private final File partitionDirectory;
        private final File logSegment;
        private final File offsetIndex;
        private final File timeIndex;

        RemoteLogSegmentFiles(final RemoteLogSegmentId id, final boolean shouldExist) throws RemoteStorageException {
            final TopicPartition tp = id.topicPartition();
            this.partitionDirectory =
                    openDirectory(format("%s-%d", tp.topic(), tp.partition()), storageDirectory).directory;

            this.logSegment = remoteFile(id, SEGMENT_SUFFIX, shouldExist);
            this.offsetIndex = remoteFile(id, OFFSET_SUFFIX, shouldExist);
            this.timeIndex = remoteFile(id, TIME_SUFFIX, shouldExist);
        }

        private void copyAll(final LogSegmentData data) throws IOException {
            transferer.transfer(data.logSegment(), logSegment);
            transferer.transfer(data.offsetIndex(), offsetIndex);
            transferer.transfer(data.timeIndex(), timeIndex);
        }

        private boolean deleteAll() {
            return deleteFilesOnly(asList(logSegment, offsetIndex, timeIndex)) && deleteQuietly(partitionDirectory);
        }

        private File remoteFile(final RemoteLogSegmentId id, final String suffix, final boolean shouldExist)
                throws RemoteResourceNotFoundException {
            File file = new File(partitionDirectory, format("%s-%s", id.id().toString(), suffix));
            if (!file.exists() && shouldExist) {
                throw new RemoteResourceNotFoundException(id, format("File %s not found", file.getName()));
            }
            return file;
        }
    }

    private static final class Directory {
        private final File directory;
        private final boolean existed;

        Directory(final File directory, final boolean existed) {
            this.directory = requireNonNull(directory);
            this.existed = existed;
        }
    }

    private static Directory openDirectory(final String relativePath, final File parent) {
        final File directory = new File(parent, relativePath);
        final boolean existed = directory.exists();

        if (!existed) {
            LOGGER.warn("Creating directory: " + directory.getAbsolutePath());
            directory.mkdirs(); // TODO
        }

        return new Directory(directory, existed);
    }

    private static boolean deleteFilesOnly(final List<File> files) {
        final Optional<File> notAFile = files.stream().filter(f -> !f.isFile()).findAny();

        if (notAFile.isPresent()) {
            LOGGER.warn(format("Found unexpected directory %s. Will not delete.", notAFile.get().getAbsolutePath()));
            return false;
        }

        return files.stream().map(LocalRemoteStorageManager::deleteQuietly).reduce(true, Boolean::logicalAnd);
    }

    private static boolean deleteQuietly(final File file) {
        try {
            LOGGER.trace("Deleting " + file.getAbsolutePath());

            return file.delete();
        } catch (final Exception e) {
            LOGGER.error(format("Encountered error while deleting %s", file.getAbsolutePath()));
        }

        return false;
    }

    private static void checkArgument(final boolean valid, final String message, final Object... args) {
        if (!valid) {
            throw new IllegalArgumentException(message + ": " + Arrays.toString(args));
        }
    }
}
