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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageListener.LocalTieredStorageListeners;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.test.TestUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.StandardOpenOption.READ;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.COPY_SEGMENT;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.DELETE_PARTITION;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.DELETE_SEGMENT;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.FETCH_LEADER_EPOCH_CHECKPOINT;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.FETCH_OFFSET_INDEX;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.FETCH_PRODUCER_SNAPSHOT;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.FETCH_SEGMENT;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.FETCH_TIME_INDEX;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.FETCH_TRANSACTION_INDEX;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.LEADER_EPOCH_CHECKPOINT;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.OFFSET_INDEX;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.PRODUCER_SNAPSHOT;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.SEGMENT;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.TIME_INDEX;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.TRANSACTION_INDEX;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.openFileset;
import static org.apache.kafka.server.log.remote.storage.RemoteTopicPartitionDirectory.openExistingTopicPartitionDirectory;
import static org.apache.kafka.server.log.remote.storage.RemoteTopicPartitionDirectory.openTopicPartitionDirectory;

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
 * <p>
 * The name of each of the files under the scope of a log segment (the log file, its indexes, etc.)
 * follows the structure startOffset-UuidBase64-FileType.
 * <p>
 * Given the root directory of the storage, segments and associated files are organized as represented below.
 * </p>
 * <code>
 * / storage-directory  / topic-0-LWgrMmVrT0a__7a4SasuPA / 00000000000000000011-bCqX9U--S-6U8XUM9II25Q.log
 * .                                                     . 00000000000000000011-bCqX9U--S-6U8XUM9II25Q.index
 * .                                                     . 00000000000000000011-bCqX9U--S-6U8XUM9II25Q.timeindex
 * .                                                     . 00000000000000000011-h956soEzTzi9a-NOQ-DvKA.log
 * .                                                     . 00000000000000000011-h956soEzTzi9a-NOQ-DvKA.index
 * .                                                     . 00000000000000000011-h956soEzTzi9a-NOQ-DvKA.timeindex
 * .
 * / topic-1-LWgrMmVrT0a__7a4SasuPA / 00000000000000000011-o8CQPT86QQmbFmi3xRmiHA.log
 * .                                . 00000000000000000011-o8CQPT86QQmbFmi3xRmiHA.index
 * .                                . 00000000000000000011-o8CQPT86QQmbFmi3xRmiHA.timeindex
 * .
 * / topic-3-DRagLm_PS9Wl8fz1X43zVg / 00000000000000000011-jvj3vhliTGeU90sIosmp_g.log
 * .                                . 00000000000000000011-jvj3vhliTGeU90sIosmp_g.index
 * .                                . 00000000000000000011-jvj3vhliTGeU90sIosmp_g.timeindex
 * </code>
 */
public final class LocalTieredStorage implements RemoteStorageManager {

    public static final String STORAGE_CONFIG_PREFIX = "remote.log.storage.local.";

    /**
     * The root directory of this storage.
     */
    public static final String STORAGE_DIR_CONFIG = "dir";

    /**
     * Delete all files and directories from this storage on close, substantially removing it
     * entirely from the file system.
     */
    public static final String DELETE_ON_CLOSE_CONFIG = "delete.on.close";

    /**
     * The implementation of the transfer of the data of the canonical segment and index files to
     * this storage. The only reason the "transferer" abstraction exists is to be able to simulate
     * file copy errors and exercise the associated failure modes.
     */
    public static final String TRANSFERER_CLASS_CONFIG = "transferer.class";

    /**
     * Whether the deleteLogSegment() implemented by this storage should actually delete data or behave
     * as a no-operation. This allows to simulate non-strongly consistent storage systems which do not
     * guarantee visibility of a successful delete for subsequent read or list operations.
     */
    public static final String ENABLE_DELETE_API_CONFIG = "delete.enable";

    /**
     * The ID of the broker which owns this instance of {@link LocalTieredStorage}.
     */
    public static final String BROKER_ID = "broker.id";

    private static final String ROOT_STORAGE_DIR_NAME = "kafka-tiered-storage";

    private volatile File storageDirectory;
    private volatile boolean deleteOnClose = false;
    private volatile boolean deleteEnabled = true;
    private volatile Transferer transferer = new Transferer() {
        @Override
        public void transfer(File from, File to) throws IOException {
            if (from.exists()) {
                Files.copy(from.toPath(), to.toPath());
            }
        }

        @Override
        public void transfer(ByteBuffer from, File to) throws IOException {
            if (from != null && from.hasRemaining()) {
                try (FileOutputStream fileOutputStream = new FileOutputStream(to, false);
                     FileChannel channel = fileOutputStream.getChannel()) {
                    channel.write(from);
                }
            }
        }
    };

    private volatile int brokerId = -1;

    private Logger logger = LoggerFactory.getLogger(LocalTieredStorage.class);

    /**
     * Used to explicit a chronological ordering of the events generated by the local tiered storage
     * which this instance gives access to.
     */
    // TODO: Makes this timestamp only dependent on the assigned broker, not the class instance.
    private final AtomicInteger eventTimestamp = new AtomicInteger(-1);

    /**
     * Used to notify users of this storage of internal updates - new topic-partition recorded (upon directory
     * creation) and segment file written (upon segment file write).
     */
    private final LocalTieredStorageListeners storageListeners = new LocalTieredStorageListeners();

    private final LocalTieredStorageHistory history = new LocalTieredStorageHistory();

    public LocalTieredStorage() {
        history.listenTo(this);
    }

    /**
     * Walks through this storage and notify the traverser of every topic-partition, segment and record discovered.
     * <p>
     * - The order of traversal of the topic-partition is not specified.
     * - The order of traversal of the segments within a topic-partition is in ascending order
     * of the modified timestamp of the segment file.
     * - The order of traversal of records within a segment corresponds to the insertion
     * order of these records in the original segment from which the segment in this storage
     * was transferred from.
     * <p>
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

        final File[] files = storageDirectory.listFiles();
        if (files == null) {
            // files can be null if the directory is empty.
            return;
        }

        Arrays.stream(files)
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

        final String storageDir = (String) configs.get(STORAGE_DIR_CONFIG);
        final String shouldDeleteOnClose = (String) configs.get(DELETE_ON_CLOSE_CONFIG);
        final String transfererClass = (String) configs.get(TRANSFERER_CLASS_CONFIG);
        final String isDeleteEnabled = (String) configs.get(ENABLE_DELETE_API_CONFIG);
        final Integer brokerIdInt = (Integer) configs.get(BROKER_ID);

        if (brokerIdInt == null) {
            throw new InvalidConfigurationException(
                    "Broker ID is required to configure the LocalTieredStorage manager.");
        }

        brokerId = brokerIdInt;
        logger = new LogContext(format("[LocalTieredStorage Id=%d] ", brokerId)).logger(this.getClass());

        if (shouldDeleteOnClose != null) {
            deleteOnClose = Boolean.parseBoolean(shouldDeleteOnClose);
        }

        if (isDeleteEnabled != null) {
            deleteEnabled = Boolean.parseBoolean(isDeleteEnabled);
        }

        if (transfererClass != null) {
            try {
                transferer = (Transferer) getClass().getClassLoader().loadClass(transfererClass)
                        .getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | ClassCastException |
                     InvocationTargetException | NoSuchMethodException e) {
                throw new RuntimeException(format("Cannot create transferer from class '%s'", transfererClass), e);
            }
        }


        if (storageDir == null) {
            storageDirectory = TestUtils.tempDirectory(ROOT_STORAGE_DIR_NAME + "-");

            logger.debug("No storage directory specified, created temporary directory: {}",
                    storageDirectory.getAbsolutePath());

        } else {
            // LocalTieredStorage implementation can be used in two modes:
            // 1. Integration test and
            // 2. To bring Kafka in the local machine with LocalTieredStorage implementation as RSM for quick testing
            //    by adding kafka-storage-<version>-test.jar in the classpath, then enabling the INCLUDE_TEST_JARS flag
            //    before running the server
            //
            // In mode-2, if the commented-out #L284 is enabled, then it creates the storageDirectory from the present
            // kafka working directory which can cause confusion:
            //
            // (eg) Kafka is installed on /opt/kafka and storageDirectory is configured as /tmp/kafka-tiered-storage,
            // then it will create it the directory in /opt/kafka/kafka-tiered-storage/tmp/kafka-tiered-storage instead
            //  of /tmp/kafka-tiered-storage.
            //
            // storageDirectory = new File(new File("."), ROOT_STORAGES_DIR_NAME + "/" + storageDir);
            storageDirectory = new File(storageDir, ROOT_STORAGE_DIR_NAME);
            final boolean existed = Files.exists(storageDirectory.toPath());
            if (!existed) {
                try {
                    logger.info("Creating directory: [{}]", storageDirectory.getAbsolutePath());
                    Files.createDirectories(storageDirectory.toPath());
                } catch (IOException e) {
                    throw new RuntimeException(format("Not able to create the storage directory '%s'", storageDirectory.getAbsolutePath()), e);
                }
            } else {
                logger.warn("Remote storage with ID [{}] already exists on the file system. Any data already " +
                        "in the remote storage will not be deleted and may result in an inconsistent state and/or " +
                        "provide stale data.", storageDir);
            }
        }

        logger.info("Created local tiered storage manager [{}]:[{}]", brokerId, storageDirectory.getName());
    }

    @Override
    public Optional<CustomMetadata> copyLogSegmentData(final RemoteLogSegmentMetadata metadata, final LogSegmentData data)
            throws RemoteStorageException {
        Callable<Optional<CustomMetadata>> callable = () -> {
            final LocalTieredStorageEvent.Builder eventBuilder = newEventBuilder(COPY_SEGMENT, metadata);
            RemoteLogSegmentFileset fileset = null;
            try {
                fileset = openFileset(storageDirectory, metadata);
                logger.info("Offloading log segment for {} from segment={}", metadata.topicIdPartition(), data.logSegment());
                fileset.copy(transferer, data);
                storageListeners.onStorageEvent(eventBuilder.withFileset(fileset).build());
            } catch (final Exception e) {
                // Keep the storage in a consistent state, i.e. a segment stored should always have with its
                // associated offset and time indexes stored as well. Here, delete any file which was copied
                // before the exception was hit. The exception is re-thrown as no remediation is expected in
                // the current scope.
                if (fileset != null) {
                    fileset.delete();
                }
                storageListeners.onStorageEvent(eventBuilder.withException(e).build());
                throw e;
            }
            return Optional.empty();
        };
        return wrap(callable);
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata metadata,
                                       final int startPos) throws RemoteStorageException {
        return fetchLogSegment(metadata, startPos, metadata.segmentSizeInBytes());
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata metadata,
                                       final int startPos,
                                       final int endPos) throws RemoteStorageException {
        checkArgument(startPos >= 0, "Start position must be positive", startPos);
        checkArgument(endPos >= startPos,
                "End position cannot be less than startPosition", startPos, endPos);
        checkArgument(metadata.segmentSizeInBytes() >= endPos,
                "End position cannot be greater than segment size", endPos, metadata.segmentSizeInBytes());

        return wrap(() -> {

            final LocalTieredStorageEvent.Builder eventBuilder = newEventBuilder(FETCH_SEGMENT, metadata);
            eventBuilder.withStartPosition(startPos).withEndPosition(endPos);

            try {
                final RemoteLogSegmentFileset fileset = openFileset(storageDirectory, metadata);

                final InputStream inputStream = newInputStream(fileset.getFile(SEGMENT).toPath(), READ);
                inputStream.skip(startPos);

                // endPosition is ignored at this stage. A wrapper around the file input stream can implement
                // the upper bound on the stream.

                storageListeners.onStorageEvent(eventBuilder.withFileset(fileset).build());

                return inputStream;

            } catch (final Exception e) {
                storageListeners.onStorageEvent(eventBuilder.withException(e).build());
                throw e;
            }
        });
    }

    @Override
    public InputStream fetchIndex(RemoteLogSegmentMetadata metadata, IndexType indexType) throws RemoteStorageException {
        EventType eventType = getEventTypeForFetch(indexType);
        RemoteLogSegmentFileset.RemoteLogSegmentFileType fileType = getLogSegmentFileType(indexType);
        return wrap(() -> {
            final LocalTieredStorageEvent.Builder eventBuilder = newEventBuilder(eventType, metadata);

            try {
                final RemoteLogSegmentFileset fileset = openFileset(storageDirectory, metadata);

                File file = fileset.getFile(fileType);

                final InputStream inputStream;
                if (fileType.isOptional() && !file.exists()) {
                    throw new RemoteResourceNotFoundException("Index file for type: " + indexType +
                        " not found for segment " + metadata.remoteLogSegmentId());
                } else {
                    inputStream = newInputStream(file.toPath(), READ);
                }

                storageListeners.onStorageEvent(eventBuilder.withFileset(fileset).build());

                return inputStream;

            } catch (final Exception e) {
                storageListeners.onStorageEvent(eventBuilder.withException(e).build());
                throw e;
            }
        });
    }

    @Override
    public void deleteLogSegmentData(final RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        wrap(() -> {
            final LocalTieredStorageEvent.Builder eventBuilder = newEventBuilder(DELETE_SEGMENT, metadata);

            if (deleteEnabled) {
                try {
                    final RemoteLogSegmentFileset fileset = openFileset(
                            storageDirectory, metadata);

                    if (!fileset.delete()) {
                        throw new RemoteStorageException("Failed to delete remote log segment with id:" +
                                metadata.remoteLogSegmentId());
                    }

                    storageListeners.onStorageEvent(eventBuilder.withFileset(fileset).build());

                } catch (final Exception e) {
                    storageListeners.onStorageEvent(eventBuilder.withException(e).build());
                    throw e;
                }
            }
            return null;
        });
    }

    // KAFKA-15166: Add deletePartition API to the RemoteStorageManager
    // @Override
    public void deletePartition(TopicIdPartition partition) throws RemoteStorageException {
        wrap(() -> {
            final LocalTieredStorageEvent.Builder eventBuilder = newEventBuilder(DELETE_PARTITION, partition);

            if (deleteEnabled) {
                try {
                    RemoteTopicPartitionDirectory partitionDirectory = openTopicPartitionDirectory(partition, storageDirectory);

                    if (!partitionDirectory.delete()) {
                        throw new RemoteStorageException("Failed to delete remote log partition:" + partition);
                    }

                    storageListeners.onStorageEvent(eventBuilder.build());

                } catch (final Exception e) {
                    storageListeners.onStorageEvent(eventBuilder.withException(e).build());
                    throw e;
                }
            }
            return null;
        });
    }

    @Override
    public void close() {
        if (deleteOnClose) {
            clear();
        }
    }

    public void clear() {
        try {
            final File[] files = storageDirectory.listFiles();
            final Optional<File> notADirectory = Arrays.stream(files).filter(f -> !f.isDirectory()).findAny();

            if (notADirectory.isPresent()) {
                logger.warn("Found file [{}] which is not a remote topic-partition directory. " +
                        "Stopping the deletion process.", notADirectory.get());
                // If an unexpected state is encountered, do not proceed with the delete of the local storage,
                // keeping it for post-mortem analysis. Do not throw either, in an attempt to keep the close()
                // method quiet.
                return;
            }

            final boolean success = Arrays.stream(files)
                    .map(dir -> openExistingTopicPartitionDirectory(dir.getName(), storageDirectory))
                    .map(RemoteTopicPartitionDirectory::delete)
                    .reduce(true, Boolean::logicalAnd);

            if (success) {
                storageDirectory.delete();
            }

            File root = new File(ROOT_STORAGE_DIR_NAME);
            if (root.exists() && root.isDirectory() && root.list().length == 0) {
                root.delete();
            }

        } catch (final Exception e) {
            logger.error("Error while deleting remote storage. Stopping the deletion process.", e);
        }
    }

    public LocalTieredStorageHistory getHistory() {
        return history;
    }

    String getStorageDirectoryRoot() throws RemoteStorageException {
        return wrap(() -> storageDirectory.getAbsolutePath());
    }

    private LocalTieredStorageEvent.Builder newEventBuilder(final EventType type, final RemoteLogSegmentMetadata md) {
        return LocalTieredStorageEvent
                .newBuilder(brokerId, type, eventTimestamp.incrementAndGet(), md.remoteLogSegmentId())
                .withMetadata(md);
    }

    private LocalTieredStorageEvent.Builder newEventBuilder(final EventType type, final TopicIdPartition tpId) {
        return LocalTieredStorageEvent
                .newBuilder(brokerId, type, eventTimestamp.incrementAndGet(), new RemoteLogSegmentId(tpId, Uuid.ZERO_UUID));
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
        } catch (final Exception e) {
            throw new RemoteStorageException("Internal error in local remote storage", e);
        }
    }

    private static void checkArgument(final boolean valid, final String message, final Object... args) {
        if (!valid) {
            throw new IllegalArgumentException(message + ": " + Arrays.toString(args));
        }
    }

    private EventType getEventTypeForFetch(IndexType indexType) {
        switch (indexType) {
            case OFFSET:
                return FETCH_OFFSET_INDEX;
            case TIMESTAMP:
                return FETCH_TIME_INDEX;
            case PRODUCER_SNAPSHOT:
                return FETCH_PRODUCER_SNAPSHOT;
            case TRANSACTION:
                return FETCH_TRANSACTION_INDEX;
            case LEADER_EPOCH:
                return FETCH_LEADER_EPOCH_CHECKPOINT;
        }
        return FETCH_SEGMENT;
    }

    private RemoteLogSegmentFileset.RemoteLogSegmentFileType getLogSegmentFileType(IndexType indexType) {
        switch (indexType) {
            case OFFSET:
                return OFFSET_INDEX;
            case TIMESTAMP:
                return TIME_INDEX;
            case PRODUCER_SNAPSHOT:
                return PRODUCER_SNAPSHOT;
            case TRANSACTION:
                return TRANSACTION_INDEX;
            case LEADER_EPOCH:
                return LEADER_EPOCH_CHECKPOINT;
        }
        return SEGMENT;
    }

    public int brokerId() {
        return brokerId;
    }
}
