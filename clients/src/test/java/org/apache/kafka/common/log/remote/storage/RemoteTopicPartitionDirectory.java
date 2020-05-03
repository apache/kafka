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
import org.slf4j.Logger;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.getUUID;
import static org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.deleteFilesOnly;
import static org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.deleteQuietly;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Represents a topic-partition directory in the local tiered storage under which filesets for
 * log segments are stored.
 *
 * <code>
 * / storage-directory / a-topic-1 / 82da091b-84f5-4d72-9ceb-3532a1f3a4c1-1-segment
 *                     .           . 82da091b-84f5-4d72-9ceb-3532a1f3a4c1-1-offset_index
 *                     .           . 82da091b-84f5-4d72-9ceb-3532a1f3a4c1-1-time_index
 *                     .
 *                     / b-topic-3 / df2bbd78-3bfd-438c-a4ff-29a45a4d4e9d-0-segment
 *                                 . df2bbd78-3bfd-438c-a4ff-29a45a4d4e9d-0-offset_index
 *                                 . df2bbd78-3bfd-438c-a4ff-29a45a4d4e9d-0-time_index
 * </code>
 */
public final class RemoteTopicPartitionDirectory {
    private static final Logger LOGGER = getLogger(RemoteLogSegmentFileset.class);

    private final File directory;
    private final boolean existed;
    private final TopicPartition topicPartition;

    RemoteTopicPartitionDirectory(final TopicPartition topicPartition, final File directory, final boolean existed) {
        this.topicPartition = requireNonNull(topicPartition);
        this.directory = requireNonNull(directory);
        this.existed = existed;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    boolean didExist() {
        return existed;
    }

    public File getDirectory() {
        return directory;
    }

    boolean delete() {
        return deleteFilesOnly(asList(directory.listFiles())) && deleteQuietly(directory);
    }

    void traverse(final LocalTieredStorageTraverser traverser) {
        traverser.visitTopicPartition(topicPartition);
        listFilesets().stream().forEach(fileset -> traverser.visitSegment(fileset));
    }

    private List<RemoteLogSegmentFileset> listFilesets() {
        Set<UUID> uuids = Arrays.stream(directory.listFiles())
                .map(file -> getUUID(file.getName()))
                .collect(toSet());

        return uuids.stream()
                .map(uuid -> RemoteLogSegmentFileset.openExistingFileset(this, uuid))
                .collect(Collectors.toList());
    }

    /**
     * Creates a new {@link RemoteTopicPartitionDirectory} instance for the directory of the
     * provided topicPartition under the root directory of the local tiered storage.
     */
    public static RemoteTopicPartitionDirectory openTopicPartitionDirectory(final TopicPartition topicPartition,
                                                                            final File storageDirectory) {

        final File directory = new File(storageDirectory, topicPartition.toString());
        final boolean existed = directory.exists();

        if (!existed) {
            LOGGER.info("Creating directory: " + directory.getAbsolutePath());
            directory.mkdirs();
        }

        return new RemoteTopicPartitionDirectory(topicPartition, directory, existed);
    }

    /**
     * Creates a new {@link RemoteTopicPartitionDirectory} instance for the directory with the given
     * name under the root directory of the local tiered storage. This method throws an
     * {@link IllegalArgumentException} if the directory does not exist.
     */
    public static RemoteTopicPartitionDirectory openExistingTopicPartitionDirectory(final String dirname,
                                                                                    final File storageDirectory) {

        final char topicParitionSeparator = '-';
        final int separatorIndex = dirname.lastIndexOf(topicParitionSeparator);

        if (separatorIndex == -1) {
            throw new IllegalArgumentException(format(
                    "Invalid format for topic-partition directory: %s", dirname));
        }

        final String topic = dirname.substring(0, separatorIndex);
        final int partition;

        try {
            partition = Integer.parseInt(dirname.substring(separatorIndex + 1));

        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(format(
                    "Invalid format for topic-partition directory: %s", dirname), ex);
        }

        final RemoteTopicPartitionDirectory directory =
                openTopicPartitionDirectory(new TopicPartition(topic, partition), storageDirectory);

        if (!directory.existed) {
            throw new IllegalArgumentException(format("Topic-partitition directory %s not found", dirname));
        }

        return directory;
    }
}
