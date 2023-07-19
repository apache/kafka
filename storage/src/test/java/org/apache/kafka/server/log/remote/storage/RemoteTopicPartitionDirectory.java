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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.slf4j.Logger;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.toSet;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.getUuid;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.deleteFilesOnly;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.deleteQuietly;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Represents a topic-partition directory in the local tiered storage under which filesets for
 * log segments are stored.
 *
 *
 * <code>
 * / storage-directory / topic-0-uuidBase64 / tvHCaSDsQZWsjr5rbtCjxA.log
 *                     .                   .  tvHCaSDsQZWsjr5rbtCjxA.index
 *                     .                   .  tvHCaSDsQZWsjr5rbtCjxA.timeindex
 *                     .
 *                     / topic-3-5fEBmixCR5-dMntYSLIr1g / BFyXlC8ySMm-Uzxw5lZSMg.log
 *                                                      . BFyXlC8ySMm-Uzxw5lZSMg.index
 *                                                      . BFyXlC8ySMm-Uzxw5lZSMg.timeindex
 * </code>
 */
public final class RemoteTopicPartitionDirectory {
    private static final Logger LOGGER = getLogger(RemoteTopicPartitionDirectory.class);
    private static final String UUID_LEGAL_CHARS = "[a-zA-Z0-9_-]{22}";

    /**
     * The format of a Kafka topic-partition directory. Follows the structure Topic-Partition-Uuid.
     */
    private static final Pattern FILENAME_FORMAT = compile("(" + Topic.LEGAL_CHARS + "+)-(\\d+)-(" + UUID_LEGAL_CHARS + ")");
    static final int GROUP_TOPIC = 1;
    static final int GROUP_PARTITION = 2;
    static final int GROUP_UUID = 3;

    private final File directory;
    private final boolean existed;
    private final TopicIdPartition topicIdPartition;

    RemoteTopicPartitionDirectory(final TopicIdPartition topicIdPartition, final File directory, final boolean existed) {
        this.topicIdPartition = requireNonNull(topicIdPartition);
        this.directory = requireNonNull(directory);
        this.existed = existed;
    }

    public TopicIdPartition getTopicIdPartition() {
        return topicIdPartition;
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
        traverser.visitTopicIdPartition(topicIdPartition);
        listFilesets().stream().forEach(fileset -> traverser.visitSegment(fileset));
    }

    private List<RemoteLogSegmentFileset> listFilesets() {
        Set<Uuid> uuids = Arrays.stream(directory.listFiles())
                .map(file -> getUuid(file.getName()))
                .collect(toSet());

        return uuids.stream()
                .map(uuid -> RemoteLogSegmentFileset.openExistingFileset(this, uuid))
                .collect(Collectors.toList());
    }

    /**
     * Creates a new {@link RemoteTopicPartitionDirectory} instance for the directory of the
     * provided topicPartition under the root directory of the local tiered storage.
     */
    public static RemoteTopicPartitionDirectory openTopicPartitionDirectory(final TopicIdPartition topicIdPartition,
                                                                            final File storageDirectory) {


        final File directory = new File(storageDirectory, toString(topicIdPartition));
        final boolean existed = directory.exists();

        if (!existed) {
            LOGGER.info("Creating directory: " + directory.getAbsolutePath());
            directory.mkdirs();
        }

        return new RemoteTopicPartitionDirectory(topicIdPartition, directory, existed);
    }

    /**
     * Creates a new {@link RemoteTopicPartitionDirectory} instance for the directory with the given
     * name under the root directory of the local tiered storage. This method throws an
     * {@link IllegalArgumentException} if the directory does not exist.
     */
    public static RemoteTopicPartitionDirectory openExistingTopicPartitionDirectory(final String dirname,
                                                                                    final File storageDirectory) {
        final Uuid uuid = Uuid.fromString(substr(dirname, GROUP_UUID));
        final String topic = substr(dirname, GROUP_TOPIC);
        final int partition;
        try {
            partition = Integer.parseInt(substr(dirname, GROUP_PARTITION));
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(format(
                    "Invalid format for topic-partition directory: %s", dirname), ex);
        }

        TopicPartition tp = new TopicPartition(topic, partition);
        TopicIdPartition idPartition = new TopicIdPartition(uuid, tp);
        final RemoteTopicPartitionDirectory directory = openTopicPartitionDirectory(idPartition, storageDirectory);
        if (!directory.didExist()) {
            throw new IllegalArgumentException(format("Topic-partition directory %s not found", dirname));
        }
        return directory;
    }

    private static String toString(TopicIdPartition topicIdPartition) {
        Uuid uuid = topicIdPartition.topicId();
        TopicPartition tp = topicIdPartition.topicPartition();
        return tp.topic() + "-" + tp.partition() + "-" + uuid.toString();
    }

    static String substr(final String filename, final int group) {
        final Matcher m = FILENAME_FORMAT.matcher(filename);
        if (!m.matches()) {
            throw new IllegalArgumentException(format("Not a topic partition directory file: %s", filename));
        }
        return m.group(group);
    }
}
