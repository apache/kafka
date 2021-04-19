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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;

import java.io.IOException;
import java.nio.file.Path;

public class FileBasedRemoteLogMetadataCache extends RemoteLogMetadataCache {

    private final CommittedLogMetadataFile committedLogMetadataFile;
    private final TopicIdPartition topicIdPartition;

    public FileBasedRemoteLogMetadataCache(TopicIdPartition topicIdPartition,
                                           Path partitionDir) {
        if (!partitionDir.toFile().exists() || !partitionDir.toFile().isDirectory()) {
            throw new KafkaException("Given partition directory:" + partitionDir + " must be an existing directory.");
        }

        this.topicIdPartition = topicIdPartition;
        committedLogMetadataFile = new CommittedLogMetadataFile(topicIdPartition, partitionDir);

        try {
            committedLogMetadataFile.read().ifPresent(snapshot -> loadRemoteLogSegmentMetadata(snapshot.remoteLogMetadatas()));
        } catch (IOException e) {
            throw new KafkaException(e);
        }

    }

    public void flushToFile(int metadataPartition,
                            Long metadataPartitionOffset) throws IOException {
        // todo-tier take a lock here as the metadata may be getting modified.
        // For each topic partition, there is only one thread updating the cache. But in future, if we want to parallelize then we may need to take
        // a lock.
        committedLogMetadataFile.write(new CommittedLogMetadataFile.Snapshot(topicIdPartition.topicId(), metadataPartition, metadataPartitionOffset,
                                                                             idToSegmentMetadata.values()));
    }
}
