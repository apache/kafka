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
package org.apache.kafka.common.log.remote.metadata.storage;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;

import java.io.IOException;

/**
 * This is invoked by {@link ConsumerTask} whenever there are updates for a topic partition on
 * RemoteLogSegmentMetadata.
 */
public interface RemoteLogSegmentMetadataUpdater {

    /**
     * Update RemoteLogSegmentMetadata for a topic partition.
     *
     * @param tp
     * @param remoteLogSegmentMetadata
     */
    void updateRemoteLogSegmentMetadata(TopicPartition tp, RemoteLogSegmentMetadata remoteLogSegmentMetadata);

    /**
     * Sync the remote log metadata state maintained for this broker.
     *
     * @throws IOException
     */
    void syncLogMetadataDataFile() throws IOException;

    /**
     * It returns partition number of remote log metadata topic for the given topic partition.
     *
     * @param tp
     * @return
     */
    int metadataPartitionFor(TopicPartition tp);
}
