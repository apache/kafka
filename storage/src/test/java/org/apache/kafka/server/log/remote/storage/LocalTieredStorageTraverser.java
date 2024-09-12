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

/**
 * Used to walk through a local remote storage, providing a support to tests to explore the content of the storage.
 * This interface is to be used with the {@link LocalTieredStorage} and is intended for tests only.
 */
public interface LocalTieredStorageTraverser {

    /**
     * Called when a new topic-partition stored on the remote storage is discovered.
     * @param topicIdPartition The new topic-partition discovered.
     */
    void visitTopicIdPartition(TopicIdPartition topicIdPartition);

    /**
     * Called when a new segment is discovered for a given topic-partition.
     * This method can only be called after {@link LocalTieredStorageTraverser#visitTopicIdPartition(TopicIdPartition)}
     * for the topic-partition the segment belongs to.
     */
    void visitSegment(RemoteLogSegmentFileset fileset);

}
