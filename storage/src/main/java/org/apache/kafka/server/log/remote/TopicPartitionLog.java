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
package org.apache.kafka.server.log.remote;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import java.util.Optional;

/**
 * Interface to decouple RemoteLogManager and Partition
 */
public interface TopicPartitionLog {

    /**
     * The topic name and partition number of the partition
     * @return A TopicPartition of the partition
     */
    TopicPartition topicPartition();

    /**
     * The log of the topic partition if present
     * @return The log of the partition or empty
     */
    Optional<UnifiedLog> unifiedLog();
}
