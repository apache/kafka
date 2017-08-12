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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.HostInfo;

import java.util.Map;
import java.util.Set;

/**
 * Interface used by a <code>StreamThread</code> to get metadata from the <code>StreamPartitionAssignor</code>
 */
public interface ThreadMetadataProvider {
    Map<TaskId, Set<TopicPartition>> standbyTasks();
    Map<TaskId, Set<TopicPartition>> activeTasks();
    Map<HostInfo, Set<TopicPartition>> getPartitionsByHostState();
    Cluster clusterMetadata();
    void close();
}
