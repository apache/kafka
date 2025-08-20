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
package org.apache.kafka.server.common;

import org.apache.kafka.common.TopicPartition;

import java.util.Objects;

/**
 * A topic partition that brokers should not replicate anymore. Flags indicate is the partition should also be deleted.
 */
public class StopPartition {

    public final TopicPartition topicPartition;
    public final boolean deleteLocalLog;
    public final boolean deleteRemoteLog;
    public final boolean stopRemoteLogMetadataManager;

    public StopPartition(TopicPartition topicPartition, boolean deleteLocalLog, boolean deleteRemoteLog, boolean stopRemoteLogMetadataManager) {
        this.topicPartition = topicPartition;
        this.deleteLocalLog = deleteLocalLog;
        this.deleteRemoteLog = deleteRemoteLog;
        this.stopRemoteLogMetadataManager = stopRemoteLogMetadataManager;
    }

    @Override
    public String toString() {
        return "StopPartition(" +
                "topicPartition=" + topicPartition +
                ", deleteLocalLog=" + deleteLocalLog +
                ", deleteRemoteLog=" + deleteRemoteLog +
                ", stopRemoteLogMetadataManager=" + stopRemoteLogMetadataManager +
                ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StopPartition that = (StopPartition) o;
        return deleteLocalLog == that.deleteLocalLog &&
               deleteRemoteLog == that.deleteRemoteLog &&
               stopRemoteLogMetadataManager == that.stopRemoteLogMetadataManager &&
               Objects.equals(topicPartition, that.topicPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, deleteLocalLog, deleteRemoteLog, stopRemoteLogMetadataManager);
    }
}
