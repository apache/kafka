/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.sink;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Context passed to SinkTasks, allowing them to access utilities in the copycat runtime.
 */
@InterfaceStability.Unstable
public abstract class SinkTaskContext {
    protected Map<TopicPartition, Long> offsets;
    protected long timeoutMs = -1L;

    public SinkTaskContext() {
        offsets = new HashMap<>();
    }

    /**
     * Reset the consumer offsets for the given topic partitions. SinkTasks should use this if they manage offsets
     * in the sink data store rather than using Kafka consumer offsets. For example, an HDFS connector might record
     * offsets in HDFS to provide exactly once delivery. When the SinkTask is started or a rebalance occurs, the task
     * would reload offsets from HDFS and use this method to reset the consumer to those offsets.
     *
     * SinkTasks that do not manage their own offsets do not need to use this method.
     *
     * @param offsets map of offsets for topic partitions
     */
    public void offset(Map<TopicPartition, Long> offsets) {
        this.offsets = offsets;
    }

    /**
     * Reset the consumer offsets for the given topic partition. SinkTasks should use if they manage offsets
     * in the sink data store rather than using Kafka consumer offsets. For example, an HDFS connector might record
     * offsets in HDFS to provide exactly once delivery. When the topic partition is recovered the task
     * would reload offsets from HDFS and use this method to reset the consumer to the offset.
     *
     * SinkTasks that do not manage their own offsets do not need to use this method.
     *
     * @param tp the topic partition to reset offset.
     * @param offset the offset to reset to.
     */
    public void offset(TopicPartition tp, long offset) {
        offsets.put(tp, offset);
    }

    /**
     * Set the timeout in milliseconds. SinkTasks should use this to indicate that they need to retry certain
     * operations after the timeout. SinkTasks may have certain operations on external systems that may need
     * to retry in case of failures. For example, append a record to an HDFS file may fail due to temporary network
     * issues. SinkTasks use this method to set how long to wait before retrying.
     * @param timeoutMs the backoff timeout in milliseconds.
     */
    public void timeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    /**
     * Get the current set of assigned TopicPartitions for this task.
     * @return the set of currently assigned TopicPartitions
     */
    public abstract Set<TopicPartition> assignment();

    /**
     * Pause consumption of messages from the specified TopicPartitions.
     * @param partitions the partitions which should be paused
     */
    public abstract void pause(TopicPartition... partitions);

    /**
     * Resume consumption of messages from previously paused TopicPartitions.
     * @param partitions the partitions to resume
     */
    public abstract void resume(TopicPartition... partitions);
}
