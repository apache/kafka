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
package org.apache.kafka.connect.sink;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

/**
 * Context passed to SinkTasks, allowing them to access utilities in the Kafka Connect runtime.
 */
public interface SinkTaskContext {

    /**
     * Get the Task configuration.  This is the latest configuration and may differ from that passed on startup.
     *
     * For example, this method can be used to obtain the latest configuration if an external secret has changed,
     * and the configuration is using variable references such as those compatible with
     * {@link org.apache.kafka.common.config.ConfigTransformer}.
     */
    Map<String, String> configs();

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
    void offset(Map<TopicPartition, Long> offsets);

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
    void offset(TopicPartition tp, long offset);

    /**
     * Set the timeout in milliseconds. SinkTasks should use this to indicate that they need to retry certain
     * operations after the timeout. SinkTasks may have certain operations on external systems that may need
     * to retry in case of failures. For example, append a record to an HDFS file may fail due to temporary network
     * issues. SinkTasks use this method to set how long to wait before retrying.
     * @param timeoutMs the backoff timeout in milliseconds.
     */
    void timeout(long timeoutMs);

    /**
     * Get the current set of assigned TopicPartitions for this task.
     * @return the set of currently assigned TopicPartitions
     */
    Set<TopicPartition> assignment();

    /**
     * Pause consumption of messages from the specified TopicPartitions.
     * @param partitions the partitions which should be paused
     */
    void pause(TopicPartition... partitions);

    /**
     * Resume consumption of messages from previously paused TopicPartitions.
     * @param partitions the partitions to resume
     */
    void resume(TopicPartition... partitions);

    /**
     * Request an offset commit. Sink tasks can use this to minimize the potential for redelivery
     * by requesting an offset commit as soon as they flush data to the destination system.
     *
     * It is only a hint to the runtime and no timing guarantee should be assumed.
     */
    void requestCommit();

    /**
     * Get the reporter to which the sink task can report problematic or failed {@link SinkRecord records}
     * passed to the {@link SinkTask#put(java.util.Collection)} method. When reporting a failed record,
     * the sink task will receive a {@link java.util.concurrent.Future} that the task can optionally use to wait until
     * the failed record and exception have been written to Kafka. Note that the result of
     * this method may be null if this connector has not been configured to use a reporter.
     *
     * <p>This method was added in Apache Kafka 2.6. Sink tasks that use this method but want to
     * maintain backward compatibility so they can also be deployed to older Connect runtimes
     * should guard the call to this method with a try-catch block, since calling this method will result in a
     * {@link NoSuchMethodException} or {@link NoClassDefFoundError} when the sink connector is deployed to
     * Connect runtimes older than Kafka 2.6. For example:
     * <pre>
     *     ErrantRecordReporter reporter;
     *     try {
     *         reporter = context.errantRecordReporter();
     *     } catch (NoSuchMethodError | NoClassDefFoundError e) {
     *         reporter = null;
     *     }
     * </pre>
     *
     * @return the reporter; null if no error reporter has been configured for the connector
     * @since 2.6
     */
    default ErrantRecordReporter errantRecordReporter() {
        return null;
    }

}
