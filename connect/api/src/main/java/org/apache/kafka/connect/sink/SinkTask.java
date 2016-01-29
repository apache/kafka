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
package org.apache.kafka.connect.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.connect.connector.Task;

import java.util.Collection;
import java.util.Map;

/**
 * SinkTask is a Task takes records loaded from Kafka and sends them to another system. In
 * addition to the basic {@link #put} interface, SinkTasks must also implement {@link #flush}
 * to support offset commits.
 */
@InterfaceStability.Unstable
public abstract class SinkTask implements Task {

    /**
     * <p>
     * The configuration key that provides the list of topics that are inputs for this
     * SinkTask.
     * </p>
     */
    public static final String TOPICS_CONFIG = "topics";

    protected SinkTaskContext context;

    public void initialize(SinkTaskContext context) {
        this.context = context;
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     * @param props initial configuration
     */
    @Override
    public abstract void start(Map<String, String> props);

    /**
     * Put the records in the sink. Usually this should send the records to the sink asynchronously
     * and immediately return.
     *
     * If this operation fails, the SinkTask may throw a {@link org.apache.kafka.connect.errors.RetriableException} to
     * indicate that the framework should attempt to retry the same call again. Other exceptions will cause the task to
     * be stopped immediately. {@link SinkTaskContext#timeout(long)} can be used to set the maximum time before the
     * batch will be retried.
     *
     * @param records the set of records to send
     */
    public abstract void put(Collection<SinkRecord> records);

    /**
     * Flush all records that have been {@link #put} for the specified topic-partitions. The
     * offsets are provided for convenience, but could also be determined by tracking all offsets
     * included in the SinkRecords passed to {@link #put}.
     *
     * @param offsets mapping of TopicPartition to committed offset
     */
    public abstract void flush(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * The SinkTask use this method to create writers for newly assigned partitions in case of partition
     * re-assignment. In partition re-assignment, some new partitions may be assigned to the SinkTask.
     * The SinkTask needs to create writers and perform necessary recovery for the newly assigned partitions.
     * This method will be called after partition re-assignment completes and before the SinkTask starts
     * fetching data. Note that any errors raised from this method will cause the task to stop.
     * @param partitions The list of partitions that are now assigned to the task (may include
     *                   partitions previously assigned to the task)
     */
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    /**
     * The SinkTask use this method to close writers and commit offsets for partitions that are no
     * longer assigned to the SinkTask. This method will be called before a rebalance operation starts
     * and after the SinkTask stops fetching data. Note that any errors raised from this method will cause
     * the task to stop.
     * @param partitions The list of partitions that were assigned to the consumer on the last
     *                   rebalance
     */
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    }

    /**
     * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once outstanding calls to other
     * methods have completed (e.g., {@link #put(Collection)} has returned) and a final {@link #flush(Map)} and offset
     * commit has completed. Implementations of this method should only need to perform final cleanup operations, such
     * as closing network connections to the sink system.
     */
    public abstract void stop();
}
