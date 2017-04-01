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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.Task;

import java.util.Collection;
import java.util.Map;

/**
 * SinkTask is a Task that takes records loaded from Kafka and sends them to another system. Each task
 * instance is assigned a set of partitions by the Connect framework and will handle all records received
 * from those partitions. As records are fetched from Kafka, they will be passed to the sink task using the
 * {@link #put(Collection)} API, which should either write them to the downstream system or batch them for
 * later writing. Periodically, Connect will call {@link #flush(Map)} to ensure that batched records are
 * actually pushed to the downstream system..
 *
 * Below we describe the lifecycle of a SinkTask.
 *
 * <ol>
 *     <li><b>Initialization:</b> SinkTasks are first initialized using {@link #initialize(SinkTaskContext)}
 *     to prepare the task's context and {@link #start(Map)} to accept configuration and start any services
 *     needed for processing.</li>
 *     <li><b>Partition Assignment:</b> After initialization, Connect will assign the task a set of partitions
 *     using {@link #open(Collection)}. These partitions are owned exclusively by this task until they
 *     have been closed with {@link #close(Collection)}.</li>
 *     <li><b>Record Processing:</b> Once partitions have been opened for writing, Connect will begin forwarding
 *     records from Kafka using the {@link #put(Collection)} API. Periodically, Connect will ask the task
 *     to flush records using {@link #flush(Map)} as described above.</li>
 *     <li><b>Partition Rebalancing:</b> Occasionally, Connect will need to change the assignment of this task.
 *     When this happens, the currently assigned partitions will be closed with {@link #close(Collection)} and
 *     the new assignment will be opened using {@link #open(Collection)}.</li>
 *     <li><b>Shutdown:</b> When the task needs to be shutdown, Connect will close active partitions (if there
 *     are any) and stop the task using {@link #stop()}</li>
  * </ol>
 *
 */
public abstract class SinkTask implements Task {

    /**
     * <p>
     * The configuration key that provides the list of topics that are inputs for this
     * SinkTask.
     * </p>
     */
    public static final String TOPICS_CONFIG = "topics";

    protected SinkTaskContext context;

    /**
     * Initialize the context of this task. Note that the partition assignment will be empty until
     * Connect has opened the partitions for writing with {@link #open(Collection)}.
     * @param context The sink task's context
     */
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
     * Flush all records that have been {@link #put(Collection)} for the specified topic-partitions.
     *
     * @param currentOffsets the current offset state as of the last call to {@link #put(Collection)}},
     *                       provided for convenience but could also be determined by tracking all offsets included in the {@link SinkRecord}s
     *                       passed to {@link #put}.
     */
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    }

    /**
     * Pre-commit hook invoked prior to an offset commit.
     *
     * The default implementation simply invokes {@link #flush(Map)} and is thus able to assume all {@code currentOffsets} are safe to commit.
     *
     * @param currentOffsets the current offset state as of the last call to {@link #put(Collection)}},
     *                       provided for convenience but could also be determined by tracking all offsets included in the {@link SinkRecord}s
     *                       passed to {@link #put}.
     *
     * @return an empty map if Connect-managed offset commit is not desired, otherwise a map of offsets by topic-partition that are safe to commit.
     */
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        flush(currentOffsets);
        return currentOffsets;
    }

    /**
     * The SinkTask use this method to create writers for newly assigned partitions in case of partition
     * rebalance. This method will be called after partition re-assignment completes and before the SinkTask starts
     * fetching data. Note that any errors raised from this method will cause the task to stop.
     * @param partitions The list of partitions that are now assigned to the task (may include
     *                   partitions previously assigned to the task)
     */
    public void open(Collection<TopicPartition> partitions) {
        this.onPartitionsAssigned(partitions);
    }

    /**
     * @deprecated Use {@link #open(Collection)} for partition initialization.
     */
    @Deprecated
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    /**
     * The SinkTask use this method to close writers for partitions that are no
     * longer assigned to the SinkTask. This method will be called before a rebalance operation starts
     * and after the SinkTask stops fetching data. After being closed, Connect will not write
     * any records to the task until a new set of partitions has been opened. Note that any errors raised
     * from this method will cause the task to stop.
     * @param partitions The list of partitions that should be closed
     */
    public void close(Collection<TopicPartition> partitions) {
        this.onPartitionsRevoked(partitions);
    }

    /**
     * @deprecated Use {@link #close(Collection)} instead for partition cleanup.
     */
    @Deprecated
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
