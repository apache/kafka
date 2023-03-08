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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Map;
import java.util.Set;

/**
 * See {@link StoreChangelogReader}.
 */
public interface ChangelogReader extends ChangelogRegister {
    /**
     * Restore all registered state stores by reading from their changelogs
     *
     * @return the total number of records restored in this call
     */
    long restore(final Map<TaskId, Task> tasks);

    /**
     * Transit to restore active changelogs mode
     */
    void enforceRestoreActive();

    /**
     * Transit to update standby changelogs mode
     */
    void transitToUpdateStandby();

    /**
     * @return true if the reader is in restoring active changelog mode;
     *         false if the reader is in updating standby changelog mode
     */
    boolean isRestoringActive();

    /**
     * @return the changelog partitions that have been completed restoring
     */
    Set<TopicPartition> completedChangelogs();

    /**
     * Returns whether all changelog partitions were completely read.
     *
     * Since changelog partitions for standby tasks are never completely read, this method will always return
     * {@code false} if the changelog reader registered changelog partitions for standby tasks.
     *
     * @return {@code true} if all changelog partitions were completely read and no standby changelog partitions are read,
     *         {@code false} otherwise
     */
    boolean allChangelogsCompleted();

    /**
     * Clear all partitions
     */
    void clear();

    /**
     * @return whether the changelog reader has just been cleared or is uninitialized
     */
    boolean isEmpty();
}
