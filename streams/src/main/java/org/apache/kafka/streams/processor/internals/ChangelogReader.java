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
     */
    void restore(final Map<TaskId, Task> tasks);

    /**
     * Transit to restore active changelogs mode
     */
    void enforceRestoreActive();

    /**
     * Transit to update standby changelogs mode
     */
    void transitToUpdateStandby();

    /**
     * @return the changelog partitions that have been completed restoring
     */
    Set<TopicPartition> completedChangelogs();

    /**
     * Clear all partitions
     */
    void clear();

    /**
     * @return whether the changelog reader has just been cleared or is uninitialized
     */
    boolean isEmpty();
}
