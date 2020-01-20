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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.TaskId;

class AssignedStandbyTasks extends AssignedTasks<StandbyTask> {

    AssignedStandbyTasks(final LogContext logContext) {
        super(logContext, "standby task");
    }

    @Override
    public void shutdown(final boolean clean) {
        final String shutdownType = clean ? "Clean" : "Unclean";
        log.debug("{} shutdown of all standby tasks" + "\n" +
                      "non-initialized standby tasks to close: {}" + "\n" +
                      "running standby tasks to close: {}",
            shutdownType, created.keySet(), running.keySet());
        super.shutdown(clean);
    }

    @Override
    int commit() {
        final int committed = super.commit();
        // TODO: this contortion would not be necessary if we got rid of the two-step
        // task.commitNeeded and task.commit and instead just had task.commitIfNeeded. Currently
        // we only call commit if commitNeeded is true, which means that we need a way to indicate
        // that we are eligible for updating the offset limit outside of commit.
        running.forEach((id, task) -> task.allowUpdateOfOffsetLimit());
        return committed;
    }

    /**
     * Closes standby tasks that were reassigned elsewhere after a rebalance.
     *
     * @param revokedTasks the tasks which are no longer owned
     * @return the changelogs of all standby tasks that were reassigned
     */
    List<TopicPartition> closeRevokedStandbyTasks(final Map<TaskId, Set<TopicPartition>> revokedTasks) {
        log.debug("Closing revoked standby tasks {}", revokedTasks);

        final List<TopicPartition> revokedChangelogs = new ArrayList<>();
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : revokedTasks.entrySet()) {
            final TaskId taskId = entry.getKey();
            final StandbyTask task;

            if (running.containsKey(taskId)) {
                task = running.get(taskId);
            } else if (created.containsKey(taskId)) {
                task = created.get(taskId);
            } else {
                log.error("Could not find the standby task {} while closing it", taskId);
                continue;
            }

            try {
                task.close(true, false);
            } catch (final RuntimeException e) {
                log.error("Closing the standby task {} failed due to the following error:", task.id(), e);
            } finally {
                removeTaskFromAllStateMaps(task, Collections.emptyMap());
                revokedChangelogs.addAll(task.changelogPartitions());
            }
        }
        return revokedChangelogs;
    }

}
