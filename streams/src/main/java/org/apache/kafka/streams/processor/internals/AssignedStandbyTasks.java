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

import org.apache.kafka.common.utils.LogContext;

class AssignedStandbyTasks extends AssignedTasks<StandbyTask> {

    AssignedStandbyTasks(final LogContext logContext) {
        super(logContext, "standby task");
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
}
