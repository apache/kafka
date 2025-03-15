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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.assignors.StickyTaskAssignor;

public class TestTaskAssignor extends StickyTaskAssignor {

    @Override
    public void onAssignmentComputed(final ConsumerPartitionAssignor.GroupAssignment assignment,
                                     final ConsumerPartitionAssignor.GroupSubscription subscription,
                                     final AssignmentError error) {
        if (assignment.groupAssignment().size() == 1) {
            return;
        }

        for (final String threadName : assignment.groupAssignment().keySet()) {
            if (threadName.contains("-StreamThread-1-")) {
                final TaskId taskWithData =  EosIntegrationTest.TASK_WITH_DATA.get();
                if (taskWithData != null && taskWithData.partition() == assignment.groupAssignment().get(threadName).partitions().get(0).partition()) {
                    EosIntegrationTest.DID_REVOKE_IDLE_TASK.set(true);
                }
            }
        }
    }
}
