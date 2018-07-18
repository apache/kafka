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

package org.apache.kafka.castle.action;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.castle.cluster.CastleCluster;
import org.apache.kafka.castle.cluster.CastleNode;
import org.apache.kafka.castle.common.CastleUtil;
import org.apache.kafka.castle.common.CastleUtil.CoordinatorFunction;
import org.apache.kafka.castle.role.TaskRole;
import org.apache.kafka.castle.tool.CastleReturnCode;
import org.apache.kafka.trogdor.coordinator.CoordinatorClient;
import org.apache.kafka.trogdor.rest.TaskDone;
import org.apache.kafka.trogdor.rest.TaskState;
import org.apache.kafka.trogdor.rest.TasksRequest;
import org.apache.kafka.trogdor.rest.TasksResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class TaskStatusAction extends Action  {
    public final static String TYPE = "taskStatus";

    private final TaskRole role;

    public TaskStatusAction(String scope, TaskRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {
                new TargetId(DaemonStatusAction.TYPE)
            },
            new String[] {},
            role.initialDelayMs());
        this.role = role;
    }

    @Override
    public void call(final CastleCluster cluster, CastleNode node) throws Throwable {
        try {
            TasksResponse response = CastleUtil.invokeCoordinator(
                cluster, node, new CoordinatorFunction<TasksResponse>() {
                    @Override
                    public TasksResponse apply(CoordinatorClient coordinatorClient) throws Exception {
                        TasksResponse response = coordinatorClient.tasks(TasksRequest.ALL);
                        if (response == null) {
                            throw new RuntimeException("Invalid null TaskResponse");
                        }
                        return response;
                    }
                });
            ObjectNode results = new ObjectNode(JsonNodeFactory.instance);
            for (String taskId : role.taskSpecs().keySet()) {
                TaskState state = response.tasks().get(taskId);
                if (state == null) {
                    cluster.clusterLog().printf("Unable to find task %s%n", taskId);
                    cluster.shutdownManager().changeReturnCode(CastleReturnCode.CLUSTER_FAILED);
                } else if (state instanceof TaskDone) {
                    TaskDone doneState = (TaskDone) state;
                    if (doneState.error().isEmpty()) {
                        cluster.clusterLog().printf("Task %s succeeded with status %s%n",
                            taskId, doneState.status());
                    } else {
                        cluster.clusterLog().printf("Task %s failed with error %s%n",
                            taskId, doneState.error());
                        cluster.shutdownManager().changeReturnCode(CastleReturnCode.CLUSTER_FAILED);
                    }
                    results.set(taskId, state.status());
                } else {
                    cluster.clusterLog().printf("Task %s is in progress with status %s%n",
                        taskId, state.status());
                    if (role.waitFor().contains(taskId)) {
                        cluster.shutdownManager().changeReturnCode(CastleReturnCode.IN_PROGRESS);
                    }
                }
            }
        } catch (Throwable e) {
            cluster.clusterLog().info("Error getting trogdor tasks status", e);
            cluster.shutdownManager().changeReturnCode(CastleReturnCode.TOOL_FAILED);
        }
    }
};
