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

package org.apache.kafka.soak.action;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.common.SoakUtil;
import org.apache.kafka.soak.common.SoakUtil.CoordinatorFunction;
import org.apache.kafka.soak.tool.SoakReturnCode;
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

    private final Collection<String> taskIds;

    public TaskStatusAction(String scope, Collection<String> taskIds) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {
                new TargetId(DaemonStatusAction.TYPE)
            },
            new String[] {});
        this.taskIds = Collections.unmodifiableCollection(taskIds == null ?
            new ArrayList<String>() : taskIds);
    }

    @Override
    public void call(final SoakCluster cluster, SoakNode node) throws Throwable {
        try {
            TasksResponse response = SoakUtil.invokeCoordinator(
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
            for (String taskId : taskIds) {
                TaskState state = response.tasks().get(taskId);
                if (state == null) {
                    cluster.clusterLog().printf("Unable to find task %s%n", taskId);
                    cluster.shutdownManager().changeReturnCode(SoakReturnCode.CLUSTER_FAILED);
                } else if (state instanceof TaskDone) {
                    TaskDone doneState = (TaskDone) state;
                    if (doneState.error().isEmpty()) {
                        cluster.clusterLog().printf("Task %s succeeded with status %s%n",
                            taskId, doneState.status());
                    } else {
                        cluster.clusterLog().printf("Task %s failed with error %s%n",
                            taskId, doneState.error());
                        cluster.shutdownManager().changeReturnCode(SoakReturnCode.CLUSTER_FAILED);
                    }
                    results.set(taskId, state.status());
                } else {
                    cluster.clusterLog().printf("Task %s is in progress with status %s%n",
                        taskId, state.status());
                    cluster.shutdownManager().changeReturnCode(SoakReturnCode.IN_PROGRESS);
                }
            }
        } catch (Throwable e) {
            cluster.clusterLog().info("Error getting trogdor tasks status", e);
            cluster.shutdownManager().changeReturnCode(SoakReturnCode.TOOL_FAILED);
        }
    }
};
