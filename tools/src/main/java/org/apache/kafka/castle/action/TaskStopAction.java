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

import org.apache.kafka.castle.cluster.CastleCluster;
import org.apache.kafka.castle.cluster.CastleNode;
import org.apache.kafka.castle.common.CastleUtil;
import org.apache.kafka.castle.role.TaskRole;
import org.apache.kafka.castle.tool.CastleReturnCode;
import org.apache.kafka.trogdor.coordinator.CoordinatorClient;
import org.apache.kafka.trogdor.rest.StopTaskRequest;
import org.apache.kafka.trogdor.rest.TaskDone;
import org.apache.kafka.trogdor.rest.TaskState;
import org.apache.kafka.trogdor.rest.TasksRequest;
import org.apache.kafka.trogdor.rest.TasksResponse;

import java.util.Collection;
import java.util.concurrent.Callable;

public class TaskStopAction extends Action  {
    public final static String TYPE = "tasksStop";

    private final Collection<String> taskIds;

    public TaskStopAction(String nodeName, TaskRole role) {
        super(new ActionId(TYPE, nodeName),
            new TargetId[] {},
            new String[] {},
            role.initialDelayMs());
        this.taskIds = role.taskSpecs().keySet();
    }

    @Override
    public void call(final CastleCluster cluster, final CastleNode node) throws Throwable {
        if (node.dns().isEmpty()) {
            node.log().printf("*** Skipping taskStop, because the node has no DNS address.%n");
            return;
        }
        if (CastleUtil.getJavaProcessStatus(cluster, node,
                TrogdorDaemonType.COORDINATOR.className()) != CastleReturnCode.SUCCESS) {
            node.log().printf("*** Ignoring TaskStopAction because the Trogdor " +
                "coordinator process does not appear to be running.%n");
            return;
        }

        // Stop all the tasks.  If the task has already stopped, the StopTaskRequest
        // will be ignored.
        CastleUtil.invokeCoordinator(cluster, node, new CastleUtil.CoordinatorFunction<Void>() {
            @Override
            public Void apply(CoordinatorClient coordinatorClient) throws Exception {
                for (String taskId : taskIds) {
                    coordinatorClient.stopTask(new StopTaskRequest(taskId));
                }
                return null;
            }
        });
        // Wait for all the tasks to be stopped.
        CastleUtil.waitFor(5, 30000, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                TasksResponse tasksResponse = CastleUtil.
                    invokeCoordinator(cluster, node, new CastleUtil.CoordinatorFunction<TasksResponse>() {
                        @Override
                        public TasksResponse apply(CoordinatorClient coordinatorClient) throws Exception {
                            return coordinatorClient.tasks(TasksRequest.ALL);
                        }
                    });
                for (String taskId : taskIds) {
                    TaskState taskState = tasksResponse.tasks().get(taskId);
                    if (taskState == null) {
                        return true;
                    }
                    if (!(taskState instanceof TaskDone)) {
                        return false;
                    }
                }
                return true;
            }
        });
    }
};
