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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.castle.cluster.CastleCluster;
import org.apache.kafka.castle.cluster.CastleNode;
import org.apache.kafka.castle.common.JsonTransformer;
import org.apache.kafka.castle.common.CastleUtil;
import org.apache.kafka.castle.common.CastleUtil.CoordinatorFunction;
import org.apache.kafka.castle.role.TaskRole;
import org.apache.kafka.castle.tool.CastleTool;
import org.apache.kafka.trogdor.coordinator.CoordinatorClient;
import org.apache.kafka.trogdor.rest.CreateTaskRequest;
import org.apache.kafka.trogdor.task.TaskSpec;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class TaskStartAction extends Action  {
    public final static String TYPE = "taskStart";

    private final Map<String, TaskSpec> taskSpecs;

    public TaskStartAction(String scope, TaskRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {
                new TargetId(DaemonStartAction.TYPE),
                new TargetId(TrogdorDaemonType.COORDINATOR.startType())
            },
            new String[] {},
            role.initialDelayMs());
        this.taskSpecs = role.taskSpecs();
    }

    @Override
    public void call(final CastleCluster cluster, CastleNode node) throws Throwable {
        CastleUtil.invokeCoordinator(cluster, node, new CoordinatorFunction<Void>() {
            @Override
            public Void apply(CoordinatorClient coordinatorClient) throws Exception {
                for (Map.Entry<String, TaskSpec> entry :
                        createTransformedTaskSpecs(cluster).entrySet()) {
                    String taskId = entry.getKey();
                    TaskSpec taskSpec = entry.getValue();
                    coordinatorClient.createTask(new CreateTaskRequest(taskId, taskSpec));
                }
                return null;
            }
        });
    }

    /**
     * Get a list of task specs to which transforms have been applied.
     *
     * @param cluster       The castle cluster.
     * @return              The transformed list of task specs.
     */
    private Map<String, TaskSpec> createTransformedTaskSpecs(CastleCluster cluster)
            throws Exception {
        Map<String, String> transforms = getTransforms(cluster);
        Map<String, TaskSpec> transformedSpecs = new TreeMap<>();
        for (Map.Entry<String, TaskSpec> entry : taskSpecs.entrySet()) {
            JsonNode inputNode = CastleTool.JSON_SERDE.valueToTree(entry.getValue());
            JsonNode outputNode = JsonTransformer.transform(inputNode, transforms);
            TaskSpec taskSpec = CastleTool.JSON_SERDE.
                treeToValue(outputNode, TaskSpec.class);
            transformedSpecs.put(entry.getKey(), taskSpec);
        }
        return transformedSpecs;
    }

    private Map<String, String> getTransforms(CastleCluster cluster) {
        HashMap<String, String> transforms = new HashMap<>();
        transforms.put("bootstrapServers", cluster.getBootstrapServers());
        return transforms;
    }
};
