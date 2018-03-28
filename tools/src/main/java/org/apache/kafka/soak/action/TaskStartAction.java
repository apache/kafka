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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.common.JsonTransformer;
import org.apache.kafka.soak.common.SoakUtil;
import org.apache.kafka.soak.common.SoakUtil.CoordinatorFunction;
import org.apache.kafka.soak.tool.SoakTool;
import org.apache.kafka.trogdor.coordinator.CoordinatorClient;
import org.apache.kafka.trogdor.rest.CreateTaskRequest;
import org.apache.kafka.trogdor.task.TaskSpec;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class TaskStartAction extends Action  {
    public final static String TYPE = "taskStart";

    private final Map<String, TaskSpec> taskSpecs;

    public TaskStartAction(String scope, Map<String, TaskSpec> taskSpecs) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {
                new TargetId(DaemonStartAction.TYPE),
                new TargetId(TrogdorDaemonType.COORDINATOR.startType())
            },
            new String[] {});
        this.taskSpecs = taskSpecs;
    }

    @Override
    public void call(final SoakCluster cluster, SoakNode node) throws Throwable {
        Thread.sleep(cluster.env().trogdorTaskDelayMs());
        SoakUtil.invokeCoordinator(cluster, node, new CoordinatorFunction<Void>() {
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
     * @param cluster       The soak cluster.
     * @return              The transformed list of task specs.
     */
    private Map<String, TaskSpec> createTransformedTaskSpecs(SoakCluster cluster)
            throws Exception {
        Map<String, String> transforms = getTransforms(cluster);
        Map<String, TaskSpec> transformedSpecs = new TreeMap<>();
        for (Map.Entry<String, TaskSpec> entry : taskSpecs.entrySet()) {
            JsonNode inputNode = SoakTool.JSON_SERDE.valueToTree(entry.getValue());
            JsonNode outputNode = JsonTransformer.transform(inputNode, transforms);
            TaskSpec taskSpec = SoakTool.JSON_SERDE.
                treeToValue(outputNode, TaskSpec.class);
            transformedSpecs.put(entry.getKey(), taskSpec);
        }
        return transformedSpecs;
    }

    private Map<String, String> getTransforms(SoakCluster cluster) {
        HashMap<String, String> transforms = new HashMap<>();
        transforms.put("bootstrapServers", cluster.getBootstrapServers());
        return transforms;
    }
};
