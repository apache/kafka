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

import org.apache.kafka.soak.cloud.Cloud.InstanceDescription;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.common.SoakLog;
import org.apache.kafka.soak.role.AwsNodeRole;

import java.util.Collection;

/**
 * Checks the status of a node's AWS instance.
 */
public final class AwsCheckAction extends Action {
    public final static String TYPE = "awsCheck";

    private final AwsNodeRole role;

    public AwsCheckAction(String scope, AwsNodeRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {},
            new String[] {},
            role.initialDelayMs());
        this.role = role;
    }

    @Override
    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
        Collection<InstanceDescription> descriptions = cluster.cloud().describeInstances();
        String instanceId = role.instanceId();
        if (instanceId.isEmpty()) {
            SoakLog.printToAll(String.format("*** No AWS instanceID configured.%n"),
                node.log(), cluster.clusterLog());
        } else {
            boolean found = false;
            for (InstanceDescription description : descriptions) {
                if (description.instanceId().equals(instanceId)) {
                    found = true;
                }
            }
            if (found) {
                SoakLog.printToAll(String.format("*** Found instanceID %s.%n", instanceId),
                    node.log(), cluster.clusterLog());
            } else {
                SoakLog.printToAll(String.format("*** Failed to find instanceID %s.%n", instanceId),
                    node.log(), cluster.clusterLog());
            }
        }
        for (InstanceDescription description : descriptions) {
            node.log().printf("*** %s%n", description);
        }
    }
}
