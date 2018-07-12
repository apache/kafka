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

import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.role.AwsNodeRole;
import org.apache.kafka.soak.tool.SoakWriteClusterFileHook;

/**
 * Destroys a node.
 */
public final class AwsDestroyAction extends Action {
    public final static String TYPE = "awsDestroy";

    private final AwsNodeRole role;

    public AwsDestroyAction(String scope, AwsNodeRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {},
            new String[] {},
            role.initialDelayMs());
        this.role = role;
    }

    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
        String instanceId = role.instanceId();
        if (!instanceId.isEmpty()) {
            cluster.cloud().terminateInstances(instanceId);
        }
        role.setPublicDns("");
        role.setPublicDns("");
        role.setInstanceId("");
        cluster.shutdownManager().addHookIfMissing(new SoakWriteClusterFileHook(cluster));
    }
}
