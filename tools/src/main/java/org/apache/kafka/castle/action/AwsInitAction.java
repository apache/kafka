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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.castle.cloud.Cloud;
import org.apache.kafka.castle.cluster.CastleCluster;
import org.apache.kafka.castle.cluster.CastleNode;
import org.apache.kafka.castle.common.CastleLog;
import org.apache.kafka.castle.role.AwsNodeRole;
import org.apache.kafka.castle.tool.CastleReturnCode;
import org.apache.kafka.castle.tool.CastleShutdownHook;
import org.apache.kafka.castle.tool.CastleTool;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Initiates a new AWS node.
 */
public final class AwsInitAction extends Action {
    public final static String TYPE = "awsInit";

    private final static int DNS_POLL_DELAY_MS = 200;

    private final static int SSH_POLL_DELAY_MS = 200;

    private final AwsNodeRole role;

    public AwsInitAction(String scope, AwsNodeRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {},
            new String[] {},
            role.initialDelayMs());
        this.role = role;
    }

    @Override
    public void call(final CastleCluster cluster, final CastleNode node) throws Throwable {
        if (new File(cluster.env().clusterOutputPath()).exists()) {
            throw new RuntimeException("Output cluster path " + cluster.env().clusterOutputPath() +
                " already exists.");
        }
        // Create a new instance.
        node.log().printf("*** Creating new instance with instance type %s, imageId %s%n",
            role.instanceType(), role.imageId());
        role.setInstanceId(cluster.cloud().newRunner().
            instanceType(role.instanceType()).
            imageId(role.imageId()).
            run());

        // Make sure that we don't leak an AWS instance if we shut down unexpectedly.
        cluster.shutdownManager().addHookIfMissing(new DestroyAwsInstancesShutdownHook(cluster));

        // Wait for the DNS to be set up.
        do {
            if (DNS_POLL_DELAY_MS > 0) {
                Thread.sleep(DNS_POLL_DELAY_MS);
            }
        } while (!checkInstanceDns(cluster, node));

        // Wait for the SSH to work
        do {
            if (SSH_POLL_DELAY_MS > 0) {
                Thread.sleep(SSH_POLL_DELAY_MS);
            }
        } while (!checkInstanceSsh(cluster, node));

        // Write out the new cluster file.
        CastleTool.JSON_SERDE.writeValue(new File(cluster.env().clusterOutputPath()), cluster.toSpec());
    }

    private boolean checkInstanceDns(CastleCluster cluster, CastleNode node) throws Throwable {
        Cloud.InstanceDescription description = cluster.cloud().describeInstance(role.instanceId());
        if (description.privateDns().isEmpty()) {
            node.log().printf("*** Waiting for private DNS name for %s...%n", role.instanceId());
            return false;
        }
        if (description.publicDns().isEmpty()) {
            node.log().printf("*** Waiting for public DNS name for %s...%n", role.instanceId());
            return false;
        }
        node.log().printf("*** Got privateDnsName = %s, publicDnsName = %s%n",
            description.privateDns(), description.publicDns());
        role.setPrivateDns(description.privateDns());
        role.setPublicDns(description.publicDns());
        return true;
    }

    private boolean checkInstanceSsh(CastleCluster cluster, CastleNode node) throws Throwable {
        try {
            cluster.cloud().remoteCommand(node).args("-n", "--", "echo").mustRun();
        } catch (Exception e) {
            node.log().printf("*** Unable to ssh to %s: %s%n",
                node.nodeName(), e.getMessage());
            return false;
        }
        CastleLog.printToAll(String.format("*** Successfully created an AWS node for %s%n",
            node.nodeName()), node.log(), cluster.clusterLog());
        return true;
    }

    /**
     * Destroys an AWS instance on shutdown.
     */
    public static final class DestroyAwsInstancesShutdownHook extends CastleShutdownHook {
        private final CastleCluster cluster;

        DestroyAwsInstancesShutdownHook(CastleCluster cluster) {
            super("DestroyAwsInstancesShutdownHook");
            this.cluster = cluster;
        }

        @Override
        public void run(CastleReturnCode returnCode) throws Throwable {
            if (returnCode == CastleReturnCode.SUCCESS) {
                String path = cluster.env().clusterOutputPath();
                try {
                    CastleTool.JSON_SERDE.writeValue(new File(path), cluster.toSpec());
                    cluster.clusterLog().printf("*** Wrote new cluster file to %s%n", path);
                } catch (Throwable e) {
                    cluster.clusterLog().printf("*** Failed to write cluster file to %s%n", path, e);
                    terminateInstances();
                    throw e;
                }
            } else {
                terminateInstances();
            }
        }

        private synchronized void terminateInstances() throws Throwable {
            List<String> instanceIds = new ArrayList<>();
            for (CastleNode node : cluster.nodes().values()) {
                AwsNodeRole nodeRole = node.getRole(AwsNodeRole.class);
                String instanceId = nodeRole.instanceId();
                if (!instanceId.isEmpty()) {
                    instanceIds.add(instanceId);
                }
            }
            if (!instanceIds.isEmpty()) {
                cluster.cloud().terminateInstances(instanceIds.toArray(new String[0]));
                cluster.clusterLog().info("*** Terminated instance IDs " +
                    Utils.join(instanceIds, ", "));
                for (CastleNode node : cluster.nodes().values()) {
                    AwsNodeRole nodeRole = node.getRole(AwsNodeRole.class);
                    if ((nodeRole != null) && instanceIds.contains(nodeRole.instanceId())) {
                        node.log().info("*** Terminated instance " + nodeRole.instanceId());
                    }
                }
            }
        }
    }
}
