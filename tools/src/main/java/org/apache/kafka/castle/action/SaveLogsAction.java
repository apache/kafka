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

import org.apache.kafka.castle.cloud.RemoteCommandResultException;
import org.apache.kafka.castle.cluster.CastleCluster;
import org.apache.kafka.castle.cluster.CastleNode;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Rsync the Kafka source directory to the cluster node.
 */
public final class SaveLogsAction extends Action {
    public static final String TYPE = "saveLogs";

    public SaveLogsAction(String scope) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {
                new TargetId(DaemonStopAction.TYPE, scope)
            },
            new String[] {},
            0);
    }

    @Override
    public void call(CastleCluster cluster, CastleNode node) throws Throwable {
        if (node.dns().isEmpty()) {
            node.log().printf("*** Skipping saveLogs, because the node has no DNS address.%n");
            return;
        }
        Files.createDirectories(Paths.get(cluster.env().outputDirectory(),
            "logs", node.nodeName()));
        int lsStatus = node.cloud().remoteCommand(node).args("ls", ActionPaths.LOGS_ROOT).run();
        if (lsStatus == 0) {
            node.cloud().remoteCommand(node).
                syncFrom(ActionPaths.LOGS_ROOT + "/",
                    cluster.env().outputDirectory() + "/logs/" + node.nodeName() + "/").
                mustRun();
        } else if ((lsStatus == 1) || (lsStatus == 2)) {
            node.log().printf("*** Skipping saveLogs, because %s was not found.%n", ActionPaths.LOGS_ROOT);
        } else {
            throw new RemoteCommandResultException(
                Arrays.asList(new String[]{"ls", ActionPaths.LOGS_ROOT}), lsStatus);
        }
    }
}
