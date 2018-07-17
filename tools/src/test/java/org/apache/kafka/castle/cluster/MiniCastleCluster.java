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

package org.apache.kafka.castle.cluster;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.castle.cloud.MockRemoteCommand;
import org.apache.kafka.castle.common.NullOutputStream;
import org.apache.kafka.castle.common.CastleLog;
import org.apache.kafka.castle.role.Role;
import org.apache.kafka.castle.tool.CastleEnvironment;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MiniCastleCluster implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MiniCastleCluster.class);

    public static class Builder {
        private final Map<String, Set<String>> nodeNamesToRoles = new HashMap<>();
        private final Map<String, Role> roles = new HashMap<>();

        public Builder() {
        }

        public Builder addNodes(String... nodeNames) {
            for (String nodeName : nodeNames) {
                this.nodeNamesToRoles.putIfAbsent(nodeName, new HashSet<>());
            }
            return this;
        }

        public Builder addRole(String roleName, Role role, String... nodeNames) {
            this.roles.put(roleName, role);
            for (String nodeName : nodeNames) {
                this.nodeNamesToRoles.computeIfAbsent(nodeName, k -> new HashSet<>()).add(roleName);
            }
            return this;
        }

        public MiniCastleCluster build() throws Exception {
            CastleCluster castleCluster = null;
            File tempDirectory = TestUtils.tempDirectory();
            boolean success = false;
            try {
                Path outputPath = Paths.get(tempDirectory.getAbsolutePath(), "output");
                Files.createDirectories(outputPath);
                CastleEnvironment env = new CastleEnvironment(
                    Paths.get(tempDirectory.getAbsolutePath(), "input_cluster.json").toString(),
                    Paths.get(tempDirectory.getAbsolutePath(), "output_cluster.json").toString(),
                    360,
                    Paths.get(tempDirectory.getAbsolutePath(), "kafka").toString(),
                    outputPath.toString());

                Map<String, CastleNodeSpec> nodes = new HashMap<>();
                for (Map.Entry<String, Set<String>> entry : nodeNamesToRoles.entrySet()) {
                    String nodeName = entry.getKey();
                    List<String> roleNameList = new ArrayList<>();
                    for (String roleName : entry.getValue()) {
                        if (!roles.containsKey(roleName)) {
                            throw new RuntimeException("Failed to find a role named " + roleName);
                        }
                        roleNameList.add(roleName);
                    }
                    nodes.put(nodeName, new CastleNodeSpec(roleNameList));
                }

                castleCluster = new CastleCluster(env,
                    new CastleLog(CastleLog.CLUSTER, NullOutputStream.INSTANCE, true),
                    new CastleClusterSpec(nodes, roles));
                success = true;
            } finally {
                if (!success)  {
                    Utils.delete(tempDirectory);
                }
            }
            return new MiniCastleCluster(castleCluster, tempDirectory);
        }
    }

    private final CastleCluster cluster;

    private final File tempDirectory;

    private MiniCastleCluster(CastleCluster cluster, File tempDirectory) throws IOException {
        this.cluster = cluster;
        this.tempDirectory = tempDirectory;
    }

    public CastleCluster cluster() {
        return cluster;
    }

    public String[] rsyncToCommandLine(String nodeName, String local, String remote) {
        return MockRemoteCommand.rsyncToCommandLine(cluster.nodes().get(nodeName), local, remote);
    }

    public String[] rsyncFromCommandLine(String nodeName, String remote, String local) {
        return MockRemoteCommand.rsyncFromCommandLine(cluster.nodes().get(nodeName), remote, local);
    }

    public void close() {
        try {
            Utils.delete(tempDirectory);
        } catch (IOException e) {
            log.error("Failed to delete {}", tempDirectory.getAbsolutePath(), e);
        }
    }
}
