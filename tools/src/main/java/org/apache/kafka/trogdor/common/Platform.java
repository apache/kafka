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

package org.apache.kafka.trogdor.common;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.File;
import java.io.IOException;

import org.apache.kafka.common.utils.Scheduler;
import org.apache.kafka.common.utils.Utils;

/**
 * Defines a cluster topology
 */
public interface Platform {
    class Config {
        public static final String TROGDOR_AGENT_PORT = "trogdor.agent.port";

        public static final String TROGDOR_COORDINATOR_PORT = "trogdor.coordinator.port";

        public static final String TROGDOR_COORDINATOR_HEARTBEAT_MS =
            "trogdor.coordinator.heartbeat.ms";

        public static final int TROGDOR_COORDINATOR_HEARTBEAT_MS_DEFAULT = 60000;

        public static Platform parse(String curNodeName, String path) throws Exception {
            JsonNode root = JsonUtil.JSON_SERDE.readTree(new File(path));
            JsonNode platformNode = root.get("platform");
            if (platformNode == null) {
                throw new RuntimeException("Expected to find a 'platform' field " +
                    "in the root JSON configuration object");
            }
            String platformName = platformNode.textValue();
            return Utils.newParameterizedInstance(platformName,
                String.class, curNodeName,
                Scheduler.class, Scheduler.SYSTEM,
                JsonNode.class, root);
        }
    }

    /**
     * Get name for this platform.
     */
    String name();

    /**
     * Get the current node.
     */
    Node curNode();

    /**
     * Get the cluster topology.
     */
    Topology topology();

    /**
     * Get the scheduler to use.
     */
    Scheduler scheduler();

    /**
     * Run a command on this local node.
     *
     * Throws an exception if the command could not be run, or if the
     * command returned a non-zero error status.
     *
     * @param command   The command
     *
     * @return          The command output.
     */
    String runCommand(String[] command) throws IOException;
}
