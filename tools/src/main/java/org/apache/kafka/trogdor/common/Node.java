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

import org.apache.kafka.trogdor.agent.Agent;
import org.apache.kafka.trogdor.coordinator.Coordinator;

import java.util.Set;

/**
 * Defines a node in a cluster topology
 */
public interface Node {
    public static class Util {
        public static int getIntConfig(Node node, String key, int defaultVal) {
            String val = node.getConfig(key);
            if (val == null) {
                return defaultVal;
            } else {
                return Integer.parseInt(val);
            }
        }

        public static int getTrogdorAgentPort(Node node) {
            return getIntConfig(node, Platform.Config.TROGDOR_AGENT_PORT, Agent.DEFAULT_PORT);
        }

        public static int getTrogdorCoordinatorPort(Node node) {
            return getIntConfig(node, Platform.Config.TROGDOR_COORDINATOR_PORT, Coordinator.DEFAULT_PORT);
        }
    }

    /**
     * Get name for this node.
     */
    String name();

    /**
     * Get hostname for this node.
     */
    String hostname();

    /**
     * Get the configuration value associated with the key, or null if there
     * is none.
     */
    String getConfig(String key);

    /**
     * Get the tags for this node.
     */
    Set<String> tags();
}
