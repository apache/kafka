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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.castle.role.Role;
import org.apache.kafka.castle.tool.CastleTool;
import org.apache.kafka.trogdor.common.StringExpander;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CastleClusterSpec {
    private final Map<String, CastleNodeSpec> nodes;
    private final Map<String, Role> roles;

    @JsonCreator
    public CastleClusterSpec(@JsonProperty("nodes") Map<String, CastleNodeSpec> nodes,
                           @JsonProperty("roles") Map<String, Role> roles) throws Exception {
        if (nodes == null) {
            this.nodes = Collections.emptyMap();
        } else {
            Map<String, CastleNodeSpec> newNodes = new HashMap<>();
            for (Map.Entry<String, CastleNodeSpec> entry: nodes.entrySet()) {
                for (String nodeName : StringExpander.expand(entry.getKey())) {
                    CastleNodeSpec nodeCopy = CastleTool.JSON_SERDE.readValue(
                        CastleTool.JSON_SERDE.writeValueAsBytes(entry.getValue()),
                        CastleNodeSpec.class);
                    newNodes.put(nodeName, nodeCopy);
                }
            }
            this.nodes = Collections.unmodifiableMap(newNodes);
        }
        this.roles = Collections.unmodifiableMap(
            (roles == null) ? new HashMap<>() : new HashMap<>(roles));
    }

    @JsonProperty
    public Map<String, CastleNodeSpec> nodes() {
        return nodes;
    }

    @JsonProperty
    public Map<String, Role> roles() {
        return roles;
    }

    /**
     * Return the roles for each node in the cluster.
     *
     * Each node will get a separate copy of each role object.
     */
    public Map<String, Map<Class<? extends Role>, Role>> nodesToRoles() throws Exception {
        Map<String, Map<Class<? extends Role>, Role>> nodesToRoles = new HashMap<>();
        for (Map.Entry<String, CastleNodeSpec> entry : nodes.entrySet()) {
            String nodeName = entry.getKey();
            Map<Class<? extends Role>, Role> roleMap = new HashMap<>();
            nodesToRoles.put(nodeName, roleMap);
            for (String roleName : entry.getValue().roleNames()) {
                Role role = roles.get(roleName);
                if (role == null) {
                    throw new RuntimeException("For node " + nodeName +
                        ", no role named " + roleName + " found.  Role names are " +
                        Utils.join(entry.getValue().roleNames(), ", "));
                }
                Role roleCopy = CastleTool.JSON_SERDE.readValue(
                    CastleTool.JSON_SERDE.writeValueAsBytes(role),
                    Role.class);
                roleMap.put(role.getClass(), roleCopy);
            }
        }
        return nodesToRoles;
    }
}
