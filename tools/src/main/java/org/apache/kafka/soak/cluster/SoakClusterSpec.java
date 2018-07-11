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

package org.apache.kafka.soak.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.role.Role;
import org.apache.kafka.soak.tool.SoakTool;
import org.apache.kafka.trogdor.common.StringExpander;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SoakClusterSpec {
    private final Map<String, SoakNodeSpec> nodes;
    private final Map<String, Role> roles;

    @JsonCreator
    public SoakClusterSpec(@JsonProperty("nodes") Map<String, SoakNodeSpec> nodes,
                           @JsonProperty("roles") Map<String, Role> roles) throws Exception {
        if (nodes == null) {
            this.nodes = Collections.emptyMap();
        } else {
            Map<String, SoakNodeSpec> newNodes = new HashMap<>();
            for (Map.Entry<String, SoakNodeSpec> entry: nodes.entrySet()) {
                for (String nodeName : StringExpander.expand(entry.getKey())) {
                    SoakNodeSpec nodeCopy = SoakTool.JSON_SERDE.readValue(
                        SoakTool.JSON_SERDE.writeValueAsBytes(entry.getValue()),
                        SoakNodeSpec.class);
                    newNodes.put(nodeName, nodeCopy);
                }
            }
            this.nodes = Collections.unmodifiableMap(newNodes);
        }
        this.roles = Collections.unmodifiableMap(
            (roles == null) ? new HashMap<>() : new HashMap<>(roles));
    }

    @JsonProperty
    public Map<String, SoakNodeSpec> nodes() {
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
        for (Map.Entry<String, SoakNodeSpec> entry : nodes.entrySet()) {
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
                Role roleCopy = SoakTool.JSON_SERDE.readValue(
                    SoakTool.JSON_SERDE.writeValueAsBytes(role),
                    Role.class);
                roleMap.put(role.getClass(), roleCopy);
            }
        }
        return nodesToRoles;
    }
}
