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

import org.apache.kafka.castle.cloud.Cloud;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.castle.common.CastleLog;
import org.apache.kafka.castle.role.AwsNodeRole;
import org.apache.kafka.castle.role.Role;

import java.util.Collections;
import java.util.Map;

/**
 * Represents a node in the castle cluster.
 */
public final class CastleNode implements AutoCloseable {
    /**
     * The index of this node in the cluster.  The node with the alphabetically
     * first name will be index 0, and so on.
     */
    private final int nodeIndex;

    /**
     * The castle cluster node name.
     */
    private final String nodeName;

    /**
     * The log for this node.
     */
    private final CastleLog castleLog;

    /**
     * The roles supported by this node.
     */
    private final Map<Class<? extends Role>, Role> roles;

    /**
     * The cloud associated with this node.
     */
    private final Cloud cloud;

    CastleNode(int nodeIndex, String nodeName, CastleLog castleLog,
                Map<Class<? extends Role>, Role> roles, Cloud cloud) {
        this.nodeIndex = nodeIndex;
        this.nodeName = nodeName;
        this.castleLog = castleLog;
        this.roles = Collections.unmodifiableMap(roles);
        this.cloud = cloud;
    }

    public int nodeIndex() {
        return nodeIndex;
    }

    public String nodeName() {
        return nodeName;
    }

    public CastleLog log() {
        return castleLog;
    }

    public <R extends Role> R getRole(Class<? extends Role> clazz) {
        Role role = roles.get(clazz);
        if (role == null) {
            return null;
        }
        return (R) role;
    }

    public String dns() {
        AwsNodeRole role = getRole(AwsNodeRole.class);
        if (role == null) {
            return "";
        }
        return role.dns();
    }

    public String privateDns() {
        AwsNodeRole role = getRole(AwsNodeRole.class);
        if (role == null) {
            return "";
        }
        return role.privateDns();
    }

    public String publicDns() {
        AwsNodeRole role = getRole(AwsNodeRole.class);
        if (role == null) {
            return "";
        }
        return role.publicDns();
    }

    public String sshIdentityFile() {
        AwsNodeRole role = getRole(AwsNodeRole.class);
        if (role == null) {
            return "";
        }
        return role.sshIdentityFile();
    }

    public String sshUser() {
        AwsNodeRole role = getRole(AwsNodeRole.class);
        if (role == null) {
            return "";
        }
        return role.sshUser();
    }

    public int sshPort() {
        AwsNodeRole role = getRole(AwsNodeRole.class);
        if (role == null) {
            return 0;
        }
        return role.sshPort();
    }

    public Cloud cloud() {
        return cloud;
    }

    public Map<Class<? extends Role>, Role> roles() {
        return roles;
    }

    @Override
    public void close() {
        Utils.closeQuietly(castleLog, "castleLog for " + nodeName);
    }
};
