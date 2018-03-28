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

package org.apache.kafka.soak.role;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.soak.action.Action;
import org.apache.kafka.soak.action.AwsCheckAction;
import org.apache.kafka.soak.action.AwsDestroyAction;
import org.apache.kafka.soak.action.AwsInitAction;

import java.util.ArrayList;
import java.util.Collection;

public class AwsNodeRole implements Role {
    /**
     * Configures the AWS image ID to use.
     */
    private final String imageId;

    /**
     * Configures the AWS instance type to use.
     */
    private final String instanceType;

    /**
     * Configures the ssh identity file to use.
     * If this is not set, no identity file will be used.
     */
    private final String sshIdentityFile;

    /**
     * Configures the ssh username to use.
     * If this is not set, no username will be specified.
     */
    private final String sshUser;

    /**
     * Configures the ssh port to use.
     * If this is not set, the system default will be used.
     */
    private final int sshPort;

    /**
     * Configures whether to use the internal DNS when accessing this node.
     * Defaults to false.
     */
    private final boolean internal;

    /**
     * Configures the private DNS address of a node.
     * If this is not set, the node has no private DNS address yet.
     */
    private final String privateDns;

    /**
     * Configures the public DNS address of a node.
     * If this is not set, the node has no public DNS address yet.
     */
    private final String publicDns;

    /**
     * The AWS instance ID, if any.
     */
    private final String instanceId;

    public AwsNodeRole(AwsNodeRole role,
                       String privateDns,
                       String publicDns,
                       String instanceId) {
        this(role.imageId,
            role.instanceType,
            role.sshIdentityFile,
            role.sshUser,
            role.sshPort,
            role.internal,
            privateDns,
            publicDns,
            instanceId);
    }

    @JsonCreator
    public AwsNodeRole(@JsonProperty("imageId") String imageId,
                       @JsonProperty("instanceType") String instanceType,
                       @JsonProperty("sshIdentityFile") String sshIdentityFile,
                       @JsonProperty("sshUser") String sshUser,
                       @JsonProperty("sshPort") int sshPort,
                       @JsonProperty("internal") boolean internal,
                       @JsonProperty("privateDns") String privateDns,
                       @JsonProperty("publicDns") String publicDns,
                       @JsonProperty("instanceId") String instanceId) {
        this.imageId = imageId == null ? "" : imageId;
        this.instanceType = instanceType == null ? "" : instanceType;
        this.sshIdentityFile = sshIdentityFile == null ? "" : sshIdentityFile;
        this.sshUser = sshUser == null ? "" : sshUser;
        this.sshPort = sshPort;
        this.internal = internal;
        this.privateDns = privateDns == null ? "" : privateDns;
        this.publicDns = publicDns == null ? "" : publicDns;
        this.instanceId = instanceId == null ? "" : instanceId;
    }

    @JsonProperty
    public String imageId() {
        return imageId;
    }

    @JsonProperty
    public String instanceType() {
        return instanceType;
    }

    @JsonProperty
    public String sshIdentityFile() {
        return sshIdentityFile;
    }

    @JsonProperty
    public String sshUser() {
        return sshUser;
    }

    @JsonProperty
    public int sshPort() {
        return sshPort;
    }

    @JsonProperty
    public boolean internal() {
        return internal;
    }

    @JsonProperty
    public String privateDns() {
        return privateDns;
    }

    @JsonProperty
    public String publicDns() {
        return publicDns;
    }

    @JsonProperty
    public String instanceId() {
        return instanceId;
    }

    @Override
    public Collection<Action> createActions(String nodeName) {
        ArrayList<Action> actions = new ArrayList<>();
        actions.add(new AwsInitAction(nodeName, this));
        actions.add(new AwsDestroyAction(nodeName));
        actions.add(new AwsCheckAction(nodeName));
        return actions;
    }

    public String dns() {
        if (internal) {
            return privateDns;
        } else {
            return publicDns;
        }
    }
};
