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
import org.apache.kafka.soak.role.AwsNodeRole;
import org.apache.kafka.soak.role.Role;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A node in the soak cluster.
 */
public class SoakNodeSpec {
    private final List<Role> roles;

    @JsonCreator
    public SoakNodeSpec(@JsonProperty("roles") List<Role> roles) {
        this.roles = Collections.unmodifiableList(
            roles == null ? new ArrayList<Role>(0) : new ArrayList<>(roles));
    }

    @JsonProperty
    public List<Role> roles() {
        return roles;
    }

    public <T extends Role> T role(Class<T> clazz) {
        for (Role role : roles) {
            if (clazz.isInstance(role)) {
                return (T) role;
            }
        }
        return null;
    }

    public SoakNodeSpec copyWithRole(Role newRole) {
        ArrayList<Role> newRoles = new ArrayList<>(roles.size() + 1);
        for (Role role : roles) {
            if (!role.getClass().equals(newRole.getClass())) {
                newRoles.add(role);
            }
        }
        newRoles.add(newRole);
        return new SoakNodeSpec(newRoles);
    }

    public String privateDns() {
        AwsNodeRole awsRole = role(AwsNodeRole.class);
        if (awsRole == null) {
            return "";
        }
        return awsRole.privateDns();
    }

    public String publicDns() {
        AwsNodeRole awsRole = role(AwsNodeRole.class);
        if (awsRole == null) {
            return "";
        }
        return awsRole.publicDns();
    }
}
