/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.auth;

public class Acl {
    public static final String WILDCARD_HOST = "*";

    private KafkaPrincipal principal;
    private PermissionType permissionType;
    private String host;
    private Operation operation;

    public Acl(KafkaPrincipal principal, PermissionType permissionType, String host, Operation operation) {
        if (principal == null || permissionType == null || host == null || operation == null) {
            throw new IllegalArgumentException("principal, permissionType, host or operation can not be null");
        }
        this.principal = principal;
        this.permissionType = permissionType;
        this.host = host;
        this.operation = operation;
    }

    public KafkaPrincipal getPrincipal() {
        return principal;
    }

    public PermissionType getPermissionType() {
        return permissionType;
    }

    public String getHost() {
        return host;
    }

    public Operation getOperation() {
        return operation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Acl acl = (Acl) o;

        if (!principal.equals(acl.principal)) return false;
        if (permissionType != acl.permissionType) return false;
        if (!host.equals(acl.host)) return false;
        return operation == acl.operation;

    }

    @Override
    public int hashCode() {
        int result = principal.hashCode();
        result = 31 * result + permissionType.hashCode();
        result = 31 * result + host.hashCode();
        result = 31 * result + operation.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s has %s permission for operations: %s from hosts: %s", principal, permissionType.name, operation.name, host);
    }

}



