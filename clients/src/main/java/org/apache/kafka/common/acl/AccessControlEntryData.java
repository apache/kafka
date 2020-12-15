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

package org.apache.kafka.common.acl;

import java.util.Objects;

/**
 * An internal, private class which contains the data stored in AccessControlEntry and
 * AccessControlEntryFilter objects.
 */
class AccessControlEntryData {
    private final String principal;
    private final String host;
    private final AclOperation operation;
    private final AclPermissionType permissionType;

    AccessControlEntryData(String principal, String host, AclOperation operation, AclPermissionType permissionType) {
        this.principal = principal;
        this.host = host;
        this.operation = operation;
        this.permissionType = permissionType;
    }

    String principal() {
        return principal;
    }

    String host() {
        return host;
    }

    AclOperation operation() {
        return operation;
    }

    AclPermissionType permissionType() {
        return permissionType;
    }

    /**
     * Returns a string describing an ANY or UNKNOWN field, or null if there is
     * no such field.
     */
    public String findIndefiniteField() {
        if (principal() == null)
            return "Principal is NULL";
        if (host() == null)
            return "Host is NULL";
        if (operation() == AclOperation.ANY)
            return "Operation is ANY";
        if (operation() == AclOperation.UNKNOWN)
            return "Operation is UNKNOWN";
        if (permissionType() == AclPermissionType.ANY)
            return "Permission type is ANY";
        if (permissionType() == AclPermissionType.UNKNOWN)
            return "Permission type is UNKNOWN";
        return null;
    }

    @Override
    public String toString() {
        return "(principal=" + (principal == null ? "<any>" : principal) +
               ", host=" + (host == null ? "<any>" : host) +
               ", operation=" + operation +
               ", permissionType=" + permissionType + ")";
    }

    /**
     * Return true if there are any UNKNOWN components.
     */
    boolean isUnknown() {
        return operation.isUnknown() || permissionType.isUnknown();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AccessControlEntryData))
            return false;
        AccessControlEntryData other = (AccessControlEntryData) o;
        return Objects.equals(principal, other.principal) &&
            Objects.equals(host, other.host) &&
            Objects.equals(operation, other.operation) &&
            Objects.equals(permissionType, other.permissionType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(principal, host, operation, permissionType);
    }
}
