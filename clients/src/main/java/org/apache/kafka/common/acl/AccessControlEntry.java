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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;

/**
 * Represents an access control entry.  ACEs are a tuple of principal, host, operation, and permissionType.
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
 */
@InterfaceStability.Evolving
public class AccessControlEntry {
    final AccessControlEntryData data;

    /**
     * Create an instance of an access control entry with the provided parameters.
     *
     * @param principal non-null principal
     * @param host non-null host
     * @param operation non-null operation, ANY is not an allowed operation
     * @param permissionType non-null permission type, ANY is not an allowed type
     */
    public AccessControlEntry(String principal, String host, AclOperation operation, AclPermissionType permissionType) {
        Objects.requireNonNull(principal);
        Objects.requireNonNull(host);
        Objects.requireNonNull(operation);
        if (operation == AclOperation.ANY)
            throw new IllegalArgumentException("operation must not be ANY");
        Objects.requireNonNull(permissionType);
        if (permissionType == AclPermissionType.ANY)
            throw new IllegalArgumentException("permissionType must not be ANY");
        this.data = new AccessControlEntryData(principal, host, operation, permissionType);
    }

    /**
     * Return the principal for this entry.
     */
    public String principal() {
        return data.principal();
    }

    /**
     * Return the host or `*` for all hosts.
     */
    public String host() {
        return data.host();
    }

    /**
     * Return the AclOperation. This method will never return AclOperation.ANY.
     */
    public AclOperation operation() {
        return data.operation();
    }

    /**
     * Return the AclPermissionType. This method will never return AclPermissionType.ANY.
     */
    public AclPermissionType permissionType() {
        return data.permissionType();
    }

    /**
     * Create a filter which matches only this AccessControlEntry.
     */
    public AccessControlEntryFilter toFilter() {
        return new AccessControlEntryFilter(data);
    }

    @Override
    public String toString() {
        return data.toString();
    }

    /**
     * Return true if this AclResource has any UNKNOWN components.
     */
    public boolean isUnknown() {
        return data.isUnknown();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AccessControlEntry))
            return false;
        AccessControlEntry other = (AccessControlEntry) o;
        return data.equals(other.data);
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }
}
