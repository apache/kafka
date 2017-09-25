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
 * Represents a filter which matches access control entries.
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
 */
@InterfaceStability.Evolving
public class AccessControlEntryFilter {
    private final AccessControlEntryData data;

    /**
     * Matches any access control entry.
     */
    public static final AccessControlEntryFilter ANY =
        new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY);

    /**
     * Create an instance of an access control entry filter with the provided parameters.
     *
     * @param principal the principal or null
     * @param host the host or null
     * @param operation non-null operation
     * @param permissionType non-null permission type
     */
    public AccessControlEntryFilter(String principal, String host, AclOperation operation, AclPermissionType permissionType) {
        Objects.requireNonNull(operation);
        Objects.requireNonNull(permissionType);
        this.data = new AccessControlEntryData(principal, host, operation, permissionType);
    }

    /**
     * This is a non-public constructor used in AccessControlEntry#toFilter
     *
     * @param data     The access control data.
     */
    AccessControlEntryFilter(AccessControlEntryData data) {
        this.data = data;
    }

    /**
     * Return the principal or null.
     */
    public String principal() {
        return data.principal();
    }

    /**
     * Return the host or null. The value `*` means any host.
     */
    public String host() {
        return data.host();
    }

    /**
     * Return the AclOperation.
     */
    public AclOperation operation() {
        return data.operation();
    }

    /**
     * Return the AclPermissionType.
     */
    public AclPermissionType permissionType() {
        return data.permissionType();
    }

    @Override
    public String toString() {
        return data.toString();
    }

    /**
     * Return true if there are any UNKNOWN components.
     */
    public boolean isUnknown() {
        return data.isUnknown();
    }

    /**
     * Returns true if this filter matches the given AccessControlEntry.
     */
    public boolean matches(AccessControlEntry other) {
        if ((principal() != null) && (!data.principal().equals(other.principal())))
            return false;
        if ((host() != null) && (!host().equals(other.host())))
            return false;
        if ((operation() != AclOperation.ANY) && (!operation().equals(other.operation())))
            return false;
        if ((permissionType() != AclPermissionType.ANY) && (!permissionType().equals(other.permissionType())))
            return false;
        return true;
    }

    /**
     * Returns true if this filter could only match one ACE -- in other words, if
     * there are no ANY or UNKNOWN fields.
     */
    public boolean matchesAtMostOne() {
        return findIndefiniteField() == null;
    }

    /**
     * Returns a string describing an ANY or UNKNOWN field, or null if there is
     * no such field.
     */
    public String findIndefiniteField() {
        return data.findIndefiniteField();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AccessControlEntryFilter))
            return false;
        AccessControlEntryFilter other = (AccessControlEntryFilter) o;
        return data.equals(other.data);
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }
}
