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
 * Represents an access control entry.  ACEs are a tuple of principal, host,
 * operation, and permissionType.
 */
public class AccessControlEntry {
    final AccessControlEntryData data;

    public AccessControlEntry(String principal, String host, AclOperation operation, AclPermissionType permissionType) {
        Objects.requireNonNull(principal);
        Objects.requireNonNull(host);
        Objects.requireNonNull(operation);
        assert operation != AclOperation.ANY;
        Objects.requireNonNull(permissionType);
        assert permissionType != AclPermissionType.ANY;
        this.data = new AccessControlEntryData(principal, host, operation, permissionType);
    }

    public String principal() {
        return data.principal();
    }

    public String host() {
        return data.host();
    }

    public AclOperation operation() {
        return data.operation();
    }

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
    public boolean unknown() {
        return data.unknown();
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
