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

package org.apache.kafka.metadata.authorizer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.metadata.AccessControlEntryRecord;

import java.util.Objects;


/**
 * A tuple of (id, acl)
 */
final public class StandardAclWithId {
    public static StandardAclWithId fromRecord(AccessControlEntryRecord record) {
        return new StandardAclWithId(record.id(), StandardAcl.fromRecord(record));
    }

    private final Uuid id;
    private final StandardAcl acl;

    public StandardAclWithId(Uuid id,
                             StandardAcl acl) {
        this.id = id;
        this.acl = acl;
    }

    public Uuid id() {
        return id;
    }

    public StandardAcl acl() {
        return acl;
    }

    public AccessControlEntryRecord toRecord() {
        return new AccessControlEntryRecord().
            setId(id).
            setResourceType(acl.resourceType().code()).
            setResourceName(acl.resourceName()).
            setPatternType(acl.patternType().code()).
            setPrincipal(acl.principal()).
            setHost(acl.host()).
            setOperation(acl.operation().code()).
            setPermissionType(acl.permissionType().code());
    }

    public AclBinding toBinding() {
        return acl.toBinding();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(StandardAclWithId.class)) return false;
        if (o == this) return true;
        StandardAclWithId other = (StandardAclWithId) o;
        return id.equals(other.id) &&
            acl.equals(other.acl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, acl);
    }

    @Override
    public String toString() {
        return "StandardAclWithId(" +
            "id=" + id +
            ", acl=" + acl +
            ")";
    }
}
