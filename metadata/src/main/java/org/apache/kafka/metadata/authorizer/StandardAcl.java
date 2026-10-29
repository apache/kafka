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

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.metadata.AccessControlEntryRecord;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;


/**
 * A Kafka ACLs which is identified by a UUID and stored in the metadata log.
 */
public record StandardAcl(ResourceType resourceType, String resourceName, PatternType patternType, String principal,
                          String host, AclOperation operation,
                          AclPermissionType permissionType) implements Comparable<StandardAcl> {
    public static StandardAcl fromRecord(AccessControlEntryRecord record) {
        return new StandardAcl(
            ResourceType.fromCode(record.resourceType()),
            record.resourceName(),
            PatternType.fromCode(record.patternType()),
            record.principal(),
            record.host(),
            AclOperation.fromCode(record.operation()),
            AclPermissionType.fromCode(record.permissionType()));
    }

    public static StandardAcl fromAclBinding(AclBinding acl) {
        return new StandardAcl(
            acl.pattern().resourceType(),
            acl.pattern().name(),
            acl.pattern().patternType(),
            acl.entry().principal(),
            acl.entry().host(),
            acl.entry().operation(),
            acl.entry().permissionType());
    }

    public KafkaPrincipal kafkaPrincipal() {
        int colonIndex = principal.indexOf(":");
        if (colonIndex == -1) {
            throw new IllegalStateException("Could not parse principal from `" + principal + "` " +
                "(no colon is present separating the principal type from the principal name)");
        }
        String principalType = principal.substring(0, colonIndex);
        String principalName = principal.substring(colonIndex + 1);
        return new KafkaPrincipal(principalType, principalName);
    }

    public AclBinding toBinding() {
        ResourcePattern resourcePattern =
            new ResourcePattern(resourceType, resourceName, patternType);
        AccessControlEntry accessControlEntry =
            new AccessControlEntry(principal, host, operation, permissionType);
        return new AclBinding(resourcePattern, accessControlEntry);
    }

    /**
     * Compare two StandardAcl objects. See {@link StandardAuthorizerData#authorize} for an
     * explanation of why we want this particular sort order.
     */
    @Override
    public int compareTo(StandardAcl other) {
        int result;
        result = resourceType.compareTo(other.resourceType);
        if (result != 0) return result;
        result = other.resourceName.compareTo(resourceName); // REVERSE sort by resource name.
        if (result != 0) return result;
        result = patternType.compareTo(other.patternType);
        if (result != 0) return result;
        result = operation.compareTo(other.operation);
        if (result != 0) return result;
        result = principal.compareTo(other.principal);
        if (result != 0) return result;
        result = host.compareTo(other.host);
        if (result != 0) return result;
        result = permissionType.compareTo(other.permissionType);
        return result;
    }
}
