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

import java.util.Objects;
import java.util.Set;


/**
 * A Kafka ACLs which is identified by a UUID and stored in the metadata log.
 */
final public class StandardAcl implements Comparable<StandardAcl> {
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

    private static enum EnumPrincipalType {
        NOT_DEFINED,
        USER,
        REGEX,
        STARTSWITH,
        ENDSWITH,
        CONTAINS
    };

    private final ResourceType resourceType;
    private final String resourceName;
    private final PatternType patternType;
    private final String principal;
    private final String host;
    private final AclOperation operation;
    private final AclPermissionType permissionType;
    private final String principalType;
    private final String principalName;
    private final EnumPrincipalType ePrincipalType;

    public StandardAcl(
                ResourceType resourceType,
                String resourceName,
                PatternType patternType,
                String principal,
                String host,
                AclOperation operation,
                AclPermissionType permissionType) {
        this.resourceType = resourceType;
        this.resourceName = resourceName;
        this.patternType = patternType;
        this.principal = principal;
        this.host = host;
        this.operation = operation;
        this.permissionType = permissionType;

        int colonIndex = principal.indexOf(":");
        if (colonIndex == -1) {
            this.principalType = EnumPrincipalType.NOT_DEFINED.toString();
            this.principalName = "Principal_Is_Empty_On_Init";
        } else {
            this.principalType = principal.substring(0, colonIndex);
            this.principalName = principal.substring(colonIndex + 1);
        }

        if (0 == this.principalType.compareTo("User")) {
            this.ePrincipalType = EnumPrincipalType.USER;
        } else if (0 == this.principalType.compareTo("Regex")) {
            this.ePrincipalType = EnumPrincipalType.REGEX;
        } else if (0 == this.principalType.compareTo("StartsWith")) {
            this.ePrincipalType = EnumPrincipalType.STARTSWITH;
        } else if (0 == this.principalType.compareTo("EndsWith")) {
            this.ePrincipalType = EnumPrincipalType.ENDSWITH;
        } else if (0 == this.principalType.compareTo("Contains")) {
            this.ePrincipalType = EnumPrincipalType.CONTAINS;
        } else {
            this.ePrincipalType = EnumPrincipalType.NOT_DEFINED;
        }
    }

    public boolean matchAtLeastOnePrincipal(Set<KafkaPrincipal> principalSet) {
        boolean result = false;
        switch (this.ePrincipalType) {
            case NOT_DEFINED:
                result = false;
                break;
            case USER:
                result = principalSet.contains(this.kafkaPrincipal());
                break;
            case REGEX:
                for (KafkaPrincipal kP : principalSet) {
                    if (kP.getName().matches(this.principalName)) {
                        result = true;
                        break;
                    }
                }
                break;
            case STARTSWITH:
                for (KafkaPrincipal kP : principalSet) {
                    if (kP.getName().startsWith(this.principalName)) {
                        result = true;
                        break;
                    }
                }
                break;
            case ENDSWITH:
                for (KafkaPrincipal kP : principalSet) {
                    if (kP.getName().endsWith(this.principalName)) {
                        result = true;
                        break;
                    }
                }
                break;
            case CONTAINS:
                for (KafkaPrincipal kP : principalSet) {
                    if (kP.getName().contains(this.principalName)) {
                        result = true;
                        break;
                    }
                }
                break;
            default:
                result = false;
                break;
        }
        return result;
    }

    public ResourceType resourceType() {
        return resourceType;
    }

    public String resourceName() {
        return resourceName;
    }

    public PatternType patternType() {
        return patternType;
    }

    public String principal() {
        return principal;
    }

    public KafkaPrincipal kafkaPrincipal() {
        return new KafkaPrincipal(this.principalType, this.principalName);
    }

    public String host() {
        return host;
    }

    public AclOperation operation() {
        return operation;
    }

    public AclPermissionType permissionType() {
        return permissionType;
    }

    public AclBinding toBinding() {
        ResourcePattern resourcePattern =
            new ResourcePattern(resourceType, resourceName, patternType);
        AccessControlEntry accessControlEntry =
            new AccessControlEntry(principal, host, operation, permissionType);
        return new AclBinding(resourcePattern, accessControlEntry);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(StandardAcl.class)) return false;
        if (o == this) return true;
        StandardAcl other = (StandardAcl) o;
        return resourceType.equals(other.resourceType) &&
            resourceName.equals(other.resourceName) &&
            patternType.equals(other.patternType) &&
            principal.equals(other.principal) &&
            host.equals(other.host) &&
            operation.equals(other.operation) &&
            permissionType.equals(other.permissionType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            resourceType,
            resourceName,
            patternType,
            principal,
            host,
            operation,
            permissionType);
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

    @Override
    public String toString() {
        return "StandardAcl(" +
            "resourceType=" + resourceType +
            ", resourceName=" + resourceName +
            ", patternType=" + patternType +
            ", principal=" + principal +
            ", host=" + host +
            ", operation=" + operation +
            ", permissionType=" + permissionType +
            ")";
    }
}
