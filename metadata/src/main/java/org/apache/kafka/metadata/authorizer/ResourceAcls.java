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

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;


/**
 * A list of ACLs for a particular resource stored in the StandardAuthorizer.
 * This class is immutable.
 */
class ResourceAcls  {
    /**
     * The set of operations which imply DESCRIBE permission, when used in an ALLOW acl.
     */
    static final Set<AclOperation> IMPLIES_DESCRIBE = Collections.unmodifiableSet(
            EnumSet.of(DESCRIBE, READ, WRITE, DELETE, ALTER));

    /**
     * The set of operations which imply DESCRIBE_CONFIGS permission, when used in an ALLOW acl.
     */
    static final Set<AclOperation> IMPLIES_DESCRIBE_CONFIGS = Collections.unmodifiableSet(
            EnumSet.of(DESCRIBE_CONFIGS, ALTER_CONFIGS));

    /**
     * The empty list of resource ACLs.
     */
    static final ResourceAcls EMPTY = new ResourceAcls(new StandardAcl[0]);

    private final StandardAcl[] acls;

    private ResourceAcls(StandardAcl[] acls) {
        this.acls = acls;
    }

    ResourceAcls(List<StandardAcl> acl) {
        this.acls = acl.toArray(new StandardAcl[0]);
    }

    boolean treatedAsPrefixAcls() {
        return acls.length > 0 && acls[0].isWildcardOrPrefix();
    }

    boolean isEmpty() {
        return acls.length == 0;
    }

    /**
     * Copy these ResourceAcls with some additions and removals.
     *
     * @param changes       The changes to make.
     *
     * @return              The new ResourceAcls object.
     */
    ResourceAcls copyWithChanges(ResourceAclsChanges changes) {
        StandardAcl[] nextAcls = new StandardAcl[acls.length + changes.netSizeChange()];
        changes.apply(acls, nextAcls);
        return new ResourceAcls(nextAcls);
    }

    /**
     * Check if there are any ACLs in this list that match the given parameters. Any DENY results
     * lead to a denial. If at least one ALLOW is found, but no DENY, the result is ALLOW.
     * Otherwise, we return null (no result).
     *
     * Note that this function assumes that the resource type and name match. They are not checked here.
     *
     * @param operation          The input operation.
     * @param matchingPrincipals The set of input matching principals
     * @param host               The input host.
     * @return                   null if the ACL does not match. The authorization result
     *                           otherwise.
     */
    MatchingAclRule authorize(
        AclOperation operation,
        Set<KafkaPrincipal> matchingPrincipals,
        String host
    ) {
        MatchingAclRule rule = null;
        for (int i = 0; i < acls.length; i++) {
            StandardAcl acl = acls[i];
            AuthorizationResult result = authorize(acl, operation, matchingPrincipals, host);
            if (result == DENIED) {
                return new MatchingAclRule(acl, DENIED);
            } else if (result == ALLOWED && rule == null) {
                rule = new MatchingAclRule(acl, ALLOWED);
            }
        }
        return rule;
    }

    /**
     * Determine what the result of applying an ACL to the given action and request
     * context should be.
     *
     * Note that this function assumes that the resource type and name match. They are not checked here.
     *
     * @param acl                The input ACL.
     * @param operation          The input operation.
     * @param matchingPrincipals The set of input matching principals
     * @param host               The input host.
     * @return                   null if the ACL does not match. The authorization result
     *                           otherwise.
     */
    static AuthorizationResult authorize(
        StandardAcl acl,
        AclOperation operation,
        Set<KafkaPrincipal> matchingPrincipals,
        String host
    ) {
        // Check if the principal matches. If it doesn't, return no result (null).
        if (!matchingPrincipals.contains(acl.kafkaPrincipal())) {
            return null;
        }
        // Check if the host matches. If it doesn't, return no result (null).
        if (!acl.host().equals(StandardAuthorizerConstants.WILDCARD) && !acl.host().equals(host)) {
            return null;
        }
        // Check if the operation field matches. Here we hit a slight complication.
        // ACLs for various operations (READ, WRITE, DELETE, ALTER), "imply" the presence
        // of DESCRIBE, even if it isn't explictly stated. A similar rule applies to
        // DESCRIBE_CONFIGS.
        //
        // But this rule only applies to ALLOW ACLs. So for example, a DENY ACL for READ
        // on a resource does not DENY describe for that resource.
        if (acl.operation() != ALL) {
            if (acl.permissionType().equals(ALLOW)) {
                switch (operation) {
                    case DESCRIBE:
                        if (!IMPLIES_DESCRIBE.contains(acl.operation())) return null;
                        break;
                    case DESCRIBE_CONFIGS:
                        if (!IMPLIES_DESCRIBE_CONFIGS.contains(acl.operation())) return null;
                        break;
                    default:
                        if (operation != acl.operation()) {
                            return null;
                        }
                        break;
                }
            } else if (operation != acl.operation()) {
                return null;
            }
        }

        return acl.permissionType().equals(ALLOW) ? ALLOWED : DENIED;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(acls);
    }

    @Override
    public boolean equals(Object o) {
        if (o ==  null || !(o.getClass().equals(this.getClass()))) return false;
        ResourceAcls other = (ResourceAcls) o;
        return Arrays.equals(acls, other.acls);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("ResourceAcls[");
        String prefix = "";
        for (StandardAcl acl : acls) {
            builder.append(prefix).append(acl);
            prefix = ", ";
        }
        builder.append("]");
        return builder.toString();
    }
}
