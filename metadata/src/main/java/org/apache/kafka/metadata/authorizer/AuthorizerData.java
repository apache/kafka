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
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.AuthorizerNotReadyException;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
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
 * THe interface that describes the data used by the {@link StandardAuthorizer}.  This is defiens the structure of the
 * data as stored in the Authorizer.
 */
public interface AuthorizerData {
    /**
     * The host or name string used in ACLs that match any host or name.
     */
    String WILDCARD = "*";

    /** The principal entry used in ACLs that match any principal. */
    String WILDCARD_PRINCIPAL = "User:*";

    /** The KafkaPrincipal equivalend to the {@link #WILDCARD_PRINCIPAL} */
    KafkaPrincipal WILDCARD_KAFKA_PRINCIPAL = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*");
    /** The set of operations which imply DESCRIBE permission, when used in an ALLOW acl. */
    Set<AclOperation> IMPLIES_DESCRIBE = Collections.unmodifiableSet(EnumSet.of(DESCRIBE, READ, WRITE, DELETE, ALTER));
    /** The set of operations which imply DESCRIBE_CONFIGS permission, when used in an ALLOW acl. */
    Set<AclOperation> IMPLIES_DESCRIBE_CONFIGS = Collections.unmodifiableSet(EnumSet.of(DESCRIBE_CONFIGS, ALTER_CONFIGS));

    /**
     * Finds the AuthorizationResult for arguments.
     * @param action the action to attempt.
     * @param requestContext the context for the request.
     * @param acl the ACL to search for.
     * @return
     */
    static AuthorizationResult findResult(Action action,
                                          AuthorizableRequestContext requestContext,
                                          StandardAcl acl) {
        return findResult(
            action,
            matchingPrincipals(requestContext),
            requestContext.clientAddress().getHostAddress(),
            acl
        );
    }

    static KafkaPrincipal baseKafkaPrincipal(AuthorizableRequestContext context) {
        KafkaPrincipal sessionPrincipal = context.principal();
        return sessionPrincipal.getClass().equals(KafkaPrincipal.class)
                ? sessionPrincipal
                : new KafkaPrincipal(sessionPrincipal.getPrincipalType(), sessionPrincipal.getName());
    }

    static Set<KafkaPrincipal> matchingPrincipals(AuthorizableRequestContext context) {
        KafkaPrincipal sessionPrincipal = context.principal();
        KafkaPrincipal basePrincipal = sessionPrincipal.getClass().equals(KafkaPrincipal.class)
                ? sessionPrincipal
                : new KafkaPrincipal(sessionPrincipal.getPrincipalType(), sessionPrincipal.getName());
        return Utils.mkSet(basePrincipal, WILDCARD_KAFKA_PRINCIPAL);
    }


    /**
     * Determine what the result of applying an ACL to the given action and request
     * context should be. Note that this function assumes that the resource name matches.
     * The resource name is not checked here.
     *
     * @param action             The input action.
     * @param matchingPrincipals The set of input matching principals
     * @param host               The input host.
     * @param acl                The input ACL.
     * @return                   null if the ACL does not match. The authorization result
     *                           otherwise.
     */
    static AuthorizationResult findResult(Action action,
                                          Set<KafkaPrincipal> matchingPrincipals,
                                          String host,
                                          StandardAcl acl) {
        // Check if the principal matches. If it doesn't, return no result (null).
        if (!matchingPrincipals.contains(acl.kafkaPrincipal())) {
            return null;
        }
        // Check if the host matches. If it doesn't, return no result (null).
        if (!acl.host().equals(WILDCARD) && !acl.host().equals(host)) {
            return null;
        }
        // Check if the operation field matches. Here we hit a slight complication.
        // ACLs for various operations (READ, WRITE, DELETE, ALTER), "imply" the presence
        // of DESCRIBE, even if it isn't explicitly stated. A similar rule applies to
        // DESCRIBE_CONFIGS.
        //
        // But this rule only applies to ALLOW ACLs. So for example, a DENY ACL for READ
        // on a resource does not DENY describe for that resource.
        if (acl.operation() != ALL) {
            if (acl.permissionType().equals(ALLOW)) {
                switch (action.operation()) {
                    case DESCRIBE:
                        if (!IMPLIES_DESCRIBE.contains(acl.operation())) return null;
                        break;
                    case DESCRIBE_CONFIGS:
                        if (!IMPLIES_DESCRIBE_CONFIGS.contains(acl.operation())) return null;
                        break;
                    default:
                        if (action.operation() != acl.operation()) {
                            return null;
                        }
                        break;
                }
            } else if (action.operation() != acl.operation()) {
                return null;
            }
        }

        return acl.permissionType().equals(ALLOW) ? ALLOWED : DENIED;
    }

    /**
     * Creates a copy of this AuthorizerData with the loading complete flag set.
     * @param newLoadingComplete the value to set the loading complete flag to.
     * @return A potentially new AuthorizerData with the loading complete flag set.
     */
    AuthorizerData copyWithNewLoadingComplete(boolean newLoadingComplete);

    /**
     * Creates a copy of this AuthorizerDaa with the new config.
     * @param nodeId the node ID for the copy.
     * @param newSuperUsers the list of super users.
     * @param newDefaultResult the default result if no specific authorization is found.
     * @return A potentially new AuthorizerData with the configuration set as specified.
     */
    AuthorizerData copyWithNewConfig(int nodeId,
                                             Set<String> newSuperUsers,
                                             AuthorizationResult newDefaultResult);

    /**
     * Creates a copy of this AuthorizerDaa with the new Acls.
     * @param acls A map containing Uuids ahd their associated StandardAcl.
     * @return A potentially new AuthorizerData with the internal data structure containing the Acls and Uuids.
     */
    AuthorizerData copyWithNewAcls(Map<Uuid, StandardAcl> acls);


    /**
     * Creates a copy of this AuthorizerDaa with the new AclMutator
     * @param newAclMutator The new AclMutator to use when modifying the ACLs.
     * @return A potentially new AuthorizerData with the internal data structure containing the Acls and Uuids.
     */
    AuthorizerData copyWithNewAclMutator(AclMutator newAclMutator);

    /**
     * Adds an ACL and associated Uuid to the data structure.
     * @param id the Uuid to add.
     * @param acl the StandardAcl associated with the Uuid.
     */
    void addAcl(Uuid id, StandardAcl acl);

    /**
     * Remove an ACL and associated Uuid from the data structure.
     * @param id the Uuid for the ACL to remove.
     */
    void removeAcl(Uuid id);

    /**
     * Gets the set of super users for this data structure.  This is the set of superusers provided in {@link #copyWithNewConfig(int, Set, AuthorizationResult)}
     * or as default during the construction of this authorizer.
     * @return the set of super users.
     */
    Set<String> superUsers();

    /**
     * Gets the default result as epcified in {@link #copyWithNewConfig(int, Set, AuthorizationResult)}
     * or as default during the construction of this authorizer.
     * @return the default authorization result.
     */
    AuthorizationResult defaultResult();

    /**
     * Returns the number of ACLs in this Authorizer.  May return -1 if the number is unknown.
     * @return the number of ACLs in this Authorizer.
     */
    int aclCount();

    /**
     * Gets the AclMutator.  This is the mutator provided in {@link #copyWithNewAclMutator(AclMutator)}.
     * @return the AclMutator currently registered with this AuthorizerData instance or {@code null} if
     * no mutator was set.
     */
    AclMutator aclMutator();

    /**
     * Gets the logger for this authorizor.
     * @return the logger for this authorizor.
     */
    Logger log();

    /**
     * Creates an iterable collection of AclBindings based on the ACLs in this authorizer.  Bindings that do not pass
     * the filter are not returned.
     * @param filter the Filter for the AclBindings to be returned.
     * @return the Iterable of AclBindings that pass the filter.
     */
    Iterable<AclBinding> acls(AclBindingFilter filter);

    /**
     * Attempt to authorize the action within the context.
     * <p>
     * The implementation should check to verify that there exists at least one ACL granting authorization to the operation
     * that is not blocked by a superseding DENY.
     * </p>
     * @param requestContext the Context of the request.
     * @param action the action that is requested.
     * @return The authorizationResult.  May not be null.
     * @throws IllegalArgumentException if the action resource pattern is not a literal
     * @throws AuthorizerNotReadyException if the loading complete flag is not set.
     */
    AuthorizationResult authorize(
            AuthorizableRequestContext requestContext,
            Action action
    );

    /**
     * Check if the caller is authorized to perform the given ACL operation on at least one
     * resource of the given type.  This method is only called when the noMatchRule is DENIED so we only have
     * to find any ALLOW.
     * <p>
     * The implementation should check to verify that there exists at least one ACL granting the operation that is not
     * blocked by a superseding DENY.
     * </p>
     *
     * @param principal the principal to limit the search by.
     * @param host the host to limit the search by.
     * @param operation the operation to limit the search to.
     * @param resourceType the resource type to limit the search to.
     * @return the authorization for the first matching resource.
     * @see Authorizer#authorizeByResourceType(AuthorizableRequestContext, AclOperation, ResourceType)
     */
    AuthorizationResult authorizeByResourceType(KafkaPrincipal principal, String host, AclOperation operation, ResourceType resourceType);

    /**
     * The definition of a matching rule.
     */
    @FunctionalInterface
    interface MatchingRule {
        MatchingRule DENY = () -> DENIED;

        MatchingRule ALLOW = () -> ALLOWED;

        /**
         * Get the result of the rule.
         * @return the result of the rule.
         */
        AuthorizationResult result();
    }

    /**
     * The rule for super users.
     */
    class SuperUserRule implements MatchingRule {
        /** The instance of the SuperUserRule */
        static final SuperUserRule INSTANCE = new SuperUserRule();

        private SuperUserRule() {
            // do not allow other instances.
        };

        @Override
        public AuthorizationResult result() {
            return ALLOWED;
        }

        @Override
        public String toString() {
            return "SuperUser";
        }
    }

    /**
     * The defalt rule definition.
     * @deprecated use {@link MatchingRule#DENY} or {@link MatchingRule#ALLOW}.
     */
    @Deprecated
    class DefaultRule implements MatchingRule {
        /** The default result */
        private final AuthorizationResult result;

        /**
         * Creates a default rule with the specified result.
         * @param result the default rule result.
         */
        DefaultRule(AuthorizationResult result) {
            this.result = result;
        }

        @Override
        public AuthorizationResult result() {
            return result;
        }

        @Override
        public String toString() {
            return result == ALLOWED ? "DefaultAllow" : "DefaultDeny";
        }
    }

    /**
     * A MatchingRule extracted from an ACL.
     */
    class MatchingAclRule implements MatchingRule {
        /** The ACL the rules is based on */
        private final StandardAcl acl;
        /** The result of the rule */
        private final AuthorizationResult result;

        /**
         * Constructs a matching rule.
         * @param acl the ACL the rule is based on.
         * @param result the result of the rule.
         */
        MatchingAclRule(StandardAcl acl, AuthorizationResult result) {
            this.acl = acl;
            this.result = result;
        }

        @Override
        public AuthorizationResult result() {
            return result;
        }

        @Override
        public String toString() {
            return "MatchingAcl(acl=" + acl + ")";
        }
    }

    /**
     * A builder for matching rules.
     */
    class MatchingRuleBuilder {
        /** a single instance of the deny rule
         * @deprecated use {@link MatchingRule#DENY} */
        @Deprecated
        static final MatchingRule DENY_RULE = MatchingRule.DENY;
        /** the default mathing rule if no ACL is found */
        private final MatchingRule noAclRule;
        /** set if a deny ACL was located */
        StandardAcl denyAcl;
        /** set if an allow ACL was located */
        StandardAcl allowAcl;
        /** set if resource ACLs were located */
        boolean hasResourceAcls;

        /**
         * Creates a builder with the default rule.
         * @param noAclRule the default rule if no matches were found.
         */
        public MatchingRuleBuilder(MatchingRule noAclRule) {
            this.noAclRule = noAclRule;
        }

        /**
         * True if a denyACL was found.
         * @return
         */
        boolean foundDeny() {
            return denyAcl != null;
        }

        /**
         * Build the Matching rule.
         * @return the Matching rule from the builder.
         */
        MatchingRule build() {
            if (denyAcl != null) {
                return new MatchingAclRule(denyAcl, DENIED);
            } else if (allowAcl != null) {
                return new MatchingAclRule(allowAcl, ALLOWED);
            } else if (!hasResourceAcls) {
                return noAclRule;
            } else {
                return MatchingRule.DENY;
            }
        }
    }
}
