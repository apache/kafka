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
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.AuthorizerNotReadyException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;


/**
 * A class which encapsulates the configuration and the ACL data owned by StandardAuthorizer.
 *
 * The class is not thread-safe.
 */
public class StandardAuthorizerData {
    /**
     * The host or name string used in ACLs that match any host or name.
     */
    public static final String WILDCARD = "*";

    /**
     * The principal entry used in ACLs that match any principal.
     */
    public static final String WILDCARD_PRINCIPAL = "User:*";
    public static final KafkaPrincipal WILDCARD_KAFKA_PRINCIPAL = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*");

    /**
     * The logger to use.
     */
    final Logger log;

    /**
     * Logger to use for auditing.
     */
    final Logger auditLog;

    /**
     * The current AclMutator.
     */
    final AclMutator aclMutator;

    /**
     * True if the authorizer loading process is complete.
     */
    final boolean loadingComplete;

    /**
     * A statically configured set of users that are authorized to do anything.
     */
    private final Set<String> superUsers;

    /**
     * The result to return if no ACLs match.
     */
    private final DefaultRule noAclRule;

    /**
     * Contains all of the current ACLs sorted by (resource type, resource name).
     */
    private final TreeSet<StandardAcl> aclsByResource;

    /**
     * Contains all of the current ACLs indexed by UUID.
     */
    private final HashMap<Uuid, StandardAcl> aclsById;

    private static Logger createLogger(int nodeId) {
        return new LogContext("[StandardAuthorizer " + nodeId + "] ").logger(StandardAuthorizerData.class);
    }

    private static Logger auditLogger() {
        return LoggerFactory.getLogger("kafka.authorizer.logger");
    }

    static StandardAuthorizerData createEmpty() {
        return new StandardAuthorizerData(createLogger(-1),
            null,
            false,
            Collections.emptySet(),
            DENIED,
            new TreeSet<>(), new HashMap<>());
    }

    private StandardAuthorizerData(Logger log,
                                   AclMutator aclMutator,
                                   boolean loadingComplete,
                                   Set<String> superUsers,
                                   AuthorizationResult defaultResult,
                                   TreeSet<StandardAcl> aclsByResource,
                                   HashMap<Uuid, StandardAcl> aclsById) {
        this.log = log;
        this.auditLog = auditLogger();
        this.aclMutator = aclMutator;
        this.loadingComplete = loadingComplete;
        this.superUsers = superUsers;
        this.noAclRule = new DefaultRule(defaultResult);
        this.aclsByResource = aclsByResource;
        this.aclsById = aclsById;
    }

    StandardAuthorizerData copyWithNewAclMutator(AclMutator newAclMutator) {
        return new StandardAuthorizerData(
            log,
            newAclMutator,
            loadingComplete,
            superUsers,
            noAclRule.result,
            aclsByResource,
            aclsById);
    }

    StandardAuthorizerData copyWithNewLoadingComplete(boolean newLoadingComplete) {
        return new StandardAuthorizerData(log,
            aclMutator,
            newLoadingComplete,
            superUsers,
            noAclRule.result,
            aclsByResource,
            aclsById);
    }

    StandardAuthorizerData copyWithNewConfig(int nodeId,
                                             Set<String> newSuperUsers,
                                             AuthorizationResult newDefaultResult) {
        return new StandardAuthorizerData(
            createLogger(nodeId),
            aclMutator,
            loadingComplete,
            newSuperUsers,
            newDefaultResult,
            aclsByResource,
            aclsById);
    }

    StandardAuthorizerData copyWithNewAcls(TreeSet<StandardAcl> aclsByResource, HashMap<Uuid,
        StandardAcl> aclsById) {
        StandardAuthorizerData newData =  new StandardAuthorizerData(
            log,
            aclMutator,
            loadingComplete,
            superUsers,
            noAclRule.result,
            aclsByResource,
            aclsById);
        log.info("Initialized with {} acl(s).", aclsById.size());
        return newData;
    }

    void addAcl(Uuid id, StandardAcl acl) {
        try {
            StandardAcl prevAcl = aclsById.putIfAbsent(id, acl);
            if (prevAcl != null) {
                throw new RuntimeException("An ACL with ID " + id + " already exists.");
            }
            if (!aclsByResource.add(acl)) {
                aclsById.remove(id);
                throw new RuntimeException("Unable to add the ACL with ID " + id +
                    " to aclsByResource");
            }
            log.trace("Added ACL {}: {}", id, acl);
        } catch (Throwable e) {
            log.error("addAcl error", e);
            throw e;
        }
    }

    void removeAcl(Uuid id) {
        try {
            StandardAcl acl = aclsById.remove(id);
            if (acl == null) {
                throw new RuntimeException("ID " + id + " not found in aclsById.");
            }
            if (!aclsByResource.remove(acl)) {
                throw new RuntimeException("Unable to remove the ACL with ID " + id +
                    " from aclsByResource");
            }
            log.trace("Removed ACL {}: {}", id, acl);
        } catch (Throwable e) {
            log.error("removeAcl error", e);
            throw e;
        }
    }

    Set<String> superUsers() {
        return superUsers;
    }

    AuthorizationResult defaultResult() {
        return noAclRule.result;
    }

    int aclCount() {
        return aclsById.size();
    }

    /**
     * Authorize an action based on the current set of ACLs.
     *
     * In order to know whether to allow or deny the action, we need to examine the ACLs
     * that apply to it. If any DENY ACLs match, the operation is denied, no matter how
     * many ALLOW ACLs match. If neither ALLOW nor DENY ACLs match, we return the default
     * result. In general it makes more sense to configure the default result to be
     * DENY, but some people (and unit tests) configure it as ALLOW.
     */
    public AuthorizationResult authorize(
        AuthorizableRequestContext requestContext,
        Action action
    ) {
        KafkaPrincipal principal = baseKafkaPrincipal(requestContext);
        final MatchingRule rule;

        // Superusers are authorized to do anything.
        if (superUsers.contains(principal.toString())) {
            rule = SuperUserRule.INSTANCE;
        } else if (!loadingComplete) {
            throw new AuthorizerNotReadyException();
        } else {
            rule = findAclRule(
                matchingPrincipals(requestContext),
                requestContext.clientAddress().getHostAddress(),
                action
            );
        }
        logAuditMessage(principal, requestContext, action, rule);
        return rule.result();
    }

    private String buildAuditMessage(
        KafkaPrincipal principal,
        AuthorizableRequestContext context,
        Action action,
        MatchingRule rule
    ) {
        StringBuilder bldr = new StringBuilder();
        bldr.append("Principal = ").append(principal);
        bldr.append(" is ").append(rule.result() == ALLOWED ? "Allowed" : "Denied");
        bldr.append(" operation = ").append(action.operation());
        bldr.append(" from host = ").append(context.clientAddress().getHostAddress());
        bldr.append(" on resource = ");
        appendResourcePattern(action.resourcePattern(), bldr);
        bldr.append(" for request = ").append(ApiKeys.forId(context.requestType()).name);
        bldr.append(" with resourceRefCount = ").append(action.resourceReferenceCount());
        bldr.append(" based on rule ").append(rule);
        return bldr.toString();
    }

    private void appendResourcePattern(ResourcePattern resourcePattern, StringBuilder bldr) {
        bldr.append(SecurityUtils.resourceTypeName(resourcePattern.resourceType()))
            .append(":")
            .append(resourcePattern.patternType())
            .append(":")
            .append(resourcePattern.name());
    }

    private void logAuditMessage(
        KafkaPrincipal principal,
        AuthorizableRequestContext requestContext,
        Action action,
        MatchingRule rule
    ) {
        switch (rule.result()) {
            case ALLOWED:
                // logIfAllowed is true if access is granted to the resource as a result of this authorization.
                // In this case, log at debug level. If false, no access is actually granted, the result is used
                // only to determine authorized operations. So log only at trace level.
                if (action.logIfAllowed() && auditLog.isDebugEnabled()) {
                    auditLog.debug(buildAuditMessage(principal, requestContext, action, rule));
                } else if (auditLog.isTraceEnabled()) {
                    auditLog.trace(buildAuditMessage(principal, requestContext, action, rule));
                }
                return;

            case DENIED:
                // logIfDenied is true if access to the resource was explicitly requested. Since this is an attempt
                // to access unauthorized resources, log at info level. If false, this is either a request to determine
                // authorized operations or a filter (e.g for regex subscriptions) to filter out authorized resources.
                // In this case, log only at trace level.
                if (action.logIfDenied()) {
                    auditLog.info(buildAuditMessage(principal, requestContext, action, rule));
                } else if (auditLog.isTraceEnabled()) {
                    auditLog.trace(buildAuditMessage(principal, requestContext, action, rule));
                }
        }
    }

    private MatchingRule findAclRule(
        Set<KafkaPrincipal> matchingPrincipals,
        String host,
        Action action
    ) {
        // This code relies on the ordering of StandardAcl within the NavigableMap.
        // Entries are sorted by resource type first, then REVERSE resource name.
        // Therefore, we can find all the applicable ACLs by starting at
        // (resource_type, resource_name) and stepping forwards until we reach
        // an ACL with a resource name which is not a prefix of the current one.
        // At that point, we need to search for if there are any more ACLs at
        // the first divergence point.
        //
        // For example, when trying to authorize a TOPIC resource named foobar, we would
        // start at element 2, and continue on to 3 and 4 following map:
        //
        // 1. rs=TOPIC rn=gar pt=PREFIX
        // 2. rs=TOPIC rn=foobar pt=PREFIX
        // 3. rs=TOPIC rn=foob pt=LITERAL
        // 4. rs=TOPIC rn=foo pt=PREFIX
        // 5. rs=TOPIC rn=fb pt=PREFIX
        // 6. rs=TOPIC rn=fa pt=PREFIX
        // 7. rs=TOPIC rn=f  pt=PREFIX
        // 8. rs=TOPIC rn=eeee pt=LITERAL
        //
        // Once we reached element 5, we would jump to element 7.
        MatchingRuleBuilder matchingRuleBuilder = new MatchingRuleBuilder(noAclRule);
        StandardAcl exemplar = new StandardAcl(
            action.resourcePattern().resourceType(),
            action.resourcePattern().name(),
            PatternType.UNKNOWN, // Note that the UNKNOWN value sorts before all others.
            "",
            "",
            AclOperation.UNKNOWN,
            AclPermissionType.UNKNOWN);
        checkSection(action, exemplar, matchingPrincipals, host, matchingRuleBuilder);
        if (matchingRuleBuilder.foundDeny()) {
            return matchingRuleBuilder.build();
        }

        // In addition to ACLs for this specific resource name, there can also be wildcard
        // ACLs that match any resource name. These are stored as type = LITERAL,
        // name = "*". We search these next.
        exemplar = new StandardAcl(
            action.resourcePattern().resourceType(),
            WILDCARD,
            LITERAL,
            "",
            "",
            AclOperation.UNKNOWN,
            AclPermissionType.UNKNOWN);
        checkSection(action, exemplar, matchingPrincipals, host, matchingRuleBuilder);
        return matchingRuleBuilder.build();
    }

    static int matchesUpTo(
        String resource,
        String pattern
    ) {
        int i = 0;
        while (true) {
            if (resource.length() == i) break;
            if (pattern.length() == i) break;
            if (resource.charAt(i) != pattern.charAt(i)) break;
            i++;
        }
        return i;
    }

    private void checkSection(
        Action action,
        StandardAcl exemplar,
        Set<KafkaPrincipal> matchingPrincipals,
        String host,
        MatchingRuleBuilder matchingRuleBuilder
    ) {
        String resourceName = action.resourcePattern().name();
        NavigableSet<StandardAcl> tailSet = aclsByResource.tailSet(exemplar, true);
        Iterator<StandardAcl> iterator = tailSet.iterator();
        while (iterator.hasNext()) {
            StandardAcl acl = iterator.next();
            if (!acl.resourceType().equals(action.resourcePattern().resourceType())) {
                // We've stepped outside the section for the resource type we care about and
                // should stop scanning.
                break;
            }
            int matchesUpTo = matchesUpTo(resourceName, acl.resourceName());
            if (matchesUpTo == acl.resourceName().length()) {
                if (acl.patternType() == LITERAL && matchesUpTo != resourceName.length()) {
                    // This is a literal ACL whose name is a prefix of the resource name, but
                    // which doesn't match it exactly. We should skip over this ACL, but keep
                    // scanning in case there are any relevant PREFIX ACLs.
                    continue;
                }

            } else if (!(acl.resourceName().equals(WILDCARD) && acl.patternType() == LITERAL)) {
                // If the ACL resource name is NOT a prefix of the current resource name,
                // and we're not dealing with the special case of a wildcard ACL, we've
                // stepped outside of the section we care about. Scan for any other potential
                // prefix matches.
                exemplar = new StandardAcl(exemplar.resourceType(),
                    exemplar.resourceName().substring(0, matchesUpTo),
                    exemplar.patternType(),
                    exemplar.principal(),
                    exemplar.host(),
                    exemplar.operation(),
                    exemplar.permissionType());
                tailSet = aclsByResource.tailSet(exemplar, true);
                iterator = tailSet.iterator();
                continue;
            }
            matchingRuleBuilder.hasResourceAcls = true;
            AuthorizationResult result = findResult(action, matchingPrincipals, host, acl);
            if (ALLOWED == result) {
                matchingRuleBuilder.allowAcl = acl;
            } else if (DENIED == result) {
                matchingRuleBuilder.denyAcl = acl;
                return;
            }
        }
    }

    /**
     * The set of operations which imply DESCRIBE permission, when used in an ALLOW acl.
     */
    private static final Set<AclOperation> IMPLIES_DESCRIBE = Collections.unmodifiableSet(
        EnumSet.of(DESCRIBE, READ, WRITE, DELETE, ALTER));

    /**
     * The set of operations which imply DESCRIBE_CONFIGS permission, when used in an ALLOW acl.
     */
    private static final Set<AclOperation> IMPLIES_DESCRIBE_CONFIGS = Collections.unmodifiableSet(
        EnumSet.of(DESCRIBE_CONFIGS, ALTER_CONFIGS));

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
     * context should be. Note that this function assumes that the resource name matches;
     * the resource name is not checked here.
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
        // of DESCRIBE, even if it isn't explictly stated. A similar rule applies to
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
     * Creates a consistent Iterable on read-only copy of AclBindings data for the given filter.
     *
     * @param filter The filter constraining the AclBindings to be present in the Iterable.
     * @return Iterable over AclBindings matching the filter.
     */
    Iterable<AclBinding> acls(AclBindingFilter filter) {
        List<AclBinding> aclBindingList = new ArrayList<>();
        aclsByResource.forEach(acl -> {
            AclBinding aclBinding = acl.toBinding();
            if (filter.matches(aclBinding)) {
                aclBindingList.add(aclBinding);
            }
        });
        return aclBindingList;
    }

    private interface MatchingRule {
        AuthorizationResult result();
    }

    private static class SuperUserRule implements MatchingRule {
        private static final SuperUserRule INSTANCE = new SuperUserRule();

        @Override
        public AuthorizationResult result() {
            return ALLOWED;
        }

        @Override
        public String toString() {
            return "SuperUser";
        }
    }

    private static class DefaultRule implements MatchingRule {
        private final AuthorizationResult result;

        private DefaultRule(AuthorizationResult result) {
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

    private static class MatchingAclRule implements MatchingRule {
        private final StandardAcl acl;
        private final AuthorizationResult result;

        private MatchingAclRule(StandardAcl acl, AuthorizationResult result) {
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

    private static class MatchingRuleBuilder {
        private static final DefaultRule DENY_RULE = new DefaultRule(DENIED);
        private final DefaultRule noAclRule;
        private StandardAcl denyAcl;
        private StandardAcl allowAcl;
        private boolean hasResourceAcls;

        public MatchingRuleBuilder(DefaultRule noAclRule) {
            this.noAclRule = noAclRule;
        }

        boolean foundDeny() {
            return denyAcl != null;
        }

        MatchingRule build() {
            if (denyAcl != null) {
                return new MatchingAclRule(denyAcl, DENIED);
            } else if (allowAcl != null) {
                return new MatchingAclRule(allowAcl, ALLOWED);
            } else if (!hasResourceAcls) {
                return noAclRule;
            } else {
                return DENY_RULE;
            }
        }
    }

    TreeSet<StandardAcl> getAclsByResource() {
        return aclsByResource;
    }

    HashMap<Uuid, StandardAcl> getAclsById() {
        return aclsById;
    }
}