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
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.common.utils.internals.SecurityUtils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.immutable.ImmutableNavigableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NavigableSet;
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
     * Contains all of the current ACLs
     */
    private AclCache aclCache;

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
            Set.of(),
            DENIED,
            new AclCache());
    }

    private StandardAuthorizerData(Logger log,
                                   AclMutator aclMutator,
                                   boolean loadingComplete,
                                   Set<String> superUsers,
                                   AuthorizationResult defaultResult,
                                   AclCache aclCache) {
        this.log = log;
        this.auditLog = auditLogger();
        this.aclMutator = aclMutator;
        this.loadingComplete = loadingComplete;
        this.superUsers = superUsers;
        this.noAclRule = new DefaultRule(defaultResult);
        this.aclCache = aclCache;
    }

    StandardAuthorizerData copyWithNewAclMutator(AclMutator newAclMutator) {
        return new StandardAuthorizerData(
            log,
            newAclMutator,
            loadingComplete,
            superUsers,
            noAclRule.result,
            aclCache);
    }

    StandardAuthorizerData copyWithNewLoadingComplete(boolean newLoadingComplete) {
        return new StandardAuthorizerData(log,
            aclMutator,
            newLoadingComplete,
            superUsers,
            noAclRule.result,
            aclCache);
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
            aclCache);
    }

    StandardAuthorizerData copyWithNewAcls(AclCache aclCache) {
        StandardAuthorizerData newData = new StandardAuthorizerData(
            log,
            aclMutator,
            loadingComplete,
            superUsers,
            noAclRule.result,
            aclCache);
        log.info("Initialized with {} acl(s).", aclCache.count());
        return newData;
    }

    void addAcl(Uuid id, StandardAcl acl) {
        try {
            aclCache = aclCache.addAcl(id, acl);
            log.trace("Added ACL {}: {}", id, acl);
        } catch (Throwable e) {
            log.error("addAcl error", e);
            throw e;
        }
    }

    void removeAcl(Uuid id) {
        try {
            AclCache aclCacheSnapshot = aclCache.removeAcl(id);
            log.trace("Removed ACL {}: {}", id, aclCacheSnapshot.getAcl(id));
            aclCache = aclCacheSnapshot;
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
        return aclCache.count();
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
        if (action.resourcePattern().patternType() != LITERAL) {
            throw new IllegalArgumentException("Only literal resources are supported. Got: " + action.resourcePattern().patternType());
        }
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
        MatchingRuleBuilder matchingRuleBuilder = new MatchingRuleBuilder(noAclRule);
        AclCache aclCacheSnapshot = aclCache;
        ResourceType resourceType = action.resourcePattern().resourceType();
        String resourceName = action.resourcePattern().name();

        // When the default result is ALLOWED, we must scan the full ACL set first
        // to determine hasResourceAcls. If ANY ACL exists for this resource (even
        // for a different principal), the result must be DENIED if no ALLOW matches,
        // rather than falling through to the default ALLOWED rule.
        if (noAclRule.result == ALLOWED) {
            StandardAcl fullExemplar = new StandardAcl(
                resourceType, resourceName, PatternType.UNKNOWN,
                "", "", AclOperation.UNKNOWN, AclPermissionType.UNKNOWN);
            checkSection(aclCacheSnapshot.aclsByResource(), action, fullExemplar, false,
                matchingPrincipals, host, matchingRuleBuilder);
            if (matchingRuleBuilder.foundDeny()) {
                return matchingRuleBuilder.build();
            }
        }

        for (KafkaPrincipal principal : matchingPrincipals) {
            ImmutableNavigableSet<StandardAcl> principalAcls =
                aclCacheSnapshot.aclsForPrincipal(principal.toString());
            if (principalAcls.isEmpty()) continue;

            StandardAcl exemplar = new StandardAcl(
                resourceType,
                resourceName,
                PatternType.UNKNOWN,
                principal.getPrincipalType() + ":" + principal.getName(),
                "",
                AclOperation.UNKNOWN,
                AclPermissionType.UNKNOWN);
            checkSection(principalAcls, action, exemplar, true, matchingPrincipals, host, matchingRuleBuilder);
            if (matchingRuleBuilder.foundDeny()) {
                return matchingRuleBuilder.build();
            }
        }

        for (KafkaPrincipal principal : matchingPrincipals) {
            ImmutableNavigableSet<StandardAcl> principalAcls =
                aclCacheSnapshot.aclsForPrincipal(principal.toString());
            if (principalAcls.isEmpty()) continue;

            StandardAcl wildcardExemplar = new StandardAcl(
                resourceType,
                WILDCARD,
                LITERAL,
                principal.getPrincipalType() + ":" + principal.getName(),
                "",
                AclOperation.UNKNOWN,
                AclPermissionType.UNKNOWN);
            checkSection(principalAcls, action, wildcardExemplar, true, matchingPrincipals, host, matchingRuleBuilder);
            if (matchingRuleBuilder.foundDeny()) {
                return matchingRuleBuilder.build();
            }
        }

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
            NavigableSet<StandardAcl> acls, Action action,
            StandardAcl exemplar, boolean skipPrincipalCheck,
            Set<KafkaPrincipal> matchingPrincipals,
            String host,
            MatchingRuleBuilder matchingRuleBuilder
    ) {
        String resourceName = action.resourcePattern().name();
        NavigableSet<StandardAcl> tailSet = acls.tailSet(exemplar, true);
        Iterator<StandardAcl> iterator = tailSet.iterator();
        while (iterator.hasNext()) {
            StandardAcl acl = iterator.next();
            if (!acl.resourceType().equals(action.resourcePattern().resourceType())) {
                break;
            }
            int matchesUpTo = matchesUpTo(resourceName, acl.resourceName());
            if (matchesUpTo == acl.resourceName().length()) {
                if (acl.patternType() == LITERAL && matchesUpTo != resourceName.length()) {
                    continue;
                }

            } else if (!(acl.resourceName().equals(WILDCARD) && acl.patternType() == LITERAL)) {
                if (matchesUpTo == 0) {
                    continue;
                }
                String newPrefix = exemplar.resourceName().substring(0, matchesUpTo);
                if (newPrefix.equals(exemplar.resourceName())) {
                    tailSet = acls.tailSet(acl, false);
                    iterator = tailSet.iterator();
                } else {
                    exemplar = new StandardAcl(exemplar.resourceType(),
                        newPrefix,
                        exemplar.patternType(),
                        exemplar.principal(),
                        exemplar.host(),
                        exemplar.operation(),
                        exemplar.permissionType());
                    tailSet = acls.tailSet(exemplar, true);
                    iterator = tailSet.iterator();
                }
                continue;
            }
            matchingRuleBuilder.hasResourceAcls = true;
            AuthorizationResult result = findResult(action, matchingPrincipals, host, acl, skipPrincipalCheck);
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
    private static final Set<AclOperation> IMPLIES_DESCRIBE =
        Set.of(DESCRIBE, READ, WRITE, DELETE, ALTER);

    /**
     * The set of operations which imply DESCRIBE_CONFIGS permission, when used in an ALLOW acl.
     */
    private static final Set<AclOperation> IMPLIES_DESCRIBE_CONFIGS =
        Set.of(DESCRIBE_CONFIGS, ALTER_CONFIGS);

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
        return Set.of(basePrincipal, WILDCARD_KAFKA_PRINCIPAL);
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
        return findResult(action, matchingPrincipals, host, acl, false);
    }

    static AuthorizationResult findResult(Action action,
                                          Set<KafkaPrincipal> matchingPrincipals,
                                          String host,
                                          StandardAcl acl,
                                          boolean skipPrincipalCheck) {
        // Check if the principal matches. If it doesn't, return no result (null).
        // When skipPrincipalCheck is true, the caller guarantees the ACL's principal
        // is already in matchingPrincipals (e.g. via principal-indexed scan).
        if (!skipPrincipalCheck && !matchingPrincipals.contains(acl.kafkaPrincipal())) {
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
     * Creates a consistent Iterable on read-only copy of AclBindings data for the given filter.
     *
     * @param filter The filter constraining the AclBindings to be present in the Iterable.
     * @return Iterable over AclBindings matching the filter.
     */
    Iterable<AclBinding> acls(AclBindingFilter filter) {
        return aclCache.acls(filter);
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

    private record DefaultRule(AuthorizationResult result) implements MatchingRule {

        @Override
        public String toString() {
            return result == ALLOWED ? "DefaultAllow" : "DefaultDeny";
        }
    }

    private record MatchingAclRule(StandardAcl acl, AuthorizationResult result) implements MatchingRule {

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

    AclCache getAclCache() {
        return aclCache;
    }
}
