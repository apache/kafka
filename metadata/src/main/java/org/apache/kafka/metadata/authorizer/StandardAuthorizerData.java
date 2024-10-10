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
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.AuthorizerNotReadyException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;


/**
 * A class which encapsulates the configuration and the ACL data owned by StandardAuthorizer.
 *
 * The class is not thread-safe.
 */
public class StandardAuthorizerData extends AbstractAuthorizerData {

    /**
     * The logger to use.
     */
    final Logger log;

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
    private final MatchingRule noAclRule;

    /**
     * Contains all of the current ACLs
     */
    private AclCache aclCache;



    private static Logger createLogger(int nodeId) {
        return new LogContext("[StandardAuthorizer " + nodeId + "] ").logger(StandardAuthorizerData.class);
    }

    static StandardAuthorizerData createEmpty() {
        return new StandardAuthorizerData(createLogger(-1),
            null,
            false,
            Collections.emptySet(),
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
        this.aclMutator = aclMutator;
        this.loadingComplete = loadingComplete;
        this.superUsers = superUsers;
        this.noAclRule = () -> defaultResult;
        this.aclCache = aclCache;
    }

    public StandardAuthorizerData copyWithNewAclMutator(AclMutator newAclMutator) {
        return new StandardAuthorizerData(
            log,
            newAclMutator,
            loadingComplete,
            superUsers,
            noAclRule.result(),
            aclCache);
    }

    public StandardAuthorizerData copyWithNewLoadingComplete(boolean newLoadingComplete) {
        return new StandardAuthorizerData(log,
            aclMutator,
            newLoadingComplete,
            superUsers,
            noAclRule.result(),
            aclCache);
    }

    public StandardAuthorizerData copyWithNewConfig(int nodeId,
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

    public StandardAuthorizerData copyWithNewAcls(Map<Uuid, StandardAcl> acls) {
        AclCache newCache = new AclCache();
        for (Map.Entry<Uuid, StandardAcl> entry : acls.entrySet()) {
            newCache = newCache.addAcl(entry.getKey(), entry.getValue());
        }

        StandardAuthorizerData newData =  new StandardAuthorizerData(
            log,
            aclMutator,
            loadingComplete,
            superUsers,
            noAclRule.result(),
            newCache);
        log.info("Initialized with {} acl(s).", newCache.count());
        return newData;
    }

    public void addAcl(Uuid id, StandardAcl acl) {
        try {
            aclCache = aclCache.addAcl(id, acl);
            log.trace("Added ACL {}: {}", id, acl);
        } catch (Throwable e) {
            log.error("addAcl error", e);
            throw e;
        }
    }

    public void removeAcl(Uuid id) {
        try {
            AclCache aclCacheSnapshot = aclCache.removeAcl(id);
            log.trace("Removed ACL {}: {}", id, aclCacheSnapshot.getAcl(id));
            aclCache = aclCacheSnapshot;
        } catch (Throwable e) {
            log.error("removeAcl error", e);
            throw e;
        }
    }

    public Set<String> superUsers() {
        return superUsers;
    }

    public AuthorizationResult defaultResult() {
        return noAclRule.result();
    }

    public int aclCount() {
        return aclCache.count();
    }

    public AclMutator aclMutator() {
        return aclMutator;
    }

    @Override
    public Logger log() {
        return log;
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
    @Override
    public AuthorizationResult authorize(
            AuthorizableRequestContext requestContext,
            Action action
    ) {
        if (action.resourcePattern().patternType() != LITERAL) {
            throw new IllegalArgumentException("Only literal resources are supported. Got: " + action.resourcePattern().patternType());
        }
        KafkaPrincipal principal = AuthorizerData.baseKafkaPrincipal(requestContext);
        final MatchingRule rule;

        // Superusers are authorized to do anything.
        if (superUsers.contains(principal.toString())) {
            rule = SuperUserRule.INSTANCE;
        } else if (!loadingComplete) {
            throw new AuthorizerNotReadyException();
        } else {
            rule = findAclRule(
                AuthorizerData.matchingPrincipals(requestContext),
                requestContext.clientAddress().getHostAddress(),
                action
            );
        }
        logAuditMessage(principal, requestContext, action, rule);
        return rule.result();
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
        AclCache aclCacheSnapshot = aclCache;
        checkSection(aclCacheSnapshot, action, exemplar, matchingPrincipals, host, matchingRuleBuilder);
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
        checkSection(aclCacheSnapshot, action, exemplar, matchingPrincipals, host, matchingRuleBuilder);
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
            AclCache aclCacheSnapshot, Action action,
            StandardAcl exemplar,
            Set<KafkaPrincipal> matchingPrincipals,
            String host,
            MatchingRuleBuilder matchingRuleBuilder
    ) {
        String resourceName = action.resourcePattern().name();
        NavigableSet<StandardAcl> tailSet = aclCacheSnapshot.aclsByResource().tailSet(exemplar, true);
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
                tailSet = aclCacheSnapshot.aclsByResource().tailSet(exemplar, true);
                iterator = tailSet.iterator();
                continue;
            }
            matchingRuleBuilder.hasResourceAcls = true;
            AuthorizationResult result = AuthorizerData.findResult(action, matchingPrincipals, host, acl);
            if (ALLOWED == result) {
                matchingRuleBuilder.allowAcl = acl;
            } else if (DENIED == result) {
                matchingRuleBuilder.denyAcl = acl;
                return;
            }
        }
    }

    /**
     * Creates a consistent Iterable on read-only copy of AclBindings data for the given filter.
     *
     * @param filter The filter constraining the AclBindings to be present in the Iterable.
     * @return Iterable over AclBindings matching the filter.
     */
    public Iterable<AclBinding> acls(AclBindingFilter filter) {
        return aclCache.acls(filter::matches);
    }

    private Predicate<AccessControlEntry> buildResourceTypeFilter(final KafkaPrincipal principal, final String host, final AclOperation operation) {

        Predicate<AccessControlEntry> filter = ace -> ace.operation() == operation || ace.operation() == ALL;

        if (principal != null) {
            filter = filter.and(ace -> principal.toString().equals(ace.principal()) || ace.principal().equals(principal.getPrincipalType() + ":*") || ace.principal().equals(WILDCARD));
        }

        if (host != null) {
            filter = filter.and(ace -> host.equals(ace.host()) || ace.host().equals(WILDCARD));
        }

        if (log.isDebugEnabled()) {
            final Predicate<AccessControlEntry> f = filter;
            filter = ace -> {
                boolean result = f.test(ace);
                log.debug("StdAuthzData typeFilter p:{} h:{} o:{} -> {} -> {}", principal, host, operation, ace, result);
                return result;
            };
        }
        return filter;
    }

    @Override
    public AuthorizationResult authorizeByResourceType(
            KafkaPrincipal principal,
            String hostAddr,
            AclOperation operation,
            ResourceType resourceType) {
        SecurityUtils.authorizeByResourceTypeCheckArgs(operation, resourceType);

        // Filter out all the resource pattern corresponding to the principal, hostAddr,
        // AclOperation, and ResourceType

        ResourcePatternFilter resourceTypeFilter = new ResourcePatternFilter(
                resourceType, null, PatternType.ANY);

        Predicate<AclBinding> aclFilter = new AclBindingFilter(
                resourceTypeFilter, AccessControlEntryFilter.ANY)::matches;

        Predicate<AccessControlEntry> aceFilter = buildResourceTypeFilter(principal, hostAddr, operation);

        aclFilter = aclFilter.and(binding -> aceFilter.test(binding.entry()));

        if (log.isDebugEnabled()) {
            final Predicate<AclBinding> inner = aclFilter;
            aclFilter = new Predicate<AclBinding>() {
                @Override
                public boolean test(AclBinding aclBinding) {
                    boolean result = inner.test(aclBinding);
                    log.debug("StdAuthzData authorizeByResourceType: {} -> {}}", aclBinding, result);
                    return result;
                }
            };
        }

        EnumMap<PatternType, NavigableSet<String>> denyPatterns =
                new EnumMap<PatternType, NavigableSet<String>>(PatternType.class) {{
                    put(PatternType.LITERAL, new TreeSet<>());
                    put(PatternType.PREFIXED, new TreeSet<>());
                }};

        EnumMap<PatternType, Set<String>> allowPatterns =
                new EnumMap<PatternType, Set<String>>(PatternType.class) {{
                    put(PatternType.LITERAL, new HashSet<>());
                    put(PatternType.PREFIXED, new HashSet<>());
                }};

        boolean hasWildCardAllow = false;

        for (AclBinding binding : aclCache.acls(aclFilter)) {
            switch (binding.entry().permissionType()) {
                case DENY:
                    switch (binding.pattern().patternType()) {
                        case LITERAL:
                            // If wildcard deny exists, return deny directly
                            if (binding.pattern().name().equals(ResourcePattern.WILDCARD_RESOURCE))
                                return AuthorizationResult.DENIED;
                            denyPatterns.get(PatternType.LITERAL).add(binding.pattern().name());
                            break;
                        case PREFIXED:
                            denyPatterns.get(PatternType.PREFIXED).add(binding.pattern().name());
                            break;
                        default:
                    }
                    break;
                case ALLOW:
                    switch (binding.pattern().patternType()) {
                        case LITERAL:
                            if (binding.pattern().name().equals(ResourcePattern.WILDCARD_RESOURCE)) {
                                hasWildCardAllow = true;
                                continue;
                            }
                            allowPatterns.get(PatternType.LITERAL).add(binding.pattern().name());
                            break;
                        case PREFIXED:
                            allowPatterns.get(PatternType.PREFIXED).add(binding.pattern().name());
                            break;
                        default:
                    }
                    break;
                default:
            }
        }

        if (hasWildCardAllow) {
            return AuthorizationResult.ALLOWED;
        }

        // if there are allow and deny literals with the same name remove the allow ones.
        allowPatterns.get(LITERAL).removeAll(denyPatterns.get(LITERAL));

        Predicate<String> allowFilter = s -> {
            for (String prefix : denyPatterns.get(PatternType.PREFIXED).subSet(s.substring(0, 1), s)) {
                if (s.startsWith(prefix)) {
                    return false;
                }
            }
            return true;
        };

        // For any literal allowed, if there's no dominant literal and prefix denied, return allow.
        // For any prefix allowed, if there's no dominant prefix denied, return allow.
        for (Map.Entry<PatternType, Set<String>> entry : allowPatterns.entrySet()) {
            for (String allowStr : entry.getValue()) {
                if (allowFilter.test(allowStr)) {
                    log.debug("ACL found {}", allowStr);
                    return AuthorizationResult.ALLOWED;
                }
            }
        }

        log.debug("No ACL found -- returning {}", noAclRule.result());
        return noAclRule.result();
    }
}
