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
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;


/**
 * A class which encapsulates the configuration and the ACL data owned by StandardAuthorizer.
 *
 * The methods in this class support lockless concurrent access.
 */
public class StandardAuthorizerData {
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
    private final DefaultRule defaultRule;

    /**
     * Maps ACL IDs to ACLs.
     */
    private final Map<Uuid, StandardAcl> aclsById;

    /**
     * All the prefix ACLs for a given resource type and name pair.
     */
    private final Map<ResourceType, PrefixNode> prefixAcls;

    /**
     * All the literal ACLs for a given resource type and name pair.
     */
    private final Map<Resource, ResourceAcls> literalAcls;

    private static Logger createLogger(int nodeId) {
        return new LogContext("[StandardAuthorizer " + nodeId + "] ").logger(StandardAuthorizerData.class);
    }

    private static Logger auditLogger() {
        return LoggerFactory.getLogger("kafka.authorizer.logger");
    }

    static StandardAuthorizerData createEmpty() {
        return new StandardAuthorizerData(
            createLogger(-1),
            null,
            false,
            Collections.emptySet(),
            DefaultRule.DENIED,
            emptyMap(),
            emptyMap(),
            emptyMap());
    }

    private StandardAuthorizerData(
        Logger log,
        AclMutator aclMutator,
        boolean loadingComplete,
        Set<String> superUsers,
        DefaultRule defaultRule,
        Map<Uuid, StandardAcl> aclsById,
        Map<ResourceType, PrefixNode> prefixAcls,
        Map<Resource, ResourceAcls> literalAcls
    ) {
        this.log = log;
        this.auditLog = auditLogger();
        this.aclMutator = aclMutator;
        this.loadingComplete = loadingComplete;
        this.superUsers = superUsers;
        this.defaultRule = defaultRule;
        this.aclsById = aclsById;
        this.prefixAcls = prefixAcls;
        this.literalAcls = literalAcls;
    }

    StandardAuthorizerData copyWithNewAclMutator(AclMutator newAclMutator) {
        return new StandardAuthorizerData(
            log,
            newAclMutator,
            loadingComplete,
            superUsers,
            defaultRule,
            aclsById,
            prefixAcls,
            literalAcls);
    }

    StandardAuthorizerData copyWithNewLoadingComplete(boolean newLoadingComplete) {
        return new StandardAuthorizerData(
            log,
            aclMutator,
            newLoadingComplete,
            superUsers,
            defaultRule,
            aclsById,
            prefixAcls,
            literalAcls);
    }

    StandardAuthorizerData copyWithNewConfig(
        int nodeId,
        Set<String> newSuperUsers,
        AuthorizationResult newDefaultResult
    ) {
        return new StandardAuthorizerData(
            createLogger(nodeId),
            aclMutator,
            loadingComplete,
            newSuperUsers,
            new DefaultRule(newDefaultResult),
            aclsById,
            prefixAcls,
            literalAcls);
    }

    StandardAuthorizerData copyWithAclSnapshot(Map<Uuid, StandardAcl> snapshot) {
        AclLoader loader = new AclLoader(snapshot);
        AclLoader.Result result = loader.build();
        return new StandardAuthorizerData(
            log,
            aclMutator,
            loadingComplete,
            superUsers,
            defaultRule,
            result.newAclsById(),
            result.newPrefixed(),
            result.newLiterals());
    }

    StandardAuthorizerData copyWithAclChanges(
        Map<Uuid, Optional<StandardAcl>> aclChanges
    ) {
        AclLoader loader = new AclLoader(aclsById, literalAcls, prefixAcls, aclChanges);
        AclLoader.Result result = loader.build();
        return new StandardAuthorizerData(
                log,
                aclMutator,
                loadingComplete,
                superUsers,
                defaultRule,
                result.newAclsById(),
                result.newPrefixed(),
                result.newLiterals());
    }

    Set<String> superUsers() {
        return superUsers;
    }

    AuthorizationResult defaultResult() {
        return defaultRule.result();
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
    AuthorizationResult authorize(
        AuthorizableRequestContext requestContext,
        Action action
    ) {
        return doAuthorize(requestContext, action, () ->
            findAuthorizationRule(matchingPrincipals(requestContext),
                requestContext.clientAddress().getHostAddress(), action));
    }

    AuthorizationResult authorizeByResourceType(
            AuthorizableRequestContext requestContext,
            AclOperation op,
            ResourceType resourceType
    ) {
        Action action = new Action(op,
                new ResourcePattern(resourceType, "NONE", PatternType.UNKNOWN), 0, true, true);
        return doAuthorize(requestContext, action, () ->
            findAuthorizationByResourceTypeRule(matchingPrincipals(requestContext),
                requestContext.clientAddress().getHostAddress(), op, resourceType));
    }

    AuthorizationResult doAuthorize(
        AuthorizableRequestContext requestContext,
        Action action,
        Supplier<MatchingRule> ruleSupplier
    ) {
        KafkaPrincipal principal = baseKafkaPrincipal(requestContext);
        MatchingRule rule;

        // Superusers are authorized to do anything.
        if (superUsers.contains(principal.toString())) {
            rule = SuperUserRule.INSTANCE;
        } else if (!loadingComplete) {
            // For non-superusers, we must check if loading is complete.
            log.debug("Raising AuthorizerNotReadyException because loading is not complete yet.");
            throw new AuthorizerNotReadyException();
        } else {
            rule = ruleSupplier.get();
            if (rule == null) {
                // If nothing matched, we return the default result.
                rule = defaultRule;
            }
        }
        rule.logAuditMessage(auditLog, principal, requestContext, action);
        return rule.result();
    }

    MatchingRule findAuthorizationRule(
        Set<KafkaPrincipal> matchingPrincipals,
        String host,
        Action action
    ) {
        // Check literal ACLs.
        ResourceType resourceType = action.resourcePattern().resourceType();
        String resourceName = action.resourcePattern().name();
        Resource literalResource = new Resource(resourceType, action.resourcePattern().name());
        MatchingRule literalRule = literalAcls.getOrDefault(literalResource, ResourceAcls.EMPTY).
                authorize(action.operation(), matchingPrincipals, host);
        if (literalRule != null && literalRule.result() == DENIED) return literalRule;

        // Check prefix ACLs and wildcards.
        PrefixNode prefixNode = prefixAcls.getOrDefault(resourceType, PrefixNode.EMPTY);
        PrefixRuleFinder finder = new PrefixRuleFinder(action.operation(),
                matchingPrincipals,
                host,
                literalRule);
        prefixNode.walk(resourceName, finder);
        return finder.bestRule();
    }

    MatchingRule findAuthorizationByResourceTypeRule(
        Set<KafkaPrincipal> matchingPrincipals,
        String host,
        AclOperation operation,
        ResourceType resourceType
    ) {
        // Check prefix ACLs and wildcards.
        PrefixRuleFinder finder = new PrefixRuleFinder(operation,
                matchingPrincipals,
                host,
                null);
        PrefixNode prefixNode = prefixAcls.getOrDefault(resourceType, PrefixNode.EMPTY);
        prefixNode.walk(finder);
        if (finder.bestRule() != null) return finder.bestRule();

        // Check literal ACLs.
        for (Entry<Resource, ResourceAcls> entry : literalAcls.entrySet()) {
            Resource resource = entry.getKey();
            if (resource.resourceType() != resourceType) continue;
            MatchingRule literalRule = literalAcls.getOrDefault(resource, ResourceAcls.EMPTY).
                    authorize(operation, matchingPrincipals, host);
            if (literalRule != null && literalRule.result() == ALLOWED) {
                finder = new PrefixRuleFinder(operation,
                        matchingPrincipals,
                        host,
                        literalRule);
                prefixNode.walk(finder);
                if (finder.bestRule() != null) return finder.bestRule();
            }
        }

        return null;
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
        return Utils.mkSet(basePrincipal, StandardAuthorizerConstants.WILDCARD_KAFKA_PRINCIPAL);
    }

    Iterable<AclBinding> acls(AclBindingFilter filter) {
        return new AclIterable(filter);
    }

    class AclIterable implements Iterable<AclBinding> {
        private final AclBindingFilter filter;

        AclIterable(AclBindingFilter filter) {
            this.filter = filter;
        }

        @Override
        public Iterator<AclBinding> iterator() {
            return new AclIterator(filter);
        }
    }

    class AclIterator implements Iterator<AclBinding> {
        private final AclBindingFilter filter;
        private Iterator<StandardAcl> iterator;
        private AclBinding next;

        AclIterator(AclBindingFilter filter) {
            this.filter = filter;
            this.iterator = aclsById.values().iterator();
            this.next = null;
        }

        @Override
        public boolean hasNext() {
            while (next == null) {
                if (!iterator.hasNext()) return false;
                AclBinding binding = iterator.next().toBinding();
                if (filter.matches(binding)) {
                    next = binding;
                }
            }
            return true;
        }

        @Override
        public AclBinding next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            AclBinding result = next;
            next = null;
            return result;
        }
    }
}
