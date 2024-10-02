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
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.metadata.authorizer.bitmap.BitMaps;
import org.apache.kafka.metadata.authorizer.trie.Inserter;
import org.apache.kafka.metadata.authorizer.trie.Node;
import org.apache.kafka.metadata.authorizer.trie.NodeData;
import org.apache.kafka.metadata.authorizer.trie.ReadOnlyNode;
import org.apache.kafka.metadata.authorizer.trie.StandardMatcher;
import org.apache.kafka.metadata.authorizer.trie.Traverser;
import org.apache.kafka.metadata.authorizer.trie.Trie;
import org.apache.kafka.metadata.authorizer.trie.Walker;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Predicate;

import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;

/**
 * An AuthorizerData implementation that stores the Authorization data in Tries based on the name of the resource in the ACL.
 *
 * <p>
 *     Design Notes:
 *     <ul>
 *         <li>There are multiple Tries, one for each Resource type seen in the stored ACLs</li>
 *         <li>This code handles the calls from the Authorizer and translates those into calls into the Trie.</li>
 *         <li>The business logic for locating an ACL and determining which ACL has precedence is defined in this class.</li>
 *         <li>Specific navigation logic is found in the trie package</li>
 *     </ul>
 * </p>
 *
 */
public class TrieAuthorizerData extends AbstractAuthorizerData {

    /**
     * A bitmap that maps the ALLOW permission on describe, read, write, delete and alter to the DESCRIBE operation.
     * @see #operationPredicate(AclOperation)
     */
    private static final int DESCRIBE_MAP = BitMaps.getIntBit(DESCRIBE.code()) | BitMaps.getIntBit(READ.code()) | BitMaps.getIntBit(WRITE.code()) | BitMaps.getIntBit(DELETE.code()) | BitMaps.getIntBit(ALTER.code());

    /**
     * A bitmap that maps the ALLOW permission on describe_configs, and alter_configs to the DESCRIBE_CONFIGS operation.
     * @see #operationPredicate(AclOperation)
     */
    private static final int CONFIGS_MAP = BitMaps.getIntBit(DESCRIBE_CONFIGS.code()) | BitMaps.getIntBit(ALTER_CONFIGS.code());

    /** A predicate that matchers LITERAL patterns */
    private static final Predicate<StandardAcl> LITERAL_PATTERN = acl -> acl.patternType() == LITERAL;
    /** A predicate that matches PREFIXED patterns */
    private static final Predicate<StandardAcl> PREFIXED_PATTERN = acl -> acl.patternType() == PREFIXED;

    /**
     * The logger to use.
     */
    private final Logger log;

    /**
     * The current AclMutator.
     */
    private final AclMutator aclMutator;

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
    private final TrieData trieData;

    /**
     * Create an empty AuthorizerData instance.
     * @return
     */
    static TrieAuthorizerData createEmpty() {
        return new TrieAuthorizerData(createLogger(-1),
                null,
                false,
                Collections.emptySet(),
                DENIED,
                new TrieData());
    }

    /**
     * Creates the logger.
     * @param nodeId the node ID to crete the logger for.
     * @return the logger.
     */
    private static Logger createLogger(int nodeId) {
        return new LogContext("[StandardAuthorizer " + nodeId + "] ").logger(TrieAuthorizerData.class);
    }

    private TrieAuthorizerData(Logger log,
                               AclMutator aclMutator,
                               boolean loadingComplete,
                               Set<String> superUsers,
                               AuthorizationResult defaultResult,
                               final TrieData trieData) {
        this.log = log;
        this.aclMutator = aclMutator;
        this.loadingComplete = loadingComplete;
        this.superUsers = superUsers;
        this.noAclRule = () -> defaultResult;
        this.trieData = trieData;
    }

    @Override
    public TrieAuthorizerData copyWithNewAclMutator(AclMutator newAclMutator) {
        return new TrieAuthorizerData(
                log,
                newAclMutator,
                loadingComplete,
                superUsers,
                noAclRule.result(),
                trieData);
    }

    @Override
    public TrieAuthorizerData copyWithNewLoadingComplete(boolean newLoadingComplete) {
        return new TrieAuthorizerData(log,
                aclMutator,
                newLoadingComplete,
                superUsers,
                noAclRule.result(),
                trieData);
    }

    @Override
    public TrieAuthorizerData copyWithNewConfig(int nodeId,
                                                Set<String> newSuperUsers,
                                                AuthorizationResult newDefaultResult) {
        return new TrieAuthorizerData(
                createLogger(nodeId),
                aclMutator,
                loadingComplete,
                newSuperUsers,
                newDefaultResult,
                trieData);
    }

    @Override
    public TrieAuthorizerData copyWithNewAcls(Map<Uuid, StandardAcl> acls) {
        TrieAuthorizerData newData =  new TrieAuthorizerData(
                log,
                aclMutator,
                loadingComplete,
                superUsers,
                noAclRule.result(),
                new TrieData(acls));
        log.info("Initialized with {} acl(s).", newData.aclCount());
        return newData;
    }


    @Override
    public void addAcl(Uuid uuid, StandardAcl acl) {
        trieData.add(uuid, acl);
    }

    @Override
    public void removeAcl(Uuid uuid) {
        trieData.remove(uuid);
    }

    @Override
    public Set<String> superUsers() {
        return superUsers;
    }

    @Override
    public AuthorizationResult defaultResult() {
        return noAclRule.result();
    }

    @Override
    public int aclCount() {
        return trieData.count();
    }

    @Override
    public AclMutator aclMutator() {
        return aclMutator;
    }

    @Override
    public Logger log() {
        return log;
    }

    @Override
    public Iterable<AclBinding> acls(AclBindingFilter filter) {
        return trieData.acls(filter);
    }

    @Override
    public AuthorizationResult authorize(
            AuthorizableRequestContext requestContext,
            Action action
    ) {
        if (action.resourcePattern().patternType() != LITERAL) {
            throw new IllegalArgumentException("Only literal resources are supported. Got: " + action.resourcePattern().patternType());
        }
        KafkaPrincipal principal = requestContext.principal(); //AuthorizerData.baseKafkaPrincipal(requestContext);
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

    /**
     * Checks if the node has a matching action within the filtered ACLs.
     * @param node The node to check.
     * @param aclFilter the ACL filter to apply.
     * @param action the Action we are seeking.
     * @return an Optional StandardACL that is present if a matching ACL is found.
     */
    private Optional<StandardAcl> check(NodeData<AclContainer> node, Predicate<StandardAcl> aclFilter, Action action) {
        AclContainer container = node.getContents();
        return container == null ? Optional.empty() : container.first(resourcePatternFilter(node.getFragment(), action).and(aclFilter));
    }

    /**
     * Checks if the node has a matching action within the filtered ACLs.
     * @param node The node to check.
     * @param aclFilter the ACL filter to apply.
     * @return an Optional StandardACL that is present if a matching ACL is found.
     */
    private Optional<StandardAcl> checkWildcard(NodeData<AclContainer> node, Predicate<StandardAcl> aclFilter) {
        AclContainer container = node.getContents();
        return container == null ? Optional.empty() : container.first(LITERAL_PATTERN.and(aclFilter));
    }

    /**
     * Creates a predicate that will match the resource pattern type required by the action against the node.
     * @param fragment The fragment that we are currently looking at.
     * @param action the action to find.
     * @return either {@code literalPattern} or {@code prefixPattern} depending on whether the node fragment is a wildcard.
     */
    private Predicate<StandardAcl> resourcePatternFilter(String fragment, Action action) {
        return action.resourcePattern().name().endsWith(fragment) ? LITERAL_PATTERN : PREFIXED_PATTERN;
    }

    /**
     * Creates a Predicate that matches the host or the wildcard.
     * @param host the host to match.
     * @return the Predicate that matches the host pattern or wildcard.
     */
    Predicate<StandardAcl> hostPredicate(final String host) {
        return  acl -> acl.host().equals(WILDCARD) || acl.host().equals(host);
    }

    /**
     * Create a predicate that matches the operation depending on the ACL operation and the action operation.
     * This method performs the expansion of the ALL in the ACL to process DESCRIBE and DESCRIBE_CONFIGS correctly.
     * @param operation The Acl operation
     * @return the Predicate that matches the ACL operation.
     */
    Predicate<StandardAcl> operationPredicate(final AclOperation operation) {
        // operation filter
        return acl -> {
            if (acl.operation() != ALL) {
                if (acl.permissionType().equals(ALLOW)) {
                    switch (operation) {
                        case DESCRIBE:
                            return BitMaps.contains(DESCRIBE_MAP, acl.operation().code());
                        case DESCRIBE_CONFIGS:
                            return BitMaps.contains(CONFIGS_MAP, acl.operation().code());
                        default:
                            return operation == acl.operation();
                    }
                } else {
                    return operation == acl.operation();
                }
            }
            return true;
        };
    }

    /**
     * performs an authorization check forto match any items of the resource type.
     * @param principal the principal to limit the search by.
     * @param host the host to limit the search by.
     * @param operation the operation to limit the search to.
     * @param resourceType the resource type to limit the search to.
     * @return the authorization for the first matching resource.
     */
    @Override
    public AuthorizationResult authorizeByResourceType(KafkaPrincipal principal, String host, AclOperation operation, ResourceType resourceType) {
        SecurityUtils.authorizeByResourceTypeCheckArgs(operation, resourceType);
        MatchingRule rule = matchingRuleByResourceType(buildResourceTypeFilter(principal, host, operation), resourceType);
        logAuditMessage(principal, host, operation, resourceType, rule);
        return rule.result();
    }

    private Predicate<StandardAcl> buildResourceTypeFilter(final KafkaPrincipal principal, final String host, final AclOperation operation) {
        Predicate<StandardAcl> filter = acl -> acl.operation() == operation || acl.operation() == ALL;

        if (principal != null) {
            filter = filter.and(acl -> principal.toString().equals(acl.principal()) || acl.principal().equals(principal.getPrincipalType() + ":*") || acl.principal().equals(WILDCARD));
        }
        if (host != null) {
            filter = filter.and(acl -> host.equals(acl.host()) || acl.host().equals(WILDCARD));
        }

        if (log.isDebugEnabled()) {
            final Predicate<StandardAcl> f = filter;
            filter = acl -> {
                boolean result = f.test(acl);
                log.debug("TriAuthzData TypeFilter: p:{} h:{} o:{} -> {} -> {}", principal, host, operation, acl, result);
                return result;
            };
        }
        return filter;
    }

    /**
     * Performs a search and locates a matching rule for the resource type.
     * Should walk the tree in preorder-NLR order.  If DENY is determined for any node do not process the children.
     * If an ALLOW is discovered return ALLOW.
     * otherwise return noAclRule.
     * @param filter The filter for node contents.
     * @param resourceType the resource type to limit the search to.
     * @return the matching rule.
     */
    private MatchingRule matchingRuleByResourceType(final Predicate<StandardAcl> filter, final ResourceType resourceType) {
        // Define the filter used in the traversal of the trie.  If the contents matches the filter we are done.

        Trie<AclContainer> trie = trieData.getTrie(resourceType);
        if (trie == null) {
            log.debug("No ACL found -- returning {}", noAclRule.result());
            return noAclRule;
        }

        // walk the trie looking for a match.
        MatchingRuleTraversal traversal = Walker.traverse(trie, new MatchingRuleTraversal(filter));
        return matchingRuleFromOptionalAcl(traversal.optionalAcl);
    }


    /**
     * Find the matching Rule for the arguments.
     * @param matchingPrincipals the principals to accept.
     * @param host the host to limit the search by.
     * @param action the action to search for.
     * @return the Matching rule for the search.
     */
    private MatchingRule findAclRule(
            Set<KafkaPrincipal> matchingPrincipals,
            String host,
            Action action
    ) {
        // principal filter
        final Predicate<StandardAcl> principalFilter = acl -> matchingPrincipals.contains(acl.kafkaPrincipal());
        final Predicate<StandardAcl> operationFilter = operationPredicate(action.operation());
        final Predicate<StandardAcl> hostFilter = hostPredicate(host);

        // acl filter = operation + host + principal
        final Predicate<StandardAcl> aclFilter = operationFilter.and(hostFilter).and(principalFilter);

        // stop searching when we hit a valid DENY.
        final Predicate<StandardAcl> permissionFilter = acl -> acl.permissionType() == DENY;

        final Predicate<StandardAcl> exitFilter = permissionFilter.and(aclFilter);
        final Predicate<StandardAcl> seenFilter = acl -> acl.resourceName().equals(action.resourcePattern().name()) ||  PREFIXED_PATTERN.test(acl);
        final boolean[] seen = new boolean[1];

        Predicate<StandardAcl> matchFilter = standardAcl -> {
            if (seenFilter.test(standardAcl)) {
                seen[0] = true;
                return aclFilter.test(standardAcl);
            }
            return false;
        };

        Predicate<NodeData<AclContainer>> exit = n -> n.getName().equals("") ? checkWildcard(n, exitFilter).isPresent() : check(n, exitFilter, action).isPresent(); // || checkWildcard(n, exitFilter).isPresent();

        List<StandardAcl> targets = trieData.find(action.resourcePattern(), matchFilter, exit);

        if (targets.isEmpty()) {
            targets = trieData.find(new ResourcePattern(action.resourcePattern().resourceType(), WILDCARD, LITERAL), aclFilter.and(LITERAL_PATTERN), n -> false);
            if (targets.isEmpty()) {
                log.info("No match for {} {} {}", action, host, matchingPrincipals);
                return seen[0] ? MatchingRule.DENY : noAclRule;
            }
        }

        StandardAcl acl = targets.get(0);
        return new MatchingAclRule(acl, acl.permissionType().equals(ALLOW) ? ALLOWED : DENIED);
    }


    /**
     * Creates a matching rule from an Optional ACL.
     * @param optionalAcl the optional ACL, it not present return noAclRule.
     * @return a matching rule.
     */
    private MatchingRule matchingRuleFromOptionalAcl(Optional<StandardAcl> optionalAcl) {
        if (optionalAcl.isPresent()) {
            StandardAcl acl = optionalAcl.get();
            return new MatchingAclRule(acl, acl.permissionType().equals(ALLOW) ? ALLOWED : DENIED);
        } else {
            return noAclRule;
        }
    }

    /**
     * A container for ACLs that are sorted by permission (Deny first), PatternType, Operation, principal and host.
     * Since these are used on Nodes the resource type and resource name have already been accounted for.
     */
    private static class AclContainer {

        /** A custom comparator to put DENY first in the list of permissions */
        private Comparator<AclPermissionType> permissionOrder = new Comparator<AclPermissionType>() {
            @Override
            public int compare(AclPermissionType aclPermissionType, AclPermissionType other) {
                if (aclPermissionType == DENY) {
                    return other == DENY ? 0 : -1;
                }
                return other == DENY ? 1 : aclPermissionType.compareTo(other);
            }
        };

        /** ensure that '*' comes last even with Unicode names */
        private Comparator<String> principalOrder = new Comparator<String>() {

            @Override
            public int compare(String str, String other) {
                int result = str.compareTo(other);
                if (result == 0)  {
                    return result;
                }
                // split off the types
                int pos1 = str.indexOf(":");
                int pos2 = other.indexOf(":");
                result = str.substring(0, pos1).compareTo(other.substring(0, pos2));
                if (result != 0) {
                    return result;
                }

                // if types are equal then names must not be equal because of equality check at start.
                String one = str.substring(pos1 + 1);
                String two = other.substring(pos2 + 1);
                result = one.compareTo(two);
                return one.equals("*") ? 1 : other.equals("*") ? -1 : result;
            }
        };

        /** ensure that '*' comes last even with Unicode host names */
        private Comparator<String> hostOrder = new Comparator<String>() {

            @Override
            public int compare(String one, String two) {
                int result = one.compareTo(two);
                if (result == 0)  {
                    return result;
                }
                return one.equals("*") ? 1 : two.equals("*") ? -1 : one.compareTo(two);
            }
        };

        /** A Comparator to order the ACLs in the container by permission (Deny first), PatternType, Operation, principal and host */
        private Comparator<StandardAcl> partialOrder = (a, b) -> {
            int result = permissionOrder.compare(a.permissionType(), b.permissionType());
            if (result != 0) return result;
            result = a.patternType().compareTo(b.patternType());
            if (result != 0) return result;
            result = a.operation().compareTo(b.operation());
            if (result != 0) return result;
            result = principalOrder.compare(a.principal(), b.principal());
            if (result != 0) return result;
            result = hostOrder.compare(a.host(), b.host());
            return result;
        };

        /** The ACLs in the container */
        private final SortedSet<StandardAcl> partialAcls;

        AclContainer() {
            partialAcls = new ConcurrentSkipListSet<>(partialOrder);
        }

        /**
         * Constructs a container with a single ACL.
         * @param acl the Acl to put in the container.
         */
        AclContainer(StandardAcl acl) {
            this();
            partialAcls.add(acl);
        }

        /**
         * Adds an ACL to the container.
         * @param acl the ACL to add.
         */
        public void add(StandardAcl acl) {
            partialAcls.add(acl);
        }

        /**
         * Removes an ACL from the container.
         * @param acl the ACL to remove.
         */
        public void remove(StandardAcl acl) {
            partialAcls.remove(acl);
        }

        /**
         * Returns {@code true} if the container is empty.
         * @return {@code true} if the container is empty.
         */
        public boolean isEmpty() {
            return partialAcls.isEmpty();
        }

        /**
         * Given the predicate find the first ACL that matches.
         * @param filter the predicate to test the ACL
         * @return an Optional StandardACL containing the matching ACL or is empty.
         */
        public Optional<StandardAcl> first(Predicate<StandardAcl> filter) {
            for (StandardAcl acl : partialAcls) {
                if (filter.test(acl)) {
                    return Optional.of(acl);
                }
            }
            return Optional.empty();
        }

        /**
         * Find the first ACL of the permission type.
         * @param permission the permission type to find.
         * @param filter the predicate the ACL must match.
         * @return an Optional StandardACL containing the matching ACL or is empty.
         */
        public Optional<StandardAcl> first(AclPermissionType permission, Predicate<StandardAcl> filter) {
            StandardAcl exemplar = new StandardAcl(
                    null,
                    null,
                    PatternType.UNKNOWN, // Note that the UNKNOWN value sorts before all others.
                    "",
                    "",
                    AclOperation.UNKNOWN,
                    permission);
            for (StandardAcl candidate : partialAcls.tailSet(exemplar)) {
                if (filter.test(candidate)) {
                    return Optional.of(candidate);
                }
            }
            return Optional.empty();
        }

        public List<StandardAcl> findMatch(Predicate<StandardAcl> pattern) {
            // do not use stream() as the overhead is too great in the hot path.
            List<StandardAcl> result = new ArrayList<>();
            for (StandardAcl acl : partialAcls) {
                if (pattern.test(acl)) {
                    result.add(acl);
                }
            }
            return result;
        }
    }

    /**
     * The container for all the Tries.  There is one Trie for each ResourceType.
     */
    private static class TrieData {

        /**
         * The map of ResourceType to Trie
         */
        private final Map<ResourceType, Trie<AclContainer>> tries;
        /**
         * The map of UUid to ACL
         */
        private final Map<Uuid, StandardAcl> uuidMap;

        /**
         * The lotter for this operation class
         */
        private static final Logger log = LoggerFactory.getLogger(TrieData.class);

        private final Walker<AclContainer> walker;

        /**
         * Creates an empty TriData structure.
         */
        TrieData() {
            log.info("Constructing TrieData");
            tries = new HashMap<>();
            uuidMap = new HashMap<>();
            walker = new Walker<>();
        }

        /**
         * Create a TrieData structure from a collection of Acls.
         *
         * @param acls a map of Acl Uuid to the associated StandardAcl.
         */
        TrieData(Map<Uuid, StandardAcl> acls) {
            this();
            for (Map.Entry<Uuid, StandardAcl> entry : acls.entrySet()) {
                add(entry.getKey(), entry.getValue());
            }
        }

        /**
         * Add an ACL to the data.
         *
         * @param uuid the Uuid associated with the ACL>
         * @param acl  the ACL to add.
         */
        public void add(Uuid uuid, StandardAcl acl) {
            uuidMap.put(uuid, acl);
            Trie<AclContainer> trie = tries.get(acl.resourceType());
            if (trie == null) {
                synchronized (tries) {
                    trie = tries.get(acl.resourceType());
                    if (trie == null) {
                        log.info("creating trie for resource type {}.", acl.resourceType());
                        trie = new Trie<>();
                        tries.put(acl.resourceType(), trie);
                    }
                }
            }
            // if we are inserting a wildcard resource make it an empty string so that it is inserted on the
            // root node and will be found first
            Inserter inserter =  walker.inserter(acl.resourceName().equals(WILDCARD) ? "" : acl.resourceName());
            trie.insert(inserter, new AclContainer(acl), (old, value) -> {
                old.partialAcls.addAll(value.partialAcls);
                return old;
            });
        }

        /**
         * Removes an ACL fro mthe data.
         *
         * @param uuid the Uuid associated with the ACL to remove.
         */
        public void remove(Uuid uuid) {
            StandardAcl acl = uuidMap.get(uuid);
            if (acl != null) {
                uuidMap.remove(uuid);
                Trie<AclContainer> trie = tries.get(acl.resourceType());
                if (trie != null) {
                    log.debug("removing trie entry for " + acl);
                    trie.remove(walker.matcher(acl.resourceName()), container -> {
                        container.remove(acl);
                        return container;
                    });
                }
            }
        }

        /**
         * Get an iterable of AclBindings over all the ACLs in the data after applying the filter.
         * Only ACLs that pass the filter will be included.
         *
         * @param filter the filter to exclude ACLs.
         * @return an Iterable of AclBindings that pass the filter.
         */
        public Iterable<AclBinding> acls(AclBindingFilter filter) {
            List<AclBinding> aclBindingList = new ArrayList<>();
            switch (filter.patternFilter().resourceType()) {
                case UNKNOWN:
                    break;
                case ANY:

                    uuidMap.values().forEach(acl -> {
                        AclBinding aclBinding = acl.toBinding();
                        if (filter.matches(aclBinding)) {
                            aclBindingList.add(aclBinding);
                        }
                    });
                    break;
                case USER:
                case GROUP:
                case TOPIC:
                case CLUSTER:
                case DELEGATION_TOKEN:
                case TRANSACTIONAL_ID:
                    Trie<AclContainer> trie = tries.get(filter.patternFilter().resourceType());
                    // returns true to stop searching
                    Predicate<Node<AclContainer>> trieFilter = n -> {
                        AclContainer container = n.getContents();
                        if (container != null) {
                            for (StandardAcl acl : container.partialAcls) {
                                AclBinding aclBinding = acl.toBinding();
                                if (filter.matches(aclBinding)) {
                                    aclBindingList.add(aclBinding);
                                }
                            }
                        }
                        return false;
                    };
                    // populates aclBindinList by side effect.
                    Walker.preOrder(trieFilter, trie);
                    break;
            }
            return aclBindingList;
        }

        /**
         * Return the number of ACLs in the data.
         * @return the number of ACLs in the data.
         */
        public int count() {
            return uuidMap.size();
        }

        /**
         * Finds the node in the proper tree.
         * @param resourcePattern the resource pattern Trie to search.
         * @param exit the predicate that forces a stop/exit from the search.
         * @return the list of matching ACLs on the matching node.
         */
        private List<StandardAcl> find(ResourcePattern resourcePattern, Predicate<StandardAcl> matchFilter, Predicate<NodeData<AclContainer>> exit) {
            List<StandardAcl> results = Collections.emptyList();
            Trie<AclContainer> trie = tries.get(resourcePattern.resourceType());
            if (trie == null) {
                log.info("No trie found for {}", resourcePattern.resourceType());
            } else {
                ReadOnlyNode<AclContainer> result = trie.search(new StandardMatcher<>(resourcePattern.name(), exit));
                log.debug("Found {}.", result.getName());
                while (result != null && results.isEmpty()) {
                    if (result.hasContents()) {
                        results =  result.getContents().findMatch(matchFilter);
                    }
                    // no result so check parent.
                    if (results.isEmpty()) {
                        result = result.getParent();
                    }
                }
            }
            return results;
        }

        private Trie<AclContainer> getTrie(ResourceType resourceType) {
            return tries.get(resourceType);
        }
    }

    /**
     * Traverser for use when ALLOW ALL is not set.
     * <p></p>
     * This traverser walks the tree using a modified PreOrder strategy looking for an ALLOW access.
     * The modification is: </p>
     * <ul>
     * <li>If it finds a matching DENY on a node it stores the matching ACL and does not search the children (TRUNCATE state)</li>
     * <li>If it finds a matching ALLOW on a node it stores the matching ACL and stops (STOP state)</li>
     * <li>If it does not find a matching ACLit if follows the normatl preorder walk strategy (CONTINUE state)</li>
     * </ul>
     */
    private static class MatchingRuleTraversal implements Traverser<AclContainer> {

        enum State { CONTINUE, TRUNCATE, STOP };

        /** The ACL that was most recently matched */
        private Optional<StandardAcl> optionalAcl = Optional.empty();

        /** the filter to match the ACLs with */
        private final Predicate<StandardAcl> filter;

        /**
         * Constructs a traverser.
         * @param filter the filter to match the ACLs with.
         */
        MatchingRuleTraversal(final Predicate<StandardAcl> filter) {
            this.filter = filter;
        }

        @Override
        public void traverse(Node<AclContainer> node) {
            checkNode(node);
        }

        /**
         * Checks a node and determines the state of the node.
         * @param node the node to check.
         * @return The state of the node.
         */
        private State checkNode(Node<AclContainer> node) {
            switch (getState(node)) {
                case CONTINUE:
                    for (Node<AclContainer> child : node.getChildren()) {
                        if (checkNode(child) == State.STOP) {
                            return State.STOP;
                        }
                    }
                    break;
                case STOP:
                    return State.STOP;
                case TRUNCATE:
                    break;
            }
            return State.CONTINUE;
        }

        /**
         * Calculates the state of a determining if it has an ACL that matches the filter.
         * @param node the node to get the State for.
         * @return the State.
         */
        private State getState(Node<AclContainer> node) {
            if (node.hasContents()) {
                List<StandardAcl> lst = node.getContents().findMatch(filter);
                if (!lst.isEmpty()) {
                    StandardAcl acl = lst.get(0);
                    optionalAcl = Optional.of(acl);
                    return acl.permissionType() == DENY ? State.TRUNCATE : State.STOP;
                }
            }
            return State.CONTINUE;
        }
    }
}
