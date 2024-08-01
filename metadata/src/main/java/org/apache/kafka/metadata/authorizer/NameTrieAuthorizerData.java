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
import org.apache.kafka.metadata.authorizer.trie.StringTrie;
import org.apache.kafka.metadata.authorizer.trie.Node;
import org.apache.kafka.metadata.authorizer.trie.Walker;
import org.apache.kafka.metadata.authorizer.trie.WildcardRegistry;
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
import java.util.TreeSet;
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
public class NameTrieAuthorizerData extends AbstractAuthorizerData {

    /**
     * A bitmap that maps the ALLOW permission on describe, read, write, delete and alter to the DESCRIBE operation.
     * @see #operationFilter(AclOperation)
     */
    private static final int DESCRIBE_MAP = BitMaps.getIntBit(DESCRIBE.code()) | BitMaps.getIntBit(READ.code()) | BitMaps.getIntBit(WRITE.code()) | BitMaps.getIntBit(DELETE.code()) | BitMaps.getIntBit(ALTER.code());

    /**
     * A bitmap that maps the ALLOW permission on describe_configs, and alter_configs to the DESCRIBE_CONFIGS operation.
     * @see #operationFilter(AclOperation)
     */
    private static final int CONFIGS_MAP = BitMaps.getIntBit(DESCRIBE_CONFIGS.code()) | BitMaps.getIntBit(ALTER_CONFIGS.code());

    /** A predicate that matchers LITERAL patterns */
    private static final Predicate<StandardAcl> LITERAL_PATTERN = acl -> acl.patternType() == LITERAL;
    /** A predicate that matches PREFIXED patterns */
    private static final Predicate<StandardAcl> PREFIXED_PATTERN = acl -> acl.patternType() == PREFIXED;

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
    private final DefaultRule noAclRule;

    /**
     * Contains all of the current ACLs
     */
    private final TrieData trieData;

    /**
     * Create an empty AuthorizerData instance.
     * @return
     */
    public static NameTrieAuthorizerData createEmpty() {
        return new NameTrieAuthorizerData(createLogger(-1),
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
        return new LogContext("[StandardAuthorizer " + nodeId + "] ").logger(StandardAuthorizerData.class);
    }

    private NameTrieAuthorizerData(Logger log,
                                   AclMutator aclMutator,
                                   boolean loadingComplete,
                                   Set<String> superUsers,
                                   AuthorizationResult defaultResult,
                                   final TrieData trieData
                                   ) {
        this.log = log;
        this.aclMutator = aclMutator;
        this.loadingComplete = loadingComplete;
        this.superUsers = superUsers;
        this.noAclRule = new DefaultRule(defaultResult);
        this.trieData = trieData;
    }

    @Override
    public NameTrieAuthorizerData copyWithNewAclMutator(AclMutator newAclMutator) {
        return new NameTrieAuthorizerData(
                log,
                newAclMutator,
                loadingComplete,
                superUsers,
                noAclRule.result(),
                trieData);
    }

    @Override
    public NameTrieAuthorizerData copyWithNewLoadingComplete(boolean newLoadingComplete) {
        return new NameTrieAuthorizerData(log,
                aclMutator,
                newLoadingComplete,
                superUsers,
                noAclRule.result(),
                trieData);
    }

    @Override
    public NameTrieAuthorizerData copyWithNewConfig(int nodeId,
                                                    Set<String> newSuperUsers,
                                                    AuthorizationResult newDefaultResult) {
        return new NameTrieAuthorizerData(
                createLogger(nodeId),
                aclMutator,
                loadingComplete,
                newSuperUsers,
                newDefaultResult,
                trieData);
    }

    @Override
    public NameTrieAuthorizerData copyWithNewAcls(Map<Uuid, StandardAcl> acls) {
        NameTrieAuthorizerData newData =  new NameTrieAuthorizerData(
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

    /**
     * Checks if the node has a matching action within the filtered ACLs.
     * @param node The node to check.
     * @param aclFilter the ACL filter to apply.
     * @param action the Action we are seeking.
     * @return an Optional StandardACL that is present if a matching ACL is found.
     */
    private Optional<StandardAcl> check(Node<AclContainer> node, Predicate<StandardAcl> aclFilter, Action action) {
        return node.getContents() == null ? Optional.empty() : node.getContents().first(resourcePatternFilter(node, action).and(aclFilter));
    }

    /**
     * Creates a predicate that will match the resource pattern type requried by the action against the node.
     * @param node The node to check.
     * @param action the action to find.
     * @return either {@code literalPattern} or {@code prefixPattern} depending on whether or not the node fragment is a wildcard.
     */
    Predicate<StandardAcl> resourcePatternFilter(Node<AclContainer> node, Action action) {
        return WildcardRegistry.isWildcard(node.getFragment()) || action.resourcePattern().name().endsWith(node.getFragment()) ? LITERAL_PATTERN : PREFIXED_PATTERN;
    }

    /**
     * Creates a Predicate that matches the host or the wildcard.
     * @param host the host to match.
     * @return the Predicate that matches the host pattern or wildcard.
     */
    Predicate<StandardAcl> hostFilter(final String host) {
        return  acl -> acl.host().equals(WILDCARD) || acl.host().equals(host);
    }

    /**
     * Create a predicate that matches the operation depending on the ACL operation and the action operation.
     * This method performs the expansion of the ALL in the ACL to process DESCRIBE and DESCRIBE_CONFIGS correctly.
     * @param op The Acl operation
     * @return the Predicate that matches the ACL operation.
     */
    Predicate<StandardAcl> operationFilter(final AclOperation op) {
        // operation filter
        return acl -> {
            if (acl.operation() != ALL) {
                if (acl.permissionType().equals(ALLOW)) {
                    switch (op) {
                        case DESCRIBE:
                            return BitMaps.contains(DESCRIBE_MAP, acl.operation().code());
                        case DESCRIBE_CONFIGS:
                            return BitMaps.contains(CONFIGS_MAP, acl.operation().code());
                        default:
                            return op == acl.operation();
                    }
                } else {
                    return op == acl.operation();
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
        MatchingRule rule = matchingRuleByResourceType(principal, host, operation, resourceType);
        logAuditMessage(principal, host, operation, resourceType, rule);
        return rule.result();
    }

    /**
     * Performs a search and locates a matching rule for the resource type.
     * @param principal the principal to limit the search by.
     * @param host the host to limit the search by.
     * @param operation the operation to limit the search to.
     * @param resourceType the resource type to limit the search to.
     * @return the matching rule.
     */
    private MatchingRule matchingRuleByResourceType(KafkaPrincipal principal, String host, AclOperation operation, ResourceType resourceType) {
        // principal filter
        final Predicate<StandardAcl> principalFilter = acl -> principal.equals(acl.kafkaPrincipal());
        final Predicate<StandardAcl> aclFilter = operationFilter(operation).and(hostFilter(host)).and(principalFilter);
        final Predicate<StandardAcl> filter = aclFilter.and(principalFilter);


        // Define the filter used in the traversal of the trie.  If the contents matches the filter we are done.
        Predicate<Node<AclContainer>> traversalFilter = n -> {
            AclContainer contents = n.getContents();
            if (contents != null) {
                Optional<StandardAcl> opt = contents.first(filter);
                if (opt.isPresent()) {
                    return false;
                }
            }
            return true;
        };
        // walk the trie looking for a match.
        final Node<AclContainer> found = Walker.preOrder(traversalFilter, trieData.getTrie(resourceType).getRoot());
        // if found is set we have the match.
        if (found != null) {
            Optional<StandardAcl> optionalAcl = found.getContents().first(filter);
            if (optionalAcl.isPresent()) {
                log.info("ACL found {}", optionalAcl);
                return matchingRuleFromOptionalAcl(optionalAcl);
            }
        }
        log.info("No ACL found -- returning DENY");
        return MatchingRuleBuilder.DENY_RULE;
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

        // acl filter = operation + host + principal
        final Predicate<StandardAcl> aclFilter = operationFilter(action.operation()).and(hostFilter(host)).and(principalFilter);

        // stop searching when we hit a valid DENY.
        Predicate<StandardAcl> permissionFilter = acl -> acl.permissionType() == DENY;

        final Predicate<StandardAcl> exitFilter = permissionFilter.and(aclFilter);
        Node<AclContainer> target = trieData.findNode(action.resourcePattern(), n -> check(n, exitFilter, action).isPresent());
        // if the root of the tree -> no matches so return noAclRule
        if (target.getFragment().equals("")) {
            log.info("No match for {} {} {}", action, host, matchingPrincipals);
            return noAclRule;
        }
        log.info("Search returned {} {} {}", target, action, host);

        // see if we have a match or hit a matching DENY then this will find it.
        Optional<StandardAcl> optionalAcl = check(target, aclFilter, action);
        if (optionalAcl.isPresent()) {
            log.info("Matching ACL found {}", optionalAcl);
            return matchingRuleFromOptionalAcl(optionalAcl);
        }

        // if the target does not have matching rule then move back up the path looking for a matching node.
        permissionFilter = acl -> acl.permissionType() == ALLOW;
        final Predicate<StandardAcl> acceptFilter = permissionFilter.and(PREFIXED_PATTERN).and(aclFilter);
        while (!target.getFragment().equals("")) {
            AclContainer pattern = target.getContents();
            if (pattern != null) {
                optionalAcl = pattern.first(ALLOW, acceptFilter);
                if (optionalAcl.isPresent()) {
                    log.info("Prefixed ACL found {}", optionalAcl);
                    return matchingRuleFromOptionalAcl(optionalAcl);
                }
            }
            // scan up the tree looking for Prefix matches.
            target = target.getParent();
        }
        log.info("No ACL found -- returning DENY");
        return MatchingRuleBuilder.DENY_RULE;
    }

    /**
     * Creates a matching rule from an Optional ACL.
     * @param optionalAcl the optional ACL must be present.
     * @return a matching rule.
     */
    private MatchingRule matchingRuleFromOptionalAcl(Optional<StandardAcl> optionalAcl) {
        StandardAcl acl = optionalAcl.get();
        return new MatchingAclRule(acl, acl.permissionType().equals(ALLOW) ? ALLOWED : DENIED);
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

        /** A Comparator to order the ACLs in the container by permission (Deny first), PatternType, Operation, principal and host */
        private Comparator<StandardAcl> partialOrder = (a, b) -> {
            int result = permissionOrder.compare(a.permissionType(), b.permissionType());
            if (result != 0) return result;
            result = a.patternType().compareTo(b.patternType());
            if (result != 0) return result;
            result = a.operation().compareTo(b.operation());
            if (result != 0) return result;
            result = a.principal().compareTo(b.principal());
            if (result != 0) return result;
            result = a.host().compareTo(b.host());
            return result;
        };

        /** The ACLs in th econtainer */
        private final SortedSet<StandardAcl> partialAcls;

        /**
         * Constructs a container with a single ACL.
         * @param acl the Acl to put in the container.
         */
        AclContainer(StandardAcl acl) {
            partialAcls = new TreeSet<>(partialOrder);
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
    }

    /**
     * The container for all the Tries.  There is one Trie for each ResourceType.
     */
    private static class TrieData {
        /**
         * The map of ResourceType to Trie
         */
        private final Map<ResourceType, StringTrie<AclContainer>> tries;
        /**
         * The map of UUid to ACL
         */
        private final Map<Uuid, StandardAcl> uuidMap;

        /**
         * The lotter for this operation class
         */
        private static final Logger log = LoggerFactory.getLogger(TrieData.class);

        /**
         * Creates an empty TriData structure.
         */
        TrieData() {
            log.info("Constructing TrieData");
            tries = new HashMap<>();
            uuidMap = new HashMap<>();
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
            StringTrie<AclContainer> trie = tries.get(acl.resourceType());
            if (trie == null) {
                log.info("creating trie for resource type {}.", acl.resourceType());
                trie = new StringTrie<>();
                tries.put(acl.resourceType(), trie);
                AclContainer pattern = new AclContainer(acl);
                trie.put(acl.resourceName(), pattern);
            } else {
                AclContainer pattern = trie.get(acl.resourceName());
                if (pattern == null) {
                    pattern = new AclContainer(acl);
                    trie.put(acl.resourceName(), pattern);
                } else {
                    pattern.add(acl);
                }
            }
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
                StringTrie<AclContainer> trie = tries.get(acl.resourceType());
                if (trie != null) {
                    log.debug("removing trie entry for " + acl);
                    AclContainer pattern = trie.get(acl.resourceName());
                    if (pattern != null) {
                        pattern.remove(acl);
                        if (pattern.isEmpty()) {
                            trie.remove(acl.resourceName());
                        }
                    }

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
                    StringTrie<AclContainer> trie = tries.get(filter.patternFilter().resourceType());
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
                        return true;
                    };
                    // populates aclBindinList by side effect.
                    Walker.preOrder(trieFilter, trie.getRoot());
                    break;
            }
            return aclBindingList;
        }

        /**
         * Return the number of ACLs in the data.
         * @return
         */
        public int count() {
            return uuidMap.size();
        }

        /**
         * Finds the node in the proper tree.
         * @param resourcePattern the resource pattern Trie to search.
         * @param exit the predicate that forces a stop/exit from the search.
         * @return the Node that matches or caused an exit.
         */
        public Node<AclContainer> findNode(ResourcePattern resourcePattern, Predicate<Node<AclContainer>> exit) {
            StringTrie<AclContainer> trie = tries.get(resourcePattern.resourceType());
            if (trie == null) {
                log.info("No trie found for {}", resourcePattern.resourceType());
                return Node.makeRoot();
            }
            Node<AclContainer> n = trie.findNode(resourcePattern.name(), exit);
            log.debug("Returning {}.", n);
            return n;
        }


        private StringTrie<AclContainer> getTrie(ResourceType resourceType) {
            return tries.get(resourceType);
        }
    }

}
