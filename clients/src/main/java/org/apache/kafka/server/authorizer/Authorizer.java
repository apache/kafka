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

package org.apache.kafka.server.authorizer;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;

import java.io.Closeable;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 *
 * Pluggable authorizer interface for Kafka brokers.
 *
 * Startup sequence in brokers:
 * <ol>
 *   <li>Broker creates authorizer instance if configured in `authorizer.class.name`.</li>
 *   <li>Broker configures and starts authorizer instance. Authorizer implementation starts loading its metadata.</li>
 *   <li>Broker starts SocketServer to accept connections and process requests.</li>
 *   <li>For each listener, SocketServer waits for authorization metadata to be available in the
 *       authorizer before accepting connections. The future returned by {@link #start(AuthorizerServerInfo)}
 *       for each listener must return only when authorizer is ready to authorize requests on the listener.</li>
 *   <li>Broker accepts connections. For each connection, broker performs authentication and then accepts Kafka requests.
 *       For each request, broker invokes {@link #authorize(AuthorizableRequestContext, List)} to authorize
 *       actions performed by the request.</li>
 * </ol>
 *
 * Authorizer implementation class may optionally implement @{@link org.apache.kafka.common.Reconfigurable}
 * to enable dynamic reconfiguration without restarting the broker.
 * <p>
 * <b>Threading model:</b>
 * <ul>
 *   <li>All authorizer operations including authorization and ACL updates must be thread-safe.</li>
 *   <li>ACL update methods are asynchronous. Implementations with low update latency may return a
 *       completed future using {@link java.util.concurrent.CompletableFuture#completedFuture(Object)}.
 *       This ensures that the request will be handled synchronously by the caller without using a
 *       purgatory to wait for the result. If ACL updates require remote communication which may block,
 *       return a future that is completed asynchronously when the remote operation completes. This enables
 *       the caller to process other requests on the request threads without blocking.</li>
 *   <li>Any threads or thread pools used for processing remote operations asynchronously can be started during
 *       {@link #start(AuthorizerServerInfo)}. These threads must be shutdown during {@link Authorizer#close()}.</li>
 * </ul>
 * </p>
 */
@InterfaceStability.Evolving
public interface Authorizer extends Configurable, Closeable {

    /**
     * Starts loading authorization metadata and returns futures that can be used to wait until
     * metadata for authorizing requests on each listener is available. Each listener will be
     * started only after its metadata is available and authorizer is ready to start authorizing
     * requests on that listener.
     *
     * @param serverInfo Metadata for the broker including broker id and listener endpoints
     * @return CompletionStage for each endpoint that completes when authorizer is ready to
     *         start authorizing requests on that listener.
     */
    Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo);

    /**
     * Authorizes the specified action. Additional metadata for the action is specified
     * in `requestContext`.
     * <p>
     * This is a synchronous API designed for use with locally cached ACLs. Since this method is invoked on the
     * request thread while processing each request, implementations of this method should avoid time-consuming
     * remote communication that may block request threads.
     *
     * @param requestContext Request context including request type, security protocol and listener name
     * @param actions Actions being authorized including resource and operation for each action
     * @return List of authorization results for each action in the same order as the provided actions
     */
    List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions);

    /**
     * Creates new ACL bindings.
     * <p>
     * This is an asynchronous API that enables the caller to avoid blocking during the update. Implementations of this
     * API can return completed futures using {@link java.util.concurrent.CompletableFuture#completedFuture(Object)}
     * to process the update synchronously on the request thread.
     *
     * @param requestContext Request context if the ACL is being created by a broker to handle
     *        a client request to create ACLs. This may be null if ACLs are created directly in ZooKeeper
     *        using AclCommand.
     * @param aclBindings ACL bindings to create
     *
     * @return Create result for each ACL binding in the same order as in the input list. Each result
     *         is returned as a CompletionStage that completes when the result is available.
     */
    List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext, List<AclBinding> aclBindings);

    /**
     * Deletes all ACL bindings that match the provided filters.
     * <p>
     * This is an asynchronous API that enables the caller to avoid blocking during the update. Implementations of this
     * API can return completed futures using {@link java.util.concurrent.CompletableFuture#completedFuture(Object)}
     * to process the update synchronously on the request thread.
     * <p>
     * Refer to the authorizer implementation docs for details on concurrent update guarantees.
     *
     * @param requestContext Request context if the ACL is being deleted by a broker to handle
     *        a client request to delete ACLs. This may be null if ACLs are deleted directly in ZooKeeper
     *        using AclCommand.
     * @param aclBindingFilters Filters to match ACL bindings that are to be deleted
     *
     * @return Delete result for each filter in the same order as in the input list.
     *         Each result indicates which ACL bindings were actually deleted as well as any
     *         bindings that matched but could not be deleted. Each result is returned as a
     *         CompletionStage that completes when the result is available.
     */
    List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters);

    /**
     * Returns ACL bindings which match the provided filter.
     * <p>
     * This is a synchronous API designed for use with locally cached ACLs. This method is invoked on the request
     * thread while processing DescribeAcls requests and should avoid time-consuming remote communication that may
     * block request threads.
     *
     * @return Iterator for ACL bindings, which may be populated lazily.
     */
    Iterable<AclBinding> acls(AclBindingFilter filter);

    /**
     * Get the current number of ACLs, for the purpose of metrics. Authorizers that don't implement this function
     * will simply return -1.
     */
    default int aclCount() {
        return -1;
    }

    /**
     * Check if the caller is authorized to perform the given ACL operation on at least one
     * resource of the given type.
     *
     * Custom authorizer implementations should consider overriding this default implementation because:
     * 1. The default implementation iterates all AclBindings multiple times, without any caching
     *    by principal, host, operation, permission types, and resource types. More efficient
     *    implementations may be added in custom authorizers that directly access cached entries.
     * 2. The default implementation cannot integrate with any audit logging included in the
     *    authorizer implementation.
     * 3. The default implementation does not support any custom authorizer configs or other access
     *    rules apart from ACLs.
     *
     * @param requestContext Request context including request resourceType, security protocol and listener name
     * @param op             The ACL operation to check
     * @param resourceType   The resource type to check
     * @return               Return {@link AuthorizationResult#ALLOWED} if the caller is authorized
     *                       to perform the given ACL operation on at least one resource of the
     *                       given type. Return {@link AuthorizationResult#DENIED} otherwise.
     */
    default AuthorizationResult authorizeByResourceType(AuthorizableRequestContext requestContext, AclOperation op, ResourceType resourceType) {
        SecurityUtils.authorizeByResourceTypeCheckArgs(op, resourceType);

        // Check a hard-coded name to ensure that super users are granted
        // access regardless of DENY ACLs.
        if (authorize(requestContext, Collections.singletonList(new Action(
                op, new ResourcePattern(resourceType, "hardcode", PatternType.LITERAL),
                0, true, false)))
                .get(0) == AuthorizationResult.ALLOWED) {
            return AuthorizationResult.ALLOWED;
        }

        // Filter out all the resource pattern corresponding to the RequestContext,
        // AclOperation, and ResourceType
        ResourcePatternFilter resourceTypeFilter = new ResourcePatternFilter(
            resourceType, null, PatternType.ANY);
        AclBindingFilter aclFilter = new AclBindingFilter(
            resourceTypeFilter, AccessControlEntryFilter.ANY);

        EnumMap<PatternType, Set<String>> denyPatterns =
            new EnumMap<>(PatternType.class) {{
                    put(PatternType.LITERAL, new HashSet<>());
                    put(PatternType.PREFIXED, new HashSet<>());
                }};
        EnumMap<PatternType, Set<String>> allowPatterns =
            new EnumMap<>(PatternType.class) {{
                    put(PatternType.LITERAL, new HashSet<>());
                    put(PatternType.PREFIXED, new HashSet<>());
                }};

        boolean hasWildCardAllow = false;

        KafkaPrincipal principal = new KafkaPrincipal(
            requestContext.principal().getPrincipalType(),
            requestContext.principal().getName());
        String hostAddr = requestContext.clientAddress().getHostAddress();

        for (AclBinding binding : acls(aclFilter)) {
            if (!binding.entry().host().equals(hostAddr) && !binding.entry().host().equals("*"))
                continue;

            if (!SecurityUtils.parseKafkaPrincipal(binding.entry().principal()).equals(principal)
                    && !binding.entry().principal().equals("User:*"))
                continue;

            if (binding.entry().operation() != op
                    && binding.entry().operation() != AclOperation.ALL)
                continue;

            if (binding.entry().permissionType() == AclPermissionType.DENY) {
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
                continue;
            }

            if (binding.entry().permissionType() != AclPermissionType.ALLOW)
                continue;

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
        }

        if (hasWildCardAllow) {
            return AuthorizationResult.ALLOWED;
        }

        // For any literal allowed, if there's no dominant literal and prefix denied, return allow.
        // For any prefix allowed, if there's no dominant prefix denied, return allow.
        for (Map.Entry<PatternType, Set<String>> entry : allowPatterns.entrySet()) {
            for (String allowStr : entry.getValue()) {
                if (entry.getKey() == PatternType.LITERAL
                        && denyPatterns.get(PatternType.LITERAL).contains(allowStr))
                    continue;
                StringBuilder sb = new StringBuilder();
                boolean hasDominatedDeny = false;
                for (char ch : allowStr.toCharArray()) {
                    sb.append(ch);
                    if (denyPatterns.get(PatternType.PREFIXED).contains(sb.toString())) {
                        hasDominatedDeny = true;
                        break;
                    }
                }
                if (!hasDominatedDeny)
                    return AuthorizationResult.ALLOWED;
            }
        }

        return AuthorizationResult.DENIED;
    }

}
