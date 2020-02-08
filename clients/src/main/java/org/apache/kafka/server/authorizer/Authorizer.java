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

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.annotation.InterfaceStability;

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
}
