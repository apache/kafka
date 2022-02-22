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
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.Authorizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


/**
 * An interface for Authorizers which store state in the __cluster_metadata log.
 *
 * These methods must all be thread-safe.
 */
public interface ClusterMetadataAuthorizer extends Authorizer {
    /**
     * Set the mutator object which should be used for creating and deleting ACLs.
     */
    void setAclMutator(AclMutator aclMutator);

    /**
     * Get the mutator object which should be used for creating and deleting ACLs.
     *
     * @throws org.apache.kafka.common.errors.NotControllerException
     *              If the aclMutator was not set.
     */
    AclMutator aclMutatorOrException();

    /**
     * Load the ACLs in the given map. Anything not in the map will be removed.
     * The authorizer will also wait for this initial snapshot load to complete when
     * coming up.
     */
    void loadSnapshot(Map<Uuid, StandardAcl> acls);

    /**
     * Add a new ACL. Any ACL with the same ID will be replaced.
     */
    void addAcl(Uuid id, StandardAcl acl);

    /**
     * Remove the ACL with the given ID.
     */
    void removeAcl(Uuid id);

    /**
     * Create ACLs. This function must be called on the active controller, or else
     * the futures will fail with NOT_CONTROLLER.
     *
     * @param requestContext    The request context.
     * @param aclBindings       The ACL bindings to create.
     *
     * @return a list of futures, one per input acl binding. Each future will be completed
     * once addAcl has been called on the controller, and the ACL has been persisted to
     * the cluster metadata log.
     */
    default List<? extends CompletionStage<AclCreateResult>> createAcls(
            AuthorizableRequestContext requestContext,
            List<AclBinding> aclBindings) {
        List<CompletableFuture<AclCreateResult>> futures = new ArrayList<>(aclBindings.size());
        AclMutator aclMutator = aclMutatorOrException();
        aclBindings.forEach(b -> futures.add(new CompletableFuture<>()));
        aclMutator.createAcls(aclBindings).whenComplete((results, throwable) -> {
            if (throwable == null && results.size() != futures.size()) {
                throwable = new UnknownServerException("Invalid size " +
                    "of result set from controller. Expected " + futures.size() +
                    "; got " + results.size());
            }
            if (throwable == null) {
                for (int i = 0; i < futures.size(); i++) {
                    futures.get(i).complete(results.get(i));
                }
            } else {
                for (CompletableFuture<AclCreateResult> future : futures) {
                    ApiException e = (throwable instanceof ApiException) ? (ApiException) throwable :
                        ApiError.fromThrowable(throwable).exception();
                    future.complete(new AclCreateResult(e));
                }
            }
        });
        return futures;
    }

    /**
     * Delete ACLs based on filters. This function must be called on the active
     * controller, or else the futures will fail with NOT_CONTROLLER.
     *
     * @param requestContext    The request context.
     * @param filters           The ACL filters.
     *
     * @return a list of futures, one per input acl filter. Each future will be completed
     * once the relevant deleteAcls have been called on the controller (if any), and th
     * ACL deletions have been persisted to the cluster metadata log (if any).
     */
    default List<? extends CompletionStage<AclDeleteResult>> deleteAcls(
            AuthorizableRequestContext requestContext,
            List<AclBindingFilter> filters) {
        List<CompletableFuture<AclDeleteResult>> futures = new ArrayList<>(filters.size());
        AclMutator aclMutator = aclMutatorOrException();
        filters.forEach(b -> futures.add(new CompletableFuture<>()));
        aclMutator.deleteAcls(filters).whenComplete((results, throwable) -> {
            if (throwable == null && results.size() != futures.size()) {
                throwable = new UnknownServerException("Invalid size " +
                    "of result set from controller. Expected " + futures.size() +
                    "; got " + results.size());
            }
            if (throwable == null) {
                for (int i = 0; i < futures.size(); i++) {
                    futures.get(i).complete(results.get(i));
                }
            } else {
                for (CompletableFuture<AclDeleteResult> future : futures) {
                    ApiException e = (throwable instanceof ApiException) ? (ApiException) throwable :
                        ApiError.fromThrowable(throwable).exception();
                    future.complete(new AclDeleteResult(e));
                }
            }
        });
        return futures;
    }
}
