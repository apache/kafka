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

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * An interface implemented by the QuorumController to implement ACL create and delete
 * operations for ClusterMetadataAuthorizer instances.
 *
 * These methods must all be thread-safe.
 */
public interface AclMutator {
    /**
     * Create the specified ACLs. If any ACL already exists, nothing will be done for that
     * one, and we will return a success result for it.
     *
     * @param aclBindings   The ACLs to create.
     * @return              The results for each AclBinding, in the order they were passed.
     */
    CompletableFuture<List<AclCreateResult>> createAcls(List<AclBinding> aclBindings);

    /**
     * Delete some ACLs based on the set of filters that is passed in.
     *
     * @param aclBindingFilters     The filters.
     * @return                      The results for each filter, in the order they were passed.
     */
    CompletableFuture<List<AclDeleteResult>> deleteAcls(
            List<AclBindingFilter> aclBindingFilters);
}
