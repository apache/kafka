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

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;


/**
 * The standard authorizer which is used in KRaft-based clusters if no other authorizer is
 * configured.
 */
public class StandardAuthorizer implements ClusterMetadataAuthorizer {
    public static final String SUPER_USERS_CONFIG = "super.users";

    public static final String ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG = "allow.everyone.if.no.acl.found";

    /**
     * A future which is completed once we have loaded up to the initial high watermark.
     */
    private final CompletableFuture<Void> initialLoadFuture = new CompletableFuture<>();

    /**
     * The current data. We use a read-write lock to synchronize reads and writes to the data. We
     * expect one writer and multiple readers accessing the ACL data, and we use the lock to make
     * sure we have consistent reads when writer tries to change the data.
     */
    private volatile AuthorizerData data;

    public StandardAuthorizer() {
        this(StandardAuthorizerData.createEmpty());
    }

    protected StandardAuthorizer(AuthorizerData data) {
        this.data = data;
    }

    protected AuthorizerData getData() {
        return data;
    }


    @Override
    public void setAclMutator(AclMutator aclMutator) {
        this.data = data.copyWithNewAclMutator(aclMutator);
    }

    @Override
    public AclMutator aclMutatorOrException() {
        AclMutator aclMutator = data.aclMutator();
        if (aclMutator == null) {
            throw new NotControllerException("The current node is not the active controller.");
        }
        return aclMutator;
    }

    @Override
    public void completeInitialLoad() {
        data = data.copyWithNewLoadingComplete(true);
        data.log().info("Completed initial ACL load process.");
        initialLoadFuture.complete(null);
    }

    // Visible for testing
    public CompletableFuture<Void> initialLoadFuture() {
        return initialLoadFuture;
    }

    @Override
    public void completeInitialLoad(Exception e) {
        if (!initialLoadFuture.isDone()) {
            data.log().error("Failed to complete initial ACL load process.", e);
            initialLoadFuture.completeExceptionally(e);
        }
    }

    @Override
    public void addAcl(Uuid id, StandardAcl acl) {
        data.addAcl(id, acl);
    }

    @Override
    public void removeAcl(Uuid id) {
        data.removeAcl(id);
    }

    @Override
    public void loadSnapshot(Map<Uuid, StandardAcl> acls) {
        data = data.copyWithNewAcls(acls);
    }

    @Override
    public Map<Endpoint, ? extends CompletionStage<Void>> start(
            AuthorizerServerInfo serverInfo) {
        Map<Endpoint, CompletableFuture<Void>> result = new HashMap<>();
        for (Endpoint endpoint : serverInfo.endpoints()) {
            if (serverInfo.earlyStartListeners().contains(
                    endpoint.listenerName().orElse(""))) {
                result.put(endpoint, CompletableFuture.completedFuture(null));
            } else {
                result.put(endpoint, initialLoadFuture);
            }
        }
        return result;
    }

    @Override
    public List<AuthorizationResult> authorize(
            AuthorizableRequestContext requestContext,
            List<Action> actions) {
        List<AuthorizationResult> results = new ArrayList<>(actions.size());
        AuthorizerData curData = data;
        for (Action action : actions) {
            AuthorizationResult result = curData.authorize(requestContext, action);
            results.add(result);
        }
        return results;
    }

    @Override
    public Iterable<AclBinding> acls(AclBindingFilter filter) {
        // The Iterable returned here is consistent because it is created over a read-only
        // copy of ACLs data.
        return data.acls(filter);
    }

    @Override
    public int aclCount() {
        return data.aclCount();
    }

    @Override
    public void close() throws IOException {
        // Complete the initialLoadFuture, if it hasn't been completed already.
        initialLoadFuture.completeExceptionally(new TimeoutException("The authorizer was " +
            "closed before the initial load could complete."));
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Set<String> superUsers = getConfiguredSuperUsers(configs);
        AuthorizationResult defaultResult = getDefaultResult(configs);
        int nodeId;
        try {
            nodeId = Integer.parseInt(configs.get("node.id").toString().trim());
        } catch (Exception e) {
            nodeId = -1;
        }
        data = data.copyWithNewConfig(nodeId, superUsers, defaultResult);
        this.data.log().info("set super.users={}, default result={}", String.join(",", superUsers), defaultResult);
    }

    AuthorizationResult defaultResult() {
        return data.defaultResult();
    }

    /**
     * Get the set of super users as specificed in a configuration map.
     * @param configs the configuration map.
     * @return the set of super users..
     */

    static Set<String> getConfiguredSuperUsers(Map<String, ?> configs) {
        Object configValue = configs.get(SUPER_USERS_CONFIG);
        if (configValue == null) return Collections.emptySet();
        String[] values = configValue.toString().split(";");
        Set<String> result = new HashSet<>();
        for (String value : values) {
            String v = value.trim();
            if (!v.isEmpty()) {
                SecurityUtils.parseKafkaPrincipal(v);
                result.add(v);
            }
        }
        return result;
    }

    /**
     * Get the specified default Result from a configuration map.
     * @param configs the configuration map.
     * @return the default AuthorizationResult.
     */
    static AuthorizationResult getDefaultResult(Map<String, ?> configs) {
        Object configValue = configs.get(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG);
        if (configValue == null) return DENIED;
        return Boolean.parseBoolean(configValue.toString().trim()) ? ALLOWED : DENIED;
    }

    @Override
    public AuthorizationResult authorizeByResourceType(AuthorizableRequestContext requestContext, AclOperation operation, ResourceType resourceType) {
        SecurityUtils.authorizeByResourceTypeCheckArgs(operation, resourceType);
        // super users are granted access regardless of DENY ACLs.
        if (data.superUsers().contains(requestContext.principal().toString())) {
            return AuthorizationResult.ALLOWED;
        }
        if (data.defaultResult() == ALLOWED) {
            // we only have to check if there is a literal prohibition on resource "*"
            ResourcePattern pattern = new ResourcePattern(resourceType, "*", LITERAL);
            Action action = new Action(operation, pattern, 1, false, false);

            return authorize(requestContext, Collections.singletonList(action)).get(0);
        }

        String host = requestContext.clientAddress().getHostAddress();
        return this.data.authorizeByResourceType(requestContext.principal(), host, operation, resourceType);
    }
}
