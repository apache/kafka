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
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.TimeoutException;
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

import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;


/**
 * The standard authorizer which is used in KRaft-based clusters if no other authorizer is
 * configured.
 */
public class StandardAuthorizer implements ClusterMetadataAuthorizer {
    public final static String SUPER_USERS_CONFIG = "super.users";

    public final static String ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG = "allow.everyone.if.no.acl.found";

    /**
     * A future which is completed once we have loaded a snapshot.
     * TODO: KAFKA-13649: StandardAuthorizer should not finish loading until it reads up to the high water mark.
     */
    private final CompletableFuture<Void> initialLoadFuture = CompletableFuture.completedFuture(null);

    /**
     * The current data. Can be read without a lock. Must be written while holding the object lock.
     */
    private volatile StandardAuthorizerData data = StandardAuthorizerData.createEmpty();

    @Override
    public synchronized void setAclMutator(AclMutator aclMutator) {
        this.data = data.copyWithNewAclMutator(aclMutator);
    }

    @Override
    public AclMutator aclMutatorOrException() {
        AclMutator aclMutator = data.aclMutator;
        if (aclMutator == null) {
            throw new NotControllerException("The current node is not the active controller.");
        }
        return aclMutator;
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
    public synchronized void loadSnapshot(Map<Uuid, StandardAcl> acls) {
        data = data.copyWithNewAcls(acls.entrySet());
    }

    @Override
    public Map<Endpoint, ? extends CompletionStage<Void>> start(
            AuthorizerServerInfo serverInfo) {
        Map<Endpoint, CompletableFuture<Void>> result = new HashMap<>();
        for (Endpoint endpoint : serverInfo.endpoints()) {
            result.put(endpoint, initialLoadFuture);
        }
        return result;
    }

    @Override
    public List<AuthorizationResult> authorize(
            AuthorizableRequestContext requestContext,
            List<Action> actions) {
        StandardAuthorizerData curData = data;
        List<AuthorizationResult> results = new ArrayList<>(actions.size());
        for (Action action: actions) {
            AuthorizationResult result = curData.authorize(requestContext, action);
            results.add(result);
        }
        return results;
    }

    @Override
    public Iterable<AclBinding> acls(AclBindingFilter filter) {
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
        // Nothing else to do here.
    }

    @Override
    public synchronized void configure(Map<String, ?> configs) {
        Set<String> superUsers = getConfiguredSuperUsers(configs);
        AuthorizationResult defaultResult = getDefaultResult(configs);
        int nodeId;
        try {
            nodeId = Integer.parseInt(configs.get("node.id").toString());
        } catch (Exception e) {
            nodeId = -1;
        }
        this.data = data.copyWithNewConfig(nodeId, superUsers, defaultResult);
        this.data.log.info("set super.users=" + String.join(",", superUsers) +
            ", default result=" + defaultResult);
    }

    // VisibleForTesting
    Set<String> superUsers()  {
        return new HashSet<>(data.superUsers());
    }

    AuthorizationResult defaultResult() {
        return data.defaultResult();
    }

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

    static AuthorizationResult getDefaultResult(Map<String, ?> configs) {
        Object configValue = configs.get(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG);
        if (configValue == null) return DENIED;
        return Boolean.valueOf(configValue.toString()) ? ALLOWED : DENIED;
    }
}
