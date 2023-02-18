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
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
     * A future which is completed once we have loaded up to the initial high water mark.
     */
    private final CompletableFuture<Void> initialLoadFuture = new CompletableFuture<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * The current data. We use a read-write lock to synchronize reads and writes to the data. We
     * expect one writer and multiple readers accessing the ACL data, and we use the lock to make
     * sure we have consistent reads when writer tries to change the data.
     */
    private volatile StandardAuthorizerData data = StandardAuthorizerData.createEmpty();

    @Override
    public void setAclMutator(AclMutator aclMutator) {
        lock.writeLock().lock();
        try {
            this.data = data.copyWithNewAclMutator(aclMutator);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public AclMutator aclMutatorOrException() {
        AclMutator aclMutator;
        lock.readLock().lock();
        try {
            aclMutator = data.aclMutator;
        } finally {
            lock.readLock().unlock();
        }
        if (aclMutator == null) {
            throw new NotControllerException("The current node is not the active controller.");
        }
        return aclMutator;
    }

    @Override
    public void completeInitialLoad() {
        lock.writeLock().lock();
        try {
            data = data.copyWithNewLoadingComplete(true);
        } finally {
            lock.writeLock().unlock();
        }
        data.log.info("Completed initial ACL load process.");
        initialLoadFuture.complete(null);
    }

    // Visible for testing
    public CompletableFuture<Void> initialLoadFuture() {
        return initialLoadFuture;
    }

    @Override
    public void completeInitialLoad(Exception e) {
        data.log.error("Failed to complete initial ACL load process.", e);
        initialLoadFuture.completeExceptionally(e);
    }

    @Override
    public void addAcl(Uuid id, StandardAcl acl) {
        lock.writeLock().lock();
        try {
            data.addAcl(id, acl);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void removeAcl(Uuid id) {
        lock.writeLock().lock();
        try {
            data.removeAcl(id);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void loadSnapshot(Map<Uuid, StandardAcl> acls) {
        StandardAuthorizerData newData = StandardAuthorizerData.createEmpty();
        for (Map.Entry<Uuid, StandardAcl> entry : acls.entrySet()) {
            newData.addAcl(entry.getKey(), entry.getValue());
        }
        lock.writeLock().lock();
        try {
            data = data.copyWithNewAcls(newData.getAclsByResource(), newData.getAclsById());
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Map<Endpoint, ? extends CompletionStage<Void>> start(
            AuthorizerServerInfo serverInfo) {
        Map<Endpoint, CompletableFuture<Void>> result = new HashMap<>();
        for (Endpoint endpoint : serverInfo.endpoints()) {
            if (serverInfo.earlyStartListeners().contains(
                    endpoint.listenerName().orElseGet(() -> ""))) {
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
        lock.readLock().lock();
        try {
            StandardAuthorizerData curData = data;
            for (Action action : actions) {
                AuthorizationResult result = curData.authorize(requestContext, action);
                results.add(result);
            }
        } finally {
            lock.readLock().unlock();
        }
        return results;
    }

    @Override
    public Iterable<AclBinding> acls(AclBindingFilter filter) {
        lock.readLock().lock();
        try {
            // The Iterable returned here is consistent because it is created over a read-only
            // copy of ACLs data.
            return data.acls(filter);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int aclCount() {
        lock.readLock().lock();
        try {
            return data.aclCount();
        } finally {
            lock.readLock().unlock();
        }
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
            nodeId = Integer.parseInt(configs.get("node.id").toString());
        } catch (Exception e) {
            nodeId = -1;
        }
        lock.writeLock().lock();
        try {
            data = data.copyWithNewConfig(nodeId, superUsers, defaultResult);
        } finally {
            lock.writeLock().unlock();
        }
        this.data.log.info("set super.users={}, default result={}", String.join(",", superUsers), defaultResult);
    }

    // VisibleForTesting
    Set<String> superUsers()  {
        lock.readLock().lock();
        try {
            return new HashSet<>(data.superUsers());
        } finally {
            lock.readLock().unlock();
        }
    }

    AuthorizationResult defaultResult() {
        lock.readLock().lock();
        try {
            return data.defaultResult();
        } finally {
            lock.readLock().unlock();
        }
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
