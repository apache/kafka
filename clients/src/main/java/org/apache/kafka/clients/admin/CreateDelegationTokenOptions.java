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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Delegation tokens provide a lightweight authentication mechanism for Kafka clients. They are
 * useful when distributing Kerberos TGTs to a large number of clients is impractical.
 * <p>
 * If no {@link #renewers(List) renewers} are specified, only the token owner (or a broker's
 * configured delegation token management principal) can renew or expire the token. If no
 * {@link #owner(org.apache.kafka.common.security.auth.KafkaPrincipal) owner} is set, the
 * authenticated caller becomes the token owner.
 *
 * @see Admin#createDelegationToken(CreateDelegationTokenOptions)
 */
public class CreateDelegationTokenOptions extends AbstractOptions<CreateDelegationTokenOptions> {
    private long maxLifetimeMs = -1;
    private List<KafkaPrincipal> renewers =  new LinkedList<>();
    private KafkaPrincipal owner = null;

    /**
     * Set the list of principals that are allowed to renew this delegation token. If not set,
     * only the token owner and the broker's delegation token management principal can renew
     * or expire the token.
     *
     * @param renewers The list of principals that can renew the token.
     */
    public CreateDelegationTokenOptions renewers(List<KafkaPrincipal> renewers) {
        this.renewers = renewers;
        return this;
    }

    /**
     * The list of principals that can renew the delegation token.
     */
    public List<KafkaPrincipal> renewers() {
        return renewers;
    }

    /**
     * Set the owner of the delegation token. If not set, the authenticated caller of
     * {@link Admin#createDelegationToken(CreateDelegationTokenOptions)} becomes the token owner.
     * Setting a different owner allows a superuser to create tokens on behalf of other principals.
     *
     * @param owner The owner principal.
     */
    public CreateDelegationTokenOptions owner(KafkaPrincipal owner) {
        this.owner = owner;
        return this;
    }

    /**
     * The owner of the delegation token, or empty if not set.
     */
    public Optional<KafkaPrincipal> owner() {
        return Optional.ofNullable(owner);
    }

    /**
     * @deprecated Since 4.0 and should not be used any longer. Please use {@link #maxLifetimeMs(long)} instead.
     */
    @Deprecated
    public CreateDelegationTokenOptions maxlifeTimeMs(long maxLifetimeMs) {
        this.maxLifetimeMs = maxLifetimeMs;
        return this;
    }

    /**
     * Set the maximum lifetime in milliseconds for the delegation token. If -1, the default
     * server-side maximum lifetime will be used, which is 7 days.
     *
     * @param maxLifetimeMs The maximum lifetime in milliseconds.
     */
    public CreateDelegationTokenOptions maxLifetimeMs(long maxLifetimeMs) {
        this.maxLifetimeMs = maxLifetimeMs;
        return this;
    }

    /**
     * @deprecated Since 4.0 and should not be used any longer. Please use {@link #maxLifetimeMs()} instead.
     */
    @Deprecated
    public long maxlifeTimeMs() {
        return maxLifetimeMs;
    }

    /**
     * Return the maximum lifetime in milliseconds for the delegation token. A value of {@code -1}
     * means the server-side default ({@code delegation.token.max.lifetime.ms}) will be used, 
     * which is 7 days by default.
     */
    public long maxLifetimeMs() {
        return maxLifetimeMs;
    }
}
