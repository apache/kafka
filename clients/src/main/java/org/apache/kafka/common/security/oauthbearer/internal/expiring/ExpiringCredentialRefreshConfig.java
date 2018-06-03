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
package org.apache.kafka.common.security.oauthbearer.internal.expiring;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.SaslConfigs;

/**
 * Immutable refresh-related configuration for expiring credentials that can be
 * parsed from a producer/consumer/broker config.
 */
public class ExpiringCredentialRefreshConfig {
    private final double loginRefreshWindowFactor;
    private final double loginRefreshWindowJitter;
    private final short loginRefreshMinPeriodSeconds;
    private final short loginRefreshBufferSeconds;
    private final boolean loginRefreshReloginAllowedBeforeLogout;

    /**
     * Constructor based on producer/consumer/broker configs and the indicated value
     * for whether or not client relogin is allowed before logout
     * 
     * @param configs
     *            the mandatory (but possibly empty) producer/consumer/broker
     *            configs upon which to build this instance
     * @param clientReloginAllowedBeforeLogout
     *            if the {@code LoginModule} and {@code SaslClient} implementations
     *            support multiple simultaneous login contexts on a single
     *            {@code Subject} at the same time. If true, then upon refresh,
     *            logout will only be invoked on the original {@code LoginContext}
     *            after a new one successfully logs in. This can be helpful if the
     *            original credential still has some lifetime left when an attempt
     *            to refresh the credential fails; the client will still be able to
     *            create new connections as long as the original credential remains
     *            valid. Otherwise, if logout is immediately invoked prior to
     *            relogin, a relogin failure leaves the client without the ability
     *            to connect until relogin does in fact succeed.
     */
    public ExpiringCredentialRefreshConfig(Map<String, ?> configs, boolean clientReloginAllowedBeforeLogout) {
        Objects.requireNonNull(configs);
        this.loginRefreshWindowFactor = (Double) configs.get(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR);
        this.loginRefreshWindowJitter = (Double) configs.get(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER);
        this.loginRefreshMinPeriodSeconds = (Short) configs.get(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS);
        this.loginRefreshBufferSeconds = (Short) configs.get(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS);
        this.loginRefreshReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout;
    }

    /**
     * Background login refresh thread will sleep until the specified window factor
     * relative to the credential's total lifetime has been reached, at which time
     * it will try to refresh the credential.
     * 
     * @return the login refresh window factor
     */
    public double loginRefreshWindowFactor() {
        return loginRefreshWindowFactor;
    }

    /**
     * Amount of random jitter added to the background login refresh thread's sleep
     * time.
     * 
     * @return the login refresh window jitter
     */
    public double loginRefreshWindowJitter() {
        return loginRefreshWindowJitter;
    }

    /**
     * The desired minimum time between checks by the background login refresh
     * thread, in seconds
     * 
     * @return the desired minimum refresh period, in seconds
     */
    public short loginRefreshMinPeriodSeconds() {
        return loginRefreshMinPeriodSeconds;
    }

    /**
     * The amount of buffer time before expiration to maintain when refreshing. If a
     * refresh is scheduled to occur closer to expiration than the number of seconds
     * defined here then the refresh will be moved up to maintain as much of the
     * desired buffer as possible.
     * 
     * @return the refresh buffer, in seconds
     */
    public short loginRefreshBufferSeconds() {
        return loginRefreshBufferSeconds;
    }

    /**
     * If the LoginModule and SaslClient implementations support multiple
     * simultaneous login contexts on a single Subject at the same time. If true,
     * then upon refresh, logout will only be invoked on the original LoginContext
     * after a new one successfully logs in. This can be helpful if the original
     * credential still has some lifetime left when an attempt to refresh the
     * credential fails; the client will still be able to create new connections as
     * long as the original credential remains valid. Otherwise, if logout is
     * immediately invoked prior to relogin, a relogin failure leaves the client
     * without the ability to connect until relogin does in fact succeed.
     * 
     * @return true if relogin is allowed prior to discarding an existing
     *         (presumably unexpired) credential, otherwise false
     */
    public boolean loginRefreshReloginAllowedBeforeLogout() {
        return loginRefreshReloginAllowedBeforeLogout;
    }
}
