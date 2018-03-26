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
package org.apache.kafka.common.security.authenticator;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.kerberos.KerberosLogin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoginManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoginManager.class);

    // static configs (broker or client)
    private static final Map<String, LoginManager> STATIC_INSTANCES = new HashMap<>();

    // dynamic configs (client-only)
    private static final Map<Password, LoginManager> DYNAMIC_INSTANCES = new HashMap<>();

    private final Login login;
    private final Object cacheKey;
    private int refCount;

    private LoginManager(JaasContext jaasContext, boolean hasKerberos, Map<String, ?> configs,
                         Object cacheKey) throws IOException, LoginException {
        this.cacheKey = cacheKey;
        login = hasKerberos ? new KerberosLogin() : new DefaultLogin();
        login.configure(configs, jaasContext);
        login.login();
    }

    /**
     * Returns an instance of `LoginManager` and increases its reference count.
     *
     * `release()` should be invoked when the `LoginManager` is no longer needed. This method will try to reuse an
     * existing `LoginManager` for the provided context type. If `jaasContext` was loaded from a dynamic config,
     * login managers are reused for the same dynamic config value. For `jaasContext` loaded from static JAAS
     * configuration, login managers are reused for static contexts with the same login context name.
     *
     * This is a bit ugly and it would be nicer if we could pass the `LoginManager` to `ChannelBuilders.create` and
     * shut it down when the broker or clients are closed. It's straightforward to do the former, but it's more
     * complicated to do the latter without making the consumer API more complex.
     *
     * @param jaasContext Static or dynamic JAAS context. `jaasContext.dynamicJaasConfig()` is non-null for dynamic context.
     *                    For static contexts, this may contain multiple login modules if the context type is SERVER.
     *                    For CLIENT static contexts and dynamic contexts of CLIENT and SERVER, 'jaasContext` contains
     *                    only one login module.
     * @param saslMechanism SASL mechanism for which login manager is being acquired. For dynamic contexts, the single
     *                      login module in `jaasContext` corresponds to this SASL mechanism. Hence `Login` class is
     *                      chosen based on this mechanism.
     * @param hasKerberos Boolean flag that indicates if Kerberos is enabled for the server listener or client. Since
     *                    static broker configuration may contain multiple login modules in a login context, KerberosLogin
     *                    must be used if Kerberos is enabled on the listener, even if `saslMechanism` is not GSSAPI.
     * @param configs Config options used to configure `Login` if a new login manager is created.
     *
     */
    public static LoginManager acquireLoginManager(JaasContext jaasContext, String saslMechanism, boolean hasKerberos,
                                                   Map<String, ?> configs) throws IOException, LoginException {
        synchronized (LoginManager.class) {
            LoginManager loginManager;
            Password jaasConfigValue = jaasContext.dynamicJaasConfig();
            if (jaasConfigValue != null) {
                loginManager = DYNAMIC_INSTANCES.get(jaasConfigValue);
                if (loginManager == null) {
                    loginManager = new LoginManager(jaasContext, saslMechanism.equals(SaslConfigs.GSSAPI_MECHANISM), configs, jaasConfigValue);
                    DYNAMIC_INSTANCES.put(jaasConfigValue, loginManager);
                }
            } else {
                loginManager = STATIC_INSTANCES.get(jaasContext.name());
                if (loginManager == null) {
                    loginManager = new LoginManager(jaasContext, hasKerberos, configs, jaasContext.name());
                    STATIC_INSTANCES.put(jaasContext.name(), loginManager);
                }
            }
            return loginManager.acquire();
        }
    }

    public Subject subject() {
        return login.subject();
    }

    public String serviceName() {
        return login.serviceName();
    }

    // Only for testing
    Object cacheKey() {
        return cacheKey;
    }

    private LoginManager acquire() {
        ++refCount;
        LOGGER.trace("{} acquired", this);
        return this;
    }

    /**
     * Decrease the reference count for this instance and release resources if it reaches 0.
     */
    public void release() {
        synchronized (LoginManager.class) {
            if (refCount == 0)
                throw new IllegalStateException("release() called on disposed " + this);
            else if (refCount == 1) {
                if (cacheKey instanceof Password) {
                    DYNAMIC_INSTANCES.remove(cacheKey);
                } else {
                    STATIC_INSTANCES.remove(cacheKey);
                }
                login.close();
            }
            --refCount;
            LOGGER.trace("{} released", this);
        }
    }

    @Override
    public String toString() {
        return "LoginManager(serviceName=" + serviceName() +
                // subject.toString() exposes private credentials, so we can't use it
                ", publicCredentials=" + subject().getPublicCredentials() +
                ", refCount=" + refCount + ')';
    }

    /* Should only be used in tests. */
    public static void closeAll() {
        synchronized (LoginManager.class) {
            for (String key : new ArrayList<>(STATIC_INSTANCES.keySet()))
                STATIC_INSTANCES.remove(key).login.close();
            for (Password key : new ArrayList<>(DYNAMIC_INSTANCES.keySet()))
                DYNAMIC_INSTANCES.remove(key).login.close();
        }
    }
}
