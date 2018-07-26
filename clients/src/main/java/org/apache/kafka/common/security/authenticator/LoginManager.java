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
import java.util.Objects;


import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.Login;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoginManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoginManager.class);

    // static configs (broker or client)
    private static final Map<LoginMetadata<String>, LoginManager> STATIC_INSTANCES = new HashMap<>();

    // dynamic configs (broker or client)
    private static final Map<LoginMetadata<Password>, LoginManager> DYNAMIC_INSTANCES = new HashMap<>();

    private final Login login;
    private final LoginMetadata<?> loginMetadata;
    private final AuthenticateCallbackHandler loginCallbackHandler;
    private int refCount;

    private LoginManager(JaasContext jaasContext, String saslMechanism, Map<String, ?> configs,
                         LoginMetadata<?> loginMetadata) throws IOException, LoginException {
        this.loginMetadata = loginMetadata;
        this.login = Utils.newInstance(loginMetadata.loginClass);
        loginCallbackHandler = Utils.newInstance(loginMetadata.loginCallbackClass);
        loginCallbackHandler.configure(configs, saslMechanism, jaasContext.configurationEntries());
        login.configure(configs, jaasContext.name(), jaasContext.configuration(), loginCallbackHandler);
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
     * @param defaultLoginClass Default login class to use if an override is not specified in `configs`
     * @param configs Config options used to configure `Login` if a new login manager is created.
     *
     */
    public static LoginManager acquireLoginManager(JaasContext jaasContext, String saslMechanism,
                                                   Class<? extends Login> defaultLoginClass,
                                                   Map<String, ?> configs) throws IOException, LoginException {
        Class<? extends Login> loginClass = configuredClassOrDefault(configs, jaasContext,
                saslMechanism, SaslConfigs.SASL_LOGIN_CLASS, defaultLoginClass);
        Class<? extends AuthenticateCallbackHandler> loginCallbackClass = configuredClassOrDefault(configs,
                jaasContext, saslMechanism, SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
                AbstractLogin.DefaultLoginCallbackHandler.class);
        synchronized (LoginManager.class) {
            LoginManager loginManager;
            Password jaasConfigValue = jaasContext.dynamicJaasConfig();
            if (jaasConfigValue != null) {
                LoginMetadata<Password> loginMetadata = new LoginMetadata<>(jaasConfigValue, loginClass, loginCallbackClass);
                loginManager = DYNAMIC_INSTANCES.get(loginMetadata);
                if (loginManager == null) {
                    loginManager = new LoginManager(jaasContext, saslMechanism, configs, loginMetadata);
                    DYNAMIC_INSTANCES.put(loginMetadata, loginManager);
                }
            } else {
                LoginMetadata<String> loginMetadata = new LoginMetadata<>(jaasContext.name(), loginClass, loginCallbackClass);
                loginManager = STATIC_INSTANCES.get(loginMetadata);
                if (loginManager == null) {
                    loginManager = new LoginManager(jaasContext, saslMechanism, configs, loginMetadata);
                    STATIC_INSTANCES.put(loginMetadata, loginManager);
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
        return loginMetadata.configInfo;
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
                if (loginMetadata.configInfo instanceof Password) {
                    DYNAMIC_INSTANCES.remove(loginMetadata);
                } else {
                    STATIC_INSTANCES.remove(loginMetadata);
                }
                login.close();
                loginCallbackHandler.close();
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
            for (LoginMetadata<String> key : new ArrayList<>(STATIC_INSTANCES.keySet()))
                STATIC_INSTANCES.remove(key).login.close();
            for (LoginMetadata<Password> key : new ArrayList<>(DYNAMIC_INSTANCES.keySet()))
                DYNAMIC_INSTANCES.remove(key).login.close();
        }
    }

    private static <T> Class<? extends T> configuredClassOrDefault(Map<String, ?> configs,
                                                     JaasContext jaasContext,
                                                     String saslMechanism,
                                                     String configName,
                                                     Class<? extends T> defaultClass) {
        String prefix  = jaasContext.type() == JaasContext.Type.SERVER ? ListenerName.saslMechanismPrefix(saslMechanism) : "";
        Class<? extends T> clazz = (Class<? extends T>) configs.get(prefix + configName);
        if (clazz != null && jaasContext.configurationEntries().size() != 1) {
            String errorMessage = configName + " cannot be specified with multiple login modules in the JAAS context. " +
                    SaslConfigs.SASL_JAAS_CONFIG + " must be configured to override mechanism-specific configs.";
            throw new ConfigException(errorMessage);
        }
        if (clazz == null)
            clazz = defaultClass;
        return clazz;
    }

    private static class LoginMetadata<T> {
        final T configInfo;
        final Class<? extends Login> loginClass;
        final Class<? extends AuthenticateCallbackHandler> loginCallbackClass;

        LoginMetadata(T configInfo, Class<? extends Login> loginClass,
                      Class<? extends AuthenticateCallbackHandler> loginCallbackClass) {
            this.configInfo = configInfo;
            this.loginClass = loginClass;
            this.loginCallbackClass = loginCallbackClass;
        }

        @Override
        public int hashCode() {
            return Objects.hash(configInfo, loginClass, loginCallbackClass);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LoginMetadata<?> loginMetadata = (LoginMetadata<?>) o;
            return Objects.equals(configInfo, loginMetadata.configInfo) &&
                   Objects.equals(loginClass, loginMetadata.loginClass) &&
                   Objects.equals(loginCallbackClass, loginMetadata.loginCallbackClass);
        }
    }
}
