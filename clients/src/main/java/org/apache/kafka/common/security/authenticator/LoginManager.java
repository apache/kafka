/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.authenticator;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.LoginType;
import org.apache.kafka.common.security.auth.Login;
import org.apache.kafka.common.security.kerberos.KerberosLogin;

public class LoginManager {

    // static configs (broker or client)
    private static final EnumMap<LoginType, LoginManager> LOGIN_TYPE_INSTANCES = new EnumMap<>(LoginType.class);

    // dynamic configs (client-only)
    private static final Map<Password, LoginManager> JAAS_CONF_INSTANCES = new HashMap<>();

    private final Login login;
    private final Object cacheKey;
    private int refCount;

    private LoginManager(LoginType loginType, boolean hasKerberos, Map<String, ?> configs, Configuration jaasConfig,
                         Password jaasConfigValue) throws IOException, LoginException {
        this.cacheKey = jaasConfigValue != null ? jaasConfigValue : loginType;
        String loginContext = loginType.contextName();
        login = hasKerberos ? new KerberosLogin() : new DefaultLogin();
        login.configure(configs, jaasConfig, loginContext);
        login.login();
    }

    /**
     * Returns an instance of `LoginManager` and increases its reference count.
     *
     * `release()` should be invoked when the `LoginManager` is no longer needed. This method will try to reuse an
     * existing `LoginManager` for the provided `loginType` and `SaslConfigs.SASL_JAAS_CONFIG` in `configs`, if available. 
     *
     * This is a bit ugly and it would be nicer if we could pass the `LoginManager` to `ChannelBuilders.create` and
     * shut it down when the broker or clients are closed. It's straightforward to do the former, but it's more
     * complicated to do the latter without making the consumer API more complex.
     *
     * @param loginType the type of the login context, it should be SERVER for the broker and CLIENT for the clients
     *                  (i.e. consumer and producer)
     * @param hasKerberos 
     * @param configs configuration as key/value pairs
     * @param jaasConfig JAAS Configuration object
     */
    public static LoginManager acquireLoginManager(LoginType loginType, boolean hasKerberos, Map<String, ?> configs,
                                                   Configuration jaasConfig) throws IOException, LoginException {
        synchronized (LoginManager.class) {
            // SASL_JAAS_CONFIG is only supported by clients
            LoginManager loginManager;
            Password jaasConfigValue = (Password) configs.get(SaslConfigs.SASL_JAAS_CONFIG);
            if (loginType == LoginType.CLIENT && jaasConfigValue != null) {
                loginManager = JAAS_CONF_INSTANCES.get(jaasConfigValue);
                if (loginManager == null) {
                    loginManager = new LoginManager(loginType, hasKerberos, configs, jaasConfig, jaasConfigValue);
                    JAAS_CONF_INSTANCES.put(jaasConfigValue, loginManager);
                }
            } else {
                loginManager = LOGIN_TYPE_INSTANCES.get(loginType);
                if (loginManager == null) {
                    loginManager = new LoginManager(loginType, hasKerberos, configs, jaasConfig, jaasConfigValue);
                    LOGIN_TYPE_INSTANCES.put(loginType, loginManager);
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

    private LoginManager acquire() {
        ++refCount;
        return this;
    }

    /**
     * Decrease the reference count for this instance and release resources if it reaches 0.
     */
    public void release() {
        synchronized (LoginManager.class) {
            if (refCount == 0)
                throw new IllegalStateException("release called on LoginManager with refCount == 0");
            else if (refCount == 1) {
                if (cacheKey instanceof Password) {
                    JAAS_CONF_INSTANCES.remove(cacheKey);
                } else {
                    LOGIN_TYPE_INSTANCES.remove(cacheKey);
                }
                login.close();
            }
            --refCount;
        }
    }

    /* Should only be used in tests. */
    public static void closeAll() {
        synchronized (LoginManager.class) {
            for (LoginType key : new ArrayList<>(LOGIN_TYPE_INSTANCES.keySet()))
                LOGIN_TYPE_INSTANCES.remove(key).login.close();
            for (Password key : new ArrayList<>(JAAS_CONF_INSTANCES.keySet()))
                JAAS_CONF_INSTANCES.remove(key).login.close();
        }
    }
}
