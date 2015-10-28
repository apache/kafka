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

package org.apache.kafka.common.security.kerberos;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Map;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.network.LoginType;
import org.apache.kafka.common.security.JaasUtils;

public class LoginManager {

    private static final EnumMap<LoginType, LoginManager> CACHED_INSTANCES = new EnumMap(LoginType.class);

    private final Login login;
    private final String serviceName;
    private final LoginType loginType;
    private int refCount;

    private LoginManager(LoginType loginType, Map<String, ?> configs) throws IOException, LoginException {
        this.loginType = loginType;
        String loginContext = loginType.contextName();
        login = new Login(loginContext, configs);
        this.serviceName = getServiceName(loginContext, configs);
        login.startThreadIfNeeded();
    }

    private static String getServiceName(String loginContext, Map<String, ?> configs) throws IOException {
        String jaasServiceName = JaasUtils.jaasConfig(loginContext, JaasUtils.SERVICE_NAME);
        String configServiceName = (String) configs.get(SaslConfigs.SASL_KERBEROS_SERVICE_NAME);
        if (jaasServiceName != null && configServiceName != null && jaasServiceName != configServiceName) {
            String message = "Conflicting serviceName values found in JAAS and Kafka configs " +
                "value in JAAS file " + jaasServiceName + ", value in Kafka config " + configServiceName;
            throw new IllegalArgumentException(message);
        }

        if (jaasServiceName != null)
            return jaasServiceName;
        if (configServiceName != null)
            return configServiceName;

        throw new IllegalArgumentException("No serviceName defined in either JAAS or Kafka config");
    }

    /**
     * Returns an instance of `LoginManager` and increases its reference count.
     *
     * `release()` should be invoked when the `LoginManager` is no longer needed. This method will try to reuse an
     * existing `LoginManager` for the provided `mode` if available. However, it expects `configs` to be the same for
     * every invocation and it will ignore them in the case where it's returning a cached instance of `LoginManager`.
     *
     * This is a bit ugly and it would be nicer if we could pass the `LoginManager` to `ChannelBuilders.create` and
     * shut it down when the broker or clients are closed. It's straightforward to do the former, but it's more
     * complicated to do the latter without making the consumer API more complex.
     *
     * @param loginType the type of the login context, it should be SERVER for the broker and CLIENT for the clients
     *                  (i.e. consumer and producer)
     * @param configs configuration as key/value pairs
     */
    public static final LoginManager acquireLoginManager(LoginType loginType, Map<String, ?> configs) throws IOException, LoginException {
        synchronized (LoginManager.class) {
            LoginManager loginManager = CACHED_INSTANCES.get(loginType);
            if (loginManager == null) {
                loginManager = new LoginManager(loginType, configs);
                CACHED_INSTANCES.put(loginType, loginManager);
            }
            return loginManager.acquire();
        }
    }

    public Subject subject() {
        return login.subject();
    }

    public String serviceName() {
        return serviceName;
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
                CACHED_INSTANCES.remove(loginType);
                login.shutdown();
            }
            --refCount;
        }
    }

    /* Should only be used in tests. */
    public static void closeAll() {
        synchronized (LoginManager.class) {
            for (LoginType loginType : new ArrayList<>(CACHED_INSTANCES.keySet())) {
                LoginManager loginManager = CACHED_INSTANCES.remove(loginType);
                loginManager.login.shutdown();
            }
        }
    }

}
