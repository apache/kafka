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
import java.util.Map;

import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.JaasUtils;

public class LoginManager {

    private static LoginManager serverInstance;
    private static LoginManager clientInstance;

    private final Login login;
    private final String serviceName;
    private final Mode mode;
    private int refCount;

    private LoginManager(Mode mode, Map<String, ?> configs) throws IOException, LoginException {
        String loginContext;
        if (mode == Mode.SERVER)
            loginContext = JaasUtils.LOGIN_CONTEXT_SERVER;
        else
            loginContext = JaasUtils.LOGIN_CONTEXT_CLIENT;
        this.mode = mode;
        this.serviceName = JaasUtils.jaasConfig(loginContext, JaasUtils.SERVICE_NAME);
        login = new Login(loginContext, configs);
        login.startThreadIfNeeded();
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
     * complicated to do the latter without making the consumer API a bit more complex.
     */
    public static final LoginManager acquireLoginManager(Mode mode, Map<String, ?> configs) throws IOException, LoginException {
        synchronized (LoginManager.class) {
            switch (mode) {
                case SERVER:
                    if (serverInstance == null)
                        serverInstance = new LoginManager(mode, configs);
                    return serverInstance.acquire();
                case CLIENT:
                    if (clientInstance == null)
                        clientInstance = new LoginManager(mode, configs);
                    return clientInstance.acquire();
                default:
                    throw new IllegalArgumentException("Unsupported `mode` received " + mode);
            }
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
                if (mode == Mode.SERVER)
                    serverInstance = null;
                else if (mode == Mode.CLIENT)
                    clientInstance = null;
                else
                    throw new IllegalStateException("Unsupported `mode` " + mode);

                login.shutdown();
            }
            --refCount;
        }
    }
}
