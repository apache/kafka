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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Configurable;


public class LoginManager implements Configurable {
    private Login login;
    private final String serviceName;
    private final String loginContext;
    private final Mode mode;
    volatile private static LoginManager instance;

    private LoginManager(Mode mode) throws IOException, LoginException {
        this.mode = mode;
        if (mode == Mode.SERVER)
            this.loginContext = JaasUtils.LOGIN_CONTEXT_SERVER;
        else
            this.loginContext = JaasUtils.LOGIN_CONTEXT_CLIENT;
        this.serviceName = JaasUtils.jaasConfig(loginContext, JaasUtils.SERVICE_NAME);
        login = new Login(loginContext);
        login.startThreadIfNeeded();
    }

    public static final LoginManager getLoginManager(Mode mode) throws IOException, LoginException {
        if (instance != null) {
            return instance;
        } else {
            synchronized (LoginManager.class)  {
                if (instance == null)
                    instance = new LoginManager(mode);
            }
        }
        return instance;
    }

    @Override
    public void configure(Map<String, ?> configs) throws KafkaException {
    }

    public Subject subject() {
        return login.subject();
    }

    public String serviceName() {
        return serviceName;
    }

    public void close() {
        login.shutdown();
    }
}
