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

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.RealmCallback;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.Subject;

import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.auth.Login;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Base login class that implements methods common to typical SASL mechanisms.
 */
public abstract class AbstractLogin implements Login {
    private static final Logger log = LoggerFactory.getLogger(AbstractLogin.class);

    private String loginContextName;
    private LoginContext loginContext;


    @Override
    public void configure(Map<String, ?> configs, String loginContextName) {
        this.loginContextName = loginContextName;
    }

    @Override
    public LoginContext login() throws LoginException {
        String jaasConfigFile = System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
        if (jaasConfigFile == null) {
            log.debug("System property '" + JaasUtils.JAVA_LOGIN_CONFIG_PARAM + "' is not set, using default JAAS configuration.");
        }
        AppConfigurationEntry[] configEntries = Configuration.getConfiguration().getAppConfigurationEntry(loginContextName);
        if (configEntries == null) {
            String errorMessage = "Could not find a '" + loginContextName + "' entry in the JAAS configuration. System property '" +
                JaasUtils.JAVA_LOGIN_CONFIG_PARAM + "' is " + (jaasConfigFile == null ? "not set" : jaasConfigFile);
            throw new IllegalArgumentException(errorMessage);
        }

        loginContext = new LoginContext(loginContextName, new LoginCallbackHandler());
        loginContext.login();
        log.info("Successfully logged in.");
        return loginContext;
    }

    @Override
    public Subject subject() {
        return loginContext.getSubject();
    }

    /**
     * Callback handler for creating login context. Login callback handlers
     * should support the callbacks required for the login modules used by
     * the KafkaServer and KafkaClient contexts. Kafka does not support
     * callback handlers which require additional user input.
     *
     */
    public static class LoginCallbackHandler implements CallbackHandler {

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nc = (NameCallback) callback;
                    nc.setName(nc.getDefaultName());
                } else if (callback instanceof PasswordCallback) {
                    String errorMessage = "Could not login: the client is being asked for a password, but the Kafka" +
                                 " client code does not currently support obtaining a password from the user.";
                    throw new UnsupportedCallbackException(callback, errorMessage);
                } else if (callback instanceof RealmCallback) {
                    RealmCallback rc = (RealmCallback) callback;
                    rc.setText(rc.getDefaultText());
                } else {
                    throw new UnsupportedCallbackException(callback, "Unrecognized SASL Login callback");
                }
            }
        }
    }
}

