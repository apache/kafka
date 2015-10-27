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

package org.apache.kafka.common.security.plain;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.Subject;

import org.apache.kafka.common.security.authenticator.Login;
import org.apache.kafka.common.security.authenticator.SaslMechanism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Login for PLAIN authentication using username/password.
 */
public class PlainLogin implements Login, CallbackHandler {
    private static final Logger log = LoggerFactory.getLogger(PlainLogin.class);
    
    private final LoginContext loginContext;
    
    public PlainLogin(final String loginContextName, Map<String, ?> configs) throws LoginException {
        loginContext = new LoginContext(loginContextName, this);
        loginContext.login();
    }
    
    @Override
    public SaslMechanism mechanism() {
        return SaslMechanism.PLAIN;
    }

    @Override
    public Subject subject() {
        return loginContext.getSubject();
    }
    
    @Override
    public void startThreadIfNeeded() {       
    }

    @Override
    public void shutdown() {
    }
    
    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                NameCallback nc = (NameCallback) callback;
                nc.setName(nc.getDefaultName());
            } else if (callback instanceof PasswordCallback) {
                // Call `setPassword` once we support obtaining a password from the user and update message below
                log.warn("Could not login: the client is being asked for a password, but the Kafka" +
                         " client code does not currently support obtaining a password from the user." +
                         " Configure password using JaaS config.");
            } else {
                throw new UnsupportedCallbackException(callback, "Unexpected callback for SASL/PLAIN");
            }
        }
    }
    

}
