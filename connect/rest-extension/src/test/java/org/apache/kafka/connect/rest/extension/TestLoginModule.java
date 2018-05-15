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

package org.apache.kafka.connect.rest.extension;

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

public class TestLoginModule implements LoginModule {

    CallbackHandler callbackHandler;
    private String expectedUsername = "user";
    private String expectedPassword = "password";
    private boolean authenticated;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler,
                           Map<String, ?> sharedState, Map<String, ?> options) {
        this.callbackHandler = callbackHandler;
    }

    @Override
    public boolean login() throws LoginException {
        Callback[] callbacks = configureCallbacks();
        try {
            callbackHandler.handle(callbacks);
        } catch (Exception e) {
            throw new LoginException(e.getMessage());
        }

        String username = ((NameCallback) callbacks[0]).getName();
        String password = new String(((PasswordCallback) callbacks[1]).getPassword());
        authenticated = expectedUsername.equals(username) && expectedPassword.equals(password);
        return authenticated;
    }

    @Override
    public boolean commit() throws LoginException {
        return authenticated;
    }

    @Override
    public boolean abort() throws LoginException {
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        return true;
    }

    private Callback[] configureCallbacks() {

        Callback[] callbacks = new Callback[2];
        callbacks[0] = new NameCallback("Enter user name");
        callbacks[1] = new PasswordCallback("Enter password", false);
        return callbacks;
    }
}
