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
package org.apache.kafka.common.security.plain;

import org.apache.kafka.common.security.plain.internals.PlainSaslServerProvider;

import java.util.Iterator;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

public class PlainLoginModule implements LoginModule {

    private static final String USERNAME_CONFIG = "username";
    private static final String PASSWORD_CONFIG = "password";

    private Subject subject;
    private Map<String, ?> options;
    private boolean loginSucceeded;

    static {
        PlainSaslServerProvider.initialize();
    }

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.subject = subject;
        this.options = options;

        String username = (String) options.get(USERNAME_CONFIG);
        if (username != null)
            subject.getPublicCredentials().add(username);
        String password = (String) options.get(PASSWORD_CONFIG);
        if (password != null)
            subject.getPrivateCredentials().add(password);
    }

    @Override
    public boolean login() throws LoginException {
        String username = (String) options.get(USERNAME_CONFIG);
        String password = (String) options.get(PASSWORD_CONFIG);

        if (username == null || username.isEmpty()) {
            throw new LoginException("Username must be specified");
        }
        if (password == null || password.isEmpty()) {
            throw new LoginException("Password must be specified");
        }

        // Ensure that the subject actually contains the configured credentials
        if (!subject.getPublicCredentials().contains(username)) {
            throw new LoginException("Configured username not present in subject credentials");
        }
        if (!subject.getPrivateCredentials().contains(password)) {
            throw new LoginException("Configured password not present in subject credentials");
        }

        loginSucceeded = true;
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        if (!loginSucceeded) {
            return false;
        }

        // Remove stored credentials from the subject on logout
        removeCredential(subject.getPublicCredentials(), options.get(USERNAME_CONFIG));
        removeCredential(subject.getPrivateCredentials(), options.get(PASSWORD_CONFIG));

        loginSucceeded = false;
        return true;
    }

    @Override
    public boolean commit() throws LoginException {
        return loginSucceeded;
    }

    @Override
    public boolean abort() throws LoginException {
        if (!loginSucceeded) {
            return false;
        }

        // Roll back any state associated with this login attempt
        removeCredential(subject.getPublicCredentials(), options.get(USERNAME_CONFIG));
        removeCredential(subject.getPrivateCredentials(), options.get(PASSWORD_CONFIG));

        loginSucceeded = false;
        return true;
    }

    private void removeCredential(Iterable<?> credentials, Object value) {
        if (value == null)
            return;

        Iterator<?> iterator = credentials.iterator();
        while (iterator.hasNext()) {
            Object credential = iterator.next();
            if (value.equals(credential)) {
                iterator.remove();
            }
        }
    }
}
