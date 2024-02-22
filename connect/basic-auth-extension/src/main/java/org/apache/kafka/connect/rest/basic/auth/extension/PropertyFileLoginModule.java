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

package org.apache.kafka.connect.rest.basic.auth.extension;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

/**
 * {@link PropertyFileLoginModule} authenticates against a properties file.
 * The credentials should be stored in the format {username}={password} in the properties file.
 * The absolute path of the file needs to specified using the option <b>file</b>
 *
 * <p><b>NOTE: This implementation is NOT intended to be used in production since the credentials are stored in PLAINTEXT in the
 * properties file.</b>
 */
public class PropertyFileLoginModule implements LoginModule {
    private static final Logger log = LoggerFactory.getLogger(PropertyFileLoginModule.class);
    private static final Map<String, Properties> CREDENTIAL_PROPERTIES_MAP = new ConcurrentHashMap<>();
    private static final String FILE_OPTIONS = "file";
    private CallbackHandler callbackHandler;
    private String fileName;
    private boolean authenticated;

    private static final Map<String, Properties> CREDENTIAL_PROPERTIES = new ConcurrentHashMap<>();

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.callbackHandler = callbackHandler;
        fileName = (String) options.get(FILE_OPTIONS);
        if (Utils.isBlank(fileName)) {
            throw new ConfigException("Property Credentials file must be specified");
        }

        if (!CREDENTIAL_PROPERTIES.containsKey(fileName)) {
            log.trace("Opening credential properties file '{}'", fileName);
            Properties credentialProperties = new Properties();
            try {
                try (InputStream inputStream = Files.newInputStream(Paths.get(fileName))) {
                    log.trace("Parsing credential properties file '{}'", fileName);
                    credentialProperties.load(inputStream);
                }
                CREDENTIAL_PROPERTIES.putIfAbsent(fileName, credentialProperties);
                if (credentialProperties.isEmpty())
                    log.warn("Credential properties file '{}' is empty; all requests will be permitted",
                        fileName);
            } catch (IOException e) {
                log.error("Error loading credentials file ", e);
                throw new ConfigException("Error loading Property Credentials file");
            }
        } else {
            log.trace(
                "Credential properties file '{}' has already been opened and parsed; will read from cached, in-memory store",
                fileName);
        }
    }

    @Override
    public boolean login() throws LoginException {
        Callback[] callbacks = configureCallbacks();
        try {
            log.trace("Authenticating user; invoking JAAS login callbacks");
            callbackHandler.handle(callbacks);
        } catch (Exception e) {
            log.warn("Authentication failed while invoking JAAS login callbacks", e);
            throw new LoginException(e.getMessage());
        }

        String username = ((NameCallback) callbacks[0]).getName();
        char[] passwordChars = ((PasswordCallback) callbacks[1]).getPassword();
        String password = passwordChars != null ? new String(passwordChars) : null;
        Properties credentialProperties = CREDENTIAL_PROPERTIES.get(fileName);

        if (credentialProperties.isEmpty()) {
            log.trace("Not validating credentials for user '{}' as credential properties file '{}' is empty",
                username,
                fileName);
            authenticated = true;
        } else if (username == null) {
            log.trace("No credentials were provided or the provided credentials were malformed");
            authenticated = false;
        } else if (password != null && password.equals(credentialProperties.get(username))) {
            log.trace("Credentials provided for user '{}' match those present in the credential properties file '{}'",
                username,
                fileName);
            authenticated = true;
        } else if (!credentialProperties.containsKey(username)) {
            log.trace("User '{}' is not present in the credential properties file '{}'",
                username,
                fileName);
            authenticated = false;
        } else {
            log.trace("Credentials provided for user '{}' do not match those present in the credential properties file '{}'",
                username,
                fileName);
            authenticated = false;
        }

        return authenticated;
    }

    @Override
    public boolean commit() {
        return authenticated;
    }

    @Override
    public boolean abort() {
        return true;
    }

    @Override
    public boolean logout() {
        return true;
    }

    private Callback[] configureCallbacks() {
        Callback[] callbacks = new Callback[2];
        callbacks[0] = new NameCallback("Enter user name");
        callbacks[1] = new PasswordCallback("Enter password", false);
        return callbacks;
    }
}
