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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    private CallbackHandler callbackHandler;
    private static final String FILE_OPTIONS = "file";
    private String fileName;
    private boolean authenticated;

    private static Map<String, Properties> credentialPropertiesMap = new ConcurrentHashMap<>();

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.callbackHandler = callbackHandler;
        fileName = (String) options.get(FILE_OPTIONS);
        if (fileName == null || fileName.trim().isEmpty()) {
            throw new ConfigException("Property Credentials file must be specified");
        }
        if (!credentialPropertiesMap.containsKey(fileName)) {
            Properties credentialProperties = new Properties();
            try {
                credentialProperties.load(Files.newInputStream(Paths.get(fileName)));
                credentialPropertiesMap.putIfAbsent(fileName, credentialProperties);
            } catch (IOException e) {
                log.error("Error loading credentials file ", e);
                throw new ConfigException("Error loading Property Credentials file");
            }
        }
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
        char[] passwordChars = ((PasswordCallback) callbacks[1]).getPassword();
        String password = passwordChars != null ? new String(passwordChars) : null;
        Properties credentialProperties = credentialPropertiesMap.get(fileName);
        authenticated = credentialProperties.isEmpty() ||
                        (password != null && password.equals(credentialProperties.get(username)));
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
