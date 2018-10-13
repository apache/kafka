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
package org.apache.kafka.common.security.authenticator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

public class TestJaasConfig extends Configuration {

    static final String LOGIN_CONTEXT_CLIENT = "KafkaClient";
    static final String LOGIN_CONTEXT_SERVER = "KafkaServer";

    static final String USERNAME = "myuser";
    static final String PASSWORD = "mypassword";

    private Map<String, AppConfigurationEntry[]> entryMap = new HashMap<>();

    public static TestJaasConfig createConfiguration(String clientMechanism, List<String> serverMechanisms) {
        TestJaasConfig config = new TestJaasConfig();
        config.createOrUpdateEntry(LOGIN_CONTEXT_CLIENT, loginModule(clientMechanism), defaultClientOptions(clientMechanism));
        for (String mechanism : serverMechanisms) {
            config.addEntry(LOGIN_CONTEXT_SERVER, loginModule(mechanism), defaultServerOptions(mechanism));
        }
        Configuration.setConfiguration(config);
        return config;
    }

    public static Password jaasConfigProperty(String mechanism, String username, String password) {
        return new Password(loginModule(mechanism) + " required username=" + username + " password=" + password + ";");
    }

    public static Password jaasConfigProperty(String mechanism, Map<String, Object> options) {
        StringBuilder builder = new StringBuilder();
        builder.append(loginModule(mechanism));
        builder.append(" required");
        for (Map.Entry<String, Object> option : options.entrySet()) {
            builder.append(' ');
            builder.append(option.getKey());
            builder.append('=');
            builder.append(option.getValue());
        }
        builder.append(';');
        return new Password(builder.toString());
    }

    public void setClientOptions(String saslMechanism, String clientUsername, String clientPassword) {
        Map<String, Object> options = new HashMap<>();
        if (clientUsername != null)
            options.put("username", clientUsername);
        if (clientPassword != null)
            options.put("password", clientPassword);
        Class<?> loginModuleClass = ScramMechanism.isScram(saslMechanism) ? ScramLoginModule.class : PlainLoginModule.class;
        createOrUpdateEntry(LOGIN_CONTEXT_CLIENT, loginModuleClass.getName(), options);
    }

    public void createOrUpdateEntry(String name, String loginModule, Map<String, Object> options) {
        AppConfigurationEntry entry = new AppConfigurationEntry(loginModule, LoginModuleControlFlag.REQUIRED, options);
        entryMap.put(name, new AppConfigurationEntry[] {entry});
    }

    public void addEntry(String name, String loginModule, Map<String, Object> options) {
        AppConfigurationEntry entry = new AppConfigurationEntry(loginModule, LoginModuleControlFlag.REQUIRED, options);
        AppConfigurationEntry[] existing = entryMap.get(name);
        AppConfigurationEntry[] newEntries = existing == null ? new AppConfigurationEntry[1] : Arrays.copyOf(existing, existing.length + 1);
        newEntries[newEntries.length - 1] = entry;
        entryMap.put(name, newEntries);
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return entryMap.get(name);
    }

    private static String loginModule(String mechanism) {
        String loginModule;
        switch (mechanism) {
            case "PLAIN":
                loginModule = PlainLoginModule.class.getName();
                break;
            case "DIGEST-MD5":
                loginModule = TestDigestLoginModule.class.getName();
                break;
            case "OAUTHBEARER":
                loginModule = OAuthBearerLoginModule.class.getName();
                break;
            default:
                if (ScramMechanism.isScram(mechanism))
                    loginModule = ScramLoginModule.class.getName();
                else
                    throw new IllegalArgumentException("Unsupported mechanism " + mechanism);
        }
        return loginModule;
    }

    public static Map<String, Object> defaultClientOptions(String mechanism) {
        switch (mechanism) {
            case "OAUTHBEARER":
                Map<String, Object> options = new HashMap<>();
                options.put("unsecuredLoginStringClaim_sub", USERNAME);
                return options;
            default:
                return defaultClientOptions();
        }
    }
    
    public static Map<String, Object> defaultClientOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("username", USERNAME);
        options.put("password", PASSWORD);
        return options;
    }

    public static Map<String, Object> defaultServerOptions(String mechanism) {
        Map<String, Object> options = new HashMap<>();
        switch (mechanism) {
            case "PLAIN":
            case "DIGEST-MD5":
                options.put("user_" + USERNAME, PASSWORD);
                break;
            case "OAUTHBEARER":
                options.put("unsecuredLoginStringClaim_sub", USERNAME);
                break;
            default:
                if (!ScramMechanism.isScram(mechanism))
                    throw new IllegalArgumentException("Unsupported mechanism " + mechanism);
        }
        return options;
    }
}
