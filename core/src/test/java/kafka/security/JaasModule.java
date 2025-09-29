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
package kafka.security;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class JaasModule {
    public static JaasModule krb5LoginModule(boolean useKeyTab, boolean storeKey, String keyTab, String principal, boolean debug, Optional<String> serviceName, boolean isIbmSecurity) {
        String name = isIbmSecurity ? "com.ibm.security.auth.module.Krb5LoginModule" : "com.sun.security.auth.module.Krb5LoginModule";

        Map<String, String> entries = new HashMap<>();
        if (isIbmSecurity) {
            entries.put("principal", principal);
            entries.put("credsType", "both");
            if (useKeyTab) {
                entries.put("useKeytab", "file:" + keyTab);
            }
        } else {
            entries.put("useKeyTab", Boolean.toString(useKeyTab));
            entries.put("storeKey", Boolean.toString(storeKey));
            entries.put("keyTab", keyTab);
            entries.put("principal", principal);
            serviceName.ifPresent(s -> entries.put("serviceName", s));
        }

        return new JaasModule(
            name,
            debug,
            entries
        );
    }

    public static JaasModule oAuthBearerLoginModule(String username, boolean debug) {
        String name = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";

        Map<String, String> entries = new HashMap<>();
        entries.put("unsecuredLoginStringClaim_sub", username);

        return new JaasModule(
            name,
            debug,
            entries
        );
    }

    public static JaasModule plainLoginModule(String username, String password) {
        return plainLoginModule(username, password, false, Collections.emptyMap());
    }

    public static JaasModule plainLoginModule(String username, String password, boolean debug, Map<String, String> validUsers) {
        String name = "org.apache.kafka.common.security.plain.PlainLoginModule";

        Map<String, String> entries = new HashMap<>();
        entries.put("username", username);
        entries.put("password", password);
        validUsers.forEach((user, pass) -> entries.put("user_" + user, pass));

        return new JaasModule(
            name,
            debug,
            entries
        );
    }

    public static JaasModule scramLoginModule(String username, String password) {
        return scramLoginModule(username, password, false, Collections.emptyMap());
    }

    public static JaasModule scramLoginModule(String username, String password, boolean debug, Map<String, String> tokenProps) {
        String name = "org.apache.kafka.common.security.scram.ScramLoginModule";

        Map<String, String> entries = new HashMap<>();
        entries.put("username", username);
        entries.put("password", password);
        entries.putAll(tokenProps);

        return new JaasModule(
            name,
            debug,
            entries
        );
    }

    private final String name;

    private final boolean debug;

    private final Map<String, String> entries;

    private JaasModule(String name, boolean debug, Map<String, String> entries) {
        this.name = name;
        this.debug = debug;
        this.entries = entries;
    }

    public String name() {
        return name;
    }

    public boolean debug() {
        return debug;
    }

    @Override
    public String toString() {
        return String.format("%s required\n  debug=%b\n  %s;\n", name, debug, entries.entrySet().stream()
                .map(e -> e.getKey() + "=\"" + e.getValue() + "\"")
                .collect(Collectors.joining("\n  ")));
    }
}
