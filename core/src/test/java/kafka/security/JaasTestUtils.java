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

import kafka.utils.TestUtils;

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.utils.Java;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.common.config.SaslConfigs.GSSAPI_MECHANISM;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.apache.kafka.common.security.plain.internals.PlainSaslServer.PLAIN_MECHANISM;

public class JaasTestUtils {

    public static class Krb5LoginModule implements JaasModule {
        private final boolean useKeyTab;
        private final boolean storeKey;
        private final String keyTab;
        private final String principal;
        private final boolean debug;
        private final Optional<String> serviceName;
        private final boolean isIbmSecurity;

        public Krb5LoginModule(boolean useKeyTab, boolean storeKey, String keyTab, String principal, boolean debug, Optional<String> serviceName, boolean isIbmSecurity) {
            this.useKeyTab = useKeyTab;
            this.storeKey = storeKey;
            this.keyTab = keyTab;
            this.principal = principal;
            this.debug = debug;
            this.serviceName = serviceName;
            this.isIbmSecurity = isIbmSecurity;
        }

        @Override
        public String name() {
            return isIbmSecurity ? "com.ibm.security.auth.module.Krb5LoginModule" : "com.sun.security.auth.module.Krb5LoginModule";
        }

        @Override
        public Map<String, String> entries() {
            if (isIbmSecurity) {
                Map<String, String> map = new HashMap<>();
                map.put("principal", principal);
                map.put("credsType", "both");
                if (useKeyTab) {
                    map.put("useKeytab", "file:" + keyTab);
                }
                return map;
            } else {
                Map<String, String> map = new HashMap<>();
                map.put("useKeyTab", Boolean.toString(useKeyTab));
                map.put("storeKey", Boolean.toString(storeKey));
                map.put("keyTab", keyTab);
                map.put("principal", principal);
                serviceName.ifPresent(s -> map.put("serviceName", s));
                return map;
            }
        }

        @Override
        public boolean debug() {
            return debug;
        }

        @Override
        public String toString() {
            return String.format("%s required\n  debug=%b\n  %s;\n",
                    name(),
                    debug,
                    entries()
                            .entrySet()
                            .stream()
                            .map(e -> e.getKey() + "=\"" + e.getValue() + "\"").reduce((e1, e2) -> e1 + "\n  " + e2)
                            .orElse(""));
        }
    }

    public static class PlainLoginModule implements JaasModule {
        private final String username;
        private final String password;
        private final boolean debug;
        private final Map<String, String> validUsers;

        public PlainLoginModule(String username, String password) {
            this(username, password, false, Collections.emptyMap());
        }

        public PlainLoginModule(String username, String password, boolean debug, Map<String, String> validUsers) {
            this.username = username;
            this.password = password;
            this.debug = debug;
            this.validUsers = validUsers;
        }

        @Override
        public String name() {
            return "org.apache.kafka.common.security.plain.PlainLoginModule";
        }

        @Override
        public Map<String, String> entries() {
            Map<String, String> map = new HashMap<>();
            map.put("username", username);
            map.put("password", password);
            validUsers.forEach((user, pass) -> map.put("user_" + user, pass));
            return map;
        }

        @Override
        public boolean debug() {
            return debug;
        }

        @Override
        public String toString() {
            return String.format("%s required\n  debug=%b\n  %s;\n",
                    name(),
                    debug,
                    entries()
                            .entrySet()
                            .stream()
                            .map(e -> e.getKey() + "=\"" + e.getValue() + "\"").reduce((e1, e2) -> e1 + "\n  " + e2)
                            .orElse(""));
        }
    }

    public static class ZkDigestModule implements JaasModule {
        private final boolean debug;
        private final Map<String, String> entries;

        public ZkDigestModule(boolean debug, Map<String, String> entries) {
            this.debug = debug;
            this.entries = entries;
        }

        @Override
        public String name() {
            return "org.apache.zookeeper.server.auth.DigestLoginModule";
        }

        @Override
        public Map<String, String> entries() {
            return entries;
        }

        @Override
        public boolean debug() {
            return debug;
        }

        @Override
        public String toString() {
            return String.format("%s required\n  debug=%b\n  %s;\n",
                    name(),
                    debug,
                    entries()
                            .entrySet()
                            .stream()
                            .map(e -> e.getKey() + "=\"" + e.getValue() + "\"")
                            .reduce((e1, e2) -> e1 + "\n  " + e2)
                            .orElse(""));
        }
    }

    public static class ScramLoginModule implements JaasModule {
        private final String username;
        private final String password;
        private final boolean debug;
        private final Map<String, String> tokenProps;

        public ScramLoginModule(String username, String password) {
            this(username, password, false, Collections.emptyMap());
        }

        public ScramLoginModule(String username, String password, boolean debug, Map<String, String> tokenProps) {
            this.username = username;
            this.password = password;
            this.debug = debug;
            this.tokenProps = tokenProps;
        }

        @Override
        public String name() {
            return "org.apache.kafka.common.security.scram.ScramLoginModule";
        }

        @Override
        public Map<String, String> entries() {
            Map<String, String> map = new HashMap<>();
            map.put("username", username);
            map.put("password", password);
            map.putAll(tokenProps);
            return map;
        }

        @Override
        public boolean debug() {
            return debug;
        }

        @Override
        public String toString() {
            return String.format("%s required\n  debug=%b\n  %s;\n",
                    name(),
                    debug,
                    entries()
                            .entrySet()
                            .stream()
                            .map(e -> e.getKey() + "=\"" + e.getValue() + "\"")
                            .reduce((e1, e2) -> e1 + "\n  " + e2)
                            .orElse(""));
        }
    }

    public static class OAuthBearerLoginModule implements JaasModule {
        private final String username;
        private final boolean debug;

        public OAuthBearerLoginModule(String username, boolean debug) {
            this.username = username;
            this.debug = debug;
        }

        @Override
        public String name() {
            return "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";
        }

        @Override
        public Map<String, String> entries() {
            Map<String, String> map = new HashMap<>();
            map.put("unsecuredLoginStringClaim_sub", username);
            return map;
        }

        @Override
        public boolean debug() {
            return debug;
        }

        @Override
        public String toString() {
            return String.format("%s required\n  debug=%b\n  %s;\n",
                    name(),
                    debug,
                    entries()
                            .entrySet()
                            .stream()
                            .map(e -> e.getKey() + "=\"" + e.getValue() + "\"")
                            .reduce((e1, e2) -> e1 + "\n  " + e2)
                            .orElse(""));
        }
    }

    public interface JaasModule {
        String name();

        boolean debug();

        Map<String, String> entries();
    }

    public static class JaasSection {
        private final String contextName;
        private final List<JaasModule> modules;

        public JaasSection(String contextName, List<JaasModule> modules) {
            this.contextName = contextName;
            this.modules = modules;
        }

        public List<JaasModule> getModules() {
            return modules;
        }

        public String getContextName() {
            return contextName;
        }

        @Override
        public String toString() {
            return String.format("%s {\n  %s\n};\n",
                    contextName,
                    String.join("\n  ", modules.stream().map(Object::toString).toArray(String[]::new)));
        }
    }

    private static final boolean IS_IBM_SECURITY = Java.isIbmJdk() && !Java.isIbmJdkSemeru();

    private static final String ZK_SERVER_CONTEXT_NAME = "Server";
    private static final String ZK_CLIENT_CONTEXT_NAME = "Client";
    private static final String ZK_USER_SUPER_PASSWD = "adminpasswd";
    private static final String ZK_USER = "fpj";
    private static final String ZK_USER_PASSWORD = "fpjsecret";

    public static final String KAFKA_SERVER_CONTEXT_NAME = "KafkaServer";
    public static final String KAFKA_SERVER_PRINCIPAL_UNQUALIFIED_NAME = "kafka";
    private static final String KAFKA_SERVER_PRINCIPAL = KAFKA_SERVER_PRINCIPAL_UNQUALIFIED_NAME + "/localhost@EXAMPLE.COM";
    public static final String KAFKA_CLIENT_CONTEXT_NAME = "KafkaClient";
    public static final String KAFKA_CLIENT_PRINCIPAL_UNQUALIFIED_NAME = "client";
    private static final String KAFKA_CLIENT_PRINCIPAL = KAFKA_CLIENT_PRINCIPAL_UNQUALIFIED_NAME + "@EXAMPLE.COM";
    public static final String KAFKA_CLIENT_PRINCIPAL_UNQUALIFIED_NAME_2 = "client2";
    private static final String KAFKA_CLIENT_PRINCIPAL_2 = KAFKA_CLIENT_PRINCIPAL_UNQUALIFIED_NAME_2 + "@EXAMPLE.COM";

    public static final String KAFKA_PLAIN_USER = "plain-user";
    private static final String KAFKA_PLAIN_PASSWORD = "plain-user-secret";
    public static final String KAFKA_PLAIN_USER_2 = "plain-user2";
    public static final String KAFKA_PLAIN_PASSWORD_2 = "plain-user2-secret";
    public static final String KAFKA_PLAIN_ADMIN = "plain-admin";
    private static final String KAFKA_PLAIN_ADMIN_PASSWORD = "plain-admin-secret";

    public static final String KAFKA_SCRAM_USER = "scram-user";
    public static final String KAFKA_SCRAM_PASSWORD = "scram-user-secret";
    public static final String KAFKA_SCRAM_USER_2 = "scram-user2";
    public static final String KAFKA_SCRAM_PASSWORD_2 = "scram-user2-secret";
    public static final String KAFKA_SCRAM_ADMIN = "scram-admin";
    public static final String KAFKA_SCRAM_ADMIN_PASSWORD = "scram-admin-secret";

    public static final String KAFKA_OAUTH_BEARER_USER = "oauthbearer-user";
    public static final String KAFKA_OAUTH_BEARER_USER_2 = "oauthbearer-user2";
    public static final String KAFKA_OAUTH_BEARER_ADMIN = "oauthbearer-admin";

    public static final String SERVICE_NAME = "kafka";

    public static Properties saslConfigs(Optional<Properties> saslProperties) {
        Properties result = saslProperties.orElse(new Properties());
        if (IS_IBM_SECURITY && !result.containsKey(SaslConfigs.SASL_KERBEROS_SERVICE_NAME)) {
            result.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, SERVICE_NAME);
        }
        return result;
    }

    public static File writeJaasContextsToFile(List<JaasSection> jaasSections) throws IOException {
        File jaasFile = TestUtils.tempFile();
        writeToFile(jaasFile, jaasSections);
        return jaasFile;
    }

    public static String scramClientLoginModule(String mechanism, String scramUser, String scramPassword) {
        if (ScramMechanism.fromMechanismName(mechanism) == ScramMechanism.UNKNOWN) {
            throw new IllegalArgumentException("Unsupported SCRAM mechanism " + mechanism);
        }
        return new ScramLoginModule(scramUser, scramPassword, false, new HashMap<>()).toString();
    }

    public static String clientLoginModule(String mechanism, Optional<File> keytabLocation, String serviceName) {
        return kafkaClientModule(mechanism, keytabLocation, KAFKA_CLIENT_PRINCIPAL, KAFKA_PLAIN_USER, KAFKA_PLAIN_PASSWORD, KAFKA_SCRAM_USER, KAFKA_SCRAM_PASSWORD, KAFKA_OAUTH_BEARER_USER, serviceName).toString();
    }

    public static String clientLoginModule(String mechanism, Optional<File> keytabLocation) {
        return clientLoginModule(mechanism, keytabLocation, SERVICE_NAME);
    }

    public static String adminLoginModule(String mechanism, Optional<File> keytabLocation, String serviceName) {
        return kafkaClientModule(mechanism, keytabLocation, KAFKA_SERVER_PRINCIPAL, KAFKA_PLAIN_ADMIN, KAFKA_PLAIN_ADMIN_PASSWORD, KAFKA_SCRAM_ADMIN, KAFKA_SCRAM_ADMIN_PASSWORD, KAFKA_OAUTH_BEARER_ADMIN, serviceName).toString();
    }

    public static String adminLoginModule(String mechanism, Optional<File> keytabLocation) {
        return adminLoginModule(mechanism, keytabLocation, SERVICE_NAME);
    }

    public static String tokenClientLoginModule(String tokenId, String password) {
        Map<String, String> tokenProps = new HashMap<>();
        tokenProps.put("tokenauth", "true");
        return new ScramLoginModule(tokenId, password, false, tokenProps).toString();
    }

    public static List<JaasSection> zkSections() {
        Map<String, String> zkServerEntries = new HashMap<>();
        zkServerEntries.put("user_super", ZK_USER_SUPER_PASSWD);
        zkServerEntries.put("user_" + ZK_USER, ZK_USER_PASSWORD);
        JaasSection zkServerSection = new JaasSection(ZK_SERVER_CONTEXT_NAME, Collections.singletonList(new ZkDigestModule(false, zkServerEntries)));

        Map<String, String> zkClientEntries = new HashMap<>();
        zkClientEntries.put("username", ZK_USER);
        zkClientEntries.put("password", ZK_USER_PASSWORD);
        JaasSection zkClientSection = new JaasSection(ZK_CLIENT_CONTEXT_NAME, Collections.singletonList(new ZkDigestModule(false, zkClientEntries)));

        return Arrays.asList(zkServerSection, zkClientSection);
    }

    public static JaasSection kafkaServerSection(String contextName, List<String> mechanisms, Optional<File> keytabLocation) {
        List<JaasModule> modules = new ArrayList<>();
        for (String mechanism : mechanisms) {
            switch (mechanism) {
                case GSSAPI_MECHANISM:
                    modules.add(new Krb5LoginModule(
                            true,
                            true,
                            keytabLocation.orElseThrow(() -> new IllegalArgumentException("Keytab location not specified for GSSAPI")).getAbsolutePath(),
                            KAFKA_SERVER_PRINCIPAL,
                            true,
                            Optional.of(SERVICE_NAME),
                            IS_IBM_SECURITY
                    ));
                    break;
                case PLAIN_MECHANISM:
                    Map<String, String> validUsers = new HashMap<>();
                    validUsers.put(KAFKA_PLAIN_ADMIN, KAFKA_PLAIN_ADMIN_PASSWORD);
                    validUsers.put(KAFKA_PLAIN_USER, KAFKA_PLAIN_PASSWORD);
                    validUsers.put(KAFKA_PLAIN_USER_2, KAFKA_PLAIN_PASSWORD_2);
                    modules.add(new PlainLoginModule(KAFKA_PLAIN_ADMIN, KAFKA_PLAIN_ADMIN_PASSWORD, false, validUsers));
                    break;
                case OAUTHBEARER_MECHANISM:
                    modules.add(new OAuthBearerLoginModule(KAFKA_OAUTH_BEARER_ADMIN, false));
                    break;
                default:
                    if (ScramMechanism.fromMechanismName(mechanism) != ScramMechanism.UNKNOWN) {
                        modules.add(new ScramLoginModule(KAFKA_SCRAM_ADMIN, KAFKA_SCRAM_ADMIN_PASSWORD, false, new HashMap<>()));
                    } else {
                        throw new IllegalArgumentException("Unsupported server mechanism " + mechanism);
                    }
                    break;
            }
        }
        return new JaasSection(contextName, modules);
    }

    private static JaasModule kafkaClientModule(String mechanism, Optional<File> keytabLocation, String clientPrincipal, String plainUser, String plainPassword, String scramUser, String scramPassword, String oauthBearerUser, String serviceName) {
        switch (mechanism) {
            case GSSAPI_MECHANISM:
                return new Krb5LoginModule(
                        true,
                        true,
                        keytabLocation.orElseThrow(() -> new IllegalArgumentException("Keytab location not specified for GSSAPI")).getAbsolutePath(),
                        clientPrincipal,
                        true,
                        Optional.of(serviceName),
                        IS_IBM_SECURITY
                );
            case PLAIN_MECHANISM:
                return new PlainLoginModule(plainUser, plainPassword, false, new HashMap<>());
            case OAUTHBEARER_MECHANISM:
                return new OAuthBearerLoginModule(oauthBearerUser, false);
            default:
                if (ScramMechanism.fromMechanismName(mechanism) != ScramMechanism.UNKNOWN) {
                    return new ScramLoginModule(scramUser, scramPassword, false, new HashMap<>());
                } else {
                    throw new IllegalArgumentException("Unsupported client mechanism " + mechanism);
                }
        }
    }

    public static JaasSection kafkaClientSection(Optional<String> mechanism, Optional<File> keytabLocation) {
        return new JaasSection(KAFKA_CLIENT_CONTEXT_NAME,
                mechanism.map(m -> kafkaClientModule(m,
                        keytabLocation,
                        KAFKA_CLIENT_PRINCIPAL_2,
                        KAFKA_PLAIN_USER_2,
                        KAFKA_PLAIN_PASSWORD_2,
                        KAFKA_SCRAM_USER_2,
                        KAFKA_SCRAM_PASSWORD_2,
                        KAFKA_OAUTH_BEARER_USER_2,
                        SERVICE_NAME)
                ).map(Collections::singletonList).orElse(Collections.emptyList()));
    }

    private static String jaasSectionsToString(List<JaasSection> jaasSections) {
        return String.join("", jaasSections.stream().map(Object::toString).toArray(String[]::new));
    }

    private static void writeToFile(File file, List<JaasSection> jaasSections) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(jaasSectionsToString(jaasSections));
        }
    }
}
