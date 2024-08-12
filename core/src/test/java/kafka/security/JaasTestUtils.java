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
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.SaslConfigs.GSSAPI_MECHANISM;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.apache.kafka.common.security.plain.internals.PlainSaslServer.PLAIN_MECHANISM;

public class JaasTestUtils {
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
                    modules.stream().map(Object::toString).collect(Collectors.joining("\n  ")));
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
        return JaasModule.scramLoginModule(scramUser, scramPassword, false, new HashMap<>()).toString();
    }

    public static String clientLoginModule(String mechanism, Optional<File> keytabLocation, String serviceName) {
        return kafkaClientModule(
            mechanism,
            keytabLocation,
            KAFKA_CLIENT_PRINCIPAL,
            KAFKA_PLAIN_USER,
            KAFKA_PLAIN_PASSWORD,
            KAFKA_SCRAM_USER,
            KAFKA_SCRAM_PASSWORD,
            KAFKA_OAUTH_BEARER_USER,
            serviceName
        ).toString();
    }

    public static String clientLoginModule(String mechanism, Optional<File> keytabLocation) {
        return clientLoginModule(mechanism, keytabLocation, SERVICE_NAME);
    }

    public static String adminLoginModule(String mechanism, Optional<File> keytabLocation, String serviceName) {
        return kafkaClientModule(
            mechanism,
            keytabLocation,
            KAFKA_SERVER_PRINCIPAL,
            KAFKA_PLAIN_ADMIN,
            KAFKA_PLAIN_ADMIN_PASSWORD,
            KAFKA_SCRAM_ADMIN,
            KAFKA_SCRAM_ADMIN_PASSWORD,
            KAFKA_OAUTH_BEARER_ADMIN,
            serviceName
        ).toString();
    }

    public static String adminLoginModule(String mechanism, Optional<File> keytabLocation) {
        return adminLoginModule(mechanism, keytabLocation, SERVICE_NAME);
    }

    public static String tokenClientLoginModule(String tokenId, String password) {
        Map<String, String> tokenProps = new HashMap<>();
        tokenProps.put("tokenauth", "true");
        return JaasModule.scramLoginModule(tokenId, password, false, tokenProps).toString();
    }

    public static List<JaasSection> zkSections() {
        Map<String, String> zkServerEntries = new HashMap<>();
        zkServerEntries.put("user_super", ZK_USER_SUPER_PASSWD);
        zkServerEntries.put("user_" + ZK_USER, ZK_USER_PASSWORD);
        JaasSection zkServerSection = new JaasSection(ZK_SERVER_CONTEXT_NAME, Collections.singletonList(JaasModule.zkDigestModule(false, zkServerEntries)));

        Map<String, String> zkClientEntries = new HashMap<>();
        zkClientEntries.put("username", ZK_USER);
        zkClientEntries.put("password", ZK_USER_PASSWORD);
        JaasSection zkClientSection = new JaasSection(ZK_CLIENT_CONTEXT_NAME, Collections.singletonList(JaasModule.zkDigestModule(false, zkClientEntries)));

        return Arrays.asList(zkServerSection, zkClientSection);
    }

    public static JaasSection kafkaServerSection(String contextName, List<String> mechanisms, Optional<File> keytabLocation) {
        List<JaasModule> modules = new ArrayList<>();
        for (String mechanism : mechanisms) {
            switch (mechanism) {
                case GSSAPI_MECHANISM:
                    modules.add(JaasModule.krb5LoginModule(
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
                    modules.add(JaasModule.plainLoginModule(KAFKA_PLAIN_ADMIN, KAFKA_PLAIN_ADMIN_PASSWORD, false, validUsers));
                    break;
                case OAUTHBEARER_MECHANISM:
                    modules.add(JaasModule.oAuthBearerLoginModule(KAFKA_OAUTH_BEARER_ADMIN, false));
                    break;
                default:
                    if (ScramMechanism.fromMechanismName(mechanism) != ScramMechanism.UNKNOWN) {
                        modules.add(JaasModule.scramLoginModule(KAFKA_SCRAM_ADMIN, KAFKA_SCRAM_ADMIN_PASSWORD, false, new HashMap<>()));
                    } else {
                        throw new IllegalArgumentException("Unsupported server mechanism " + mechanism);
                    }
                    break;
            }
        }
        return new JaasSection(contextName, modules);
    }

    private static JaasModule kafkaClientModule(String mechanism,
                                                Optional<File> keytabLocation,
                                                String clientPrincipal,
                                                String plainUser,
                                                String plainPassword,
                                                String scramUser,
                                                String scramPassword,
                                                String oauthBearerUser,
                                                String serviceName) {
        switch (mechanism) {
            case GSSAPI_MECHANISM:
                return JaasModule.krb5LoginModule(
                        true,
                        true,
                        keytabLocation.orElseThrow(() -> new IllegalArgumentException("Keytab location not specified for GSSAPI")).getAbsolutePath(),
                        clientPrincipal,
                        true,
                        Optional.of(serviceName),
                        IS_IBM_SECURITY
                );
            case PLAIN_MECHANISM:
                return JaasModule.plainLoginModule(plainUser, plainPassword, false, new HashMap<>());
            case OAUTHBEARER_MECHANISM:
                return JaasModule.oAuthBearerLoginModule(oauthBearerUser, false);
            default:
                if (ScramMechanism.fromMechanismName(mechanism) != ScramMechanism.UNKNOWN) {
                    return JaasModule.scramLoginModule(scramUser, scramPassword, false, new HashMap<>());
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

    private static void writeToFile(File file, List<JaasSection> jaasSections) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(String.join("", jaasSections.stream().map(Object::toString).toArray(String[]::new)));
        }
    }
}
