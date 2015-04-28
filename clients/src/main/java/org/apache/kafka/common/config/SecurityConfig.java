/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.common.config;


import java.util.Map;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.protocol.SecurityProtocol;


/**
 * Security Related config for clients and server.
 */

public class SecurityConfig extends AbstractConfig {
    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS THESE ARE PART OF THE PUBLIC API AND
     * CHANGE WILL BREAK USER CODE.
     */

    private static final ConfigDef CONFIG;

    public static final String SECURITY_PROTOCOL_CONFIG = "security.protocol";
    public static final String SECURITY_PROTOCOL_DOC = "Protocol used to communicate with brokers. Currently only PLAINTEXT and SSL are supported.";

    public static final String SSL_PROTOCOL_CONFIG = "ssl.protocol";
    public static final String SSL_PROTOCOL_DOC = "The TLS protocol used for broker connections if security protocol is SSL. "
            + "Any version of TLS is accepted by default.";

    public static final String SSL_CIPHER_SUITES_CONFIG = "ssl.cipher.suites";
    public static final String SSL_CIPHER_SUITES_DOC = "The list of cipher suites enabled for SSL connections. "
            + "Default value is the list of cipher suites enabled for the Java Virtual Machine.";

    public static final String SSL_ENABLED_PROTOCOLS_CONFIG = "ssl.enabled.protocols";
    public static final String SSL_ENABLED_PROTOCOLS_DOC = "The list of protocols enabled for SSL connections. "
            + "Default value is the list of protocols enabled for the Java Virtual Machine.";


    public static final String SSL_KEYSTORE_TYPE_CONFIG = "ssl.keystore.type";
    public static final String SSL_KEYSTORE_TYPE_DOC = "The file format of the key store file. "
            + "Default value is the default key store format of the Java Virtual Machine.";

    public static final String SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location";
    public static final String SSL_KEYSTORE_LOCATION_DOC = "The location of the key store file. "
            + "This is optional for Client and can be used for two-way authentication for client.";

    public static final String SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password";
    public static final String SSL_KEYSTORE_PASSWORD_DOC = "The store password for the key store file. ";


    public static final String SSL_KEY_PASSWORD_CONFIG = "ssl.key.password";
    public static final String SSL_KEY_PASSWORD_DOC = "The password of the private key in the key store file. "
            + "This is optional for client.";

    public static final String SSL_TRUSTSTORE_TYPE_CONFIG = "ssl.truststore.type";
    public static final String SSL_TRUSTSTORE_TYPE_DOC = "The file format of the trust store file. "
            + "Default value is JKS.";

    public static final String SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";
    public static final String SSL_TRUSTSTORE_LOCATION_DOC = "The location of the trust store file. ";

    public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password";
    public static final String SSL_TRUSTSTORE_PASSWORD_DOC = "The password for the trust store file. ";

    public static final String SSL_CLIENT_REQUIRE_CERT_CONFIG = "ssl.client.require.cert";
    public static final String SSL_CLIENT_REQUIRE_CERT_DOC = "This is to enforce two-way authentication between client and server."
            + "Default value is false. If set to true client need to prover Keystrore releated config";

    public static final String SSL_KEYMANAGER_ALGORITHM_CONFIG = "ssl.keymanager.algorithm";
    public static final String SSL_KEYMANAGER_ALGORITHM_DOC = "The algorithm used by key manager factory for SSL connections. "
            + "Default value is the key manager factory algorithm configured for the Java Virtual Machine.";

    public static final String SSL_TRUSTMANAGER_ALGORITHM_CONFIG = "ssl.trustmanager.algorithm";
    public static final String SSL_TRUSTMANAGER_ALGORITHM_DOC = "The algorithm used by trust manager factory for SSL connections. "
            + "Default value is the trust manager factory algorithm configured for the Java Virtual Machine.";


    static {
        CONFIG = new ConfigDef().define(SECURITY_PROTOCOL_CONFIG, Type.STRING, SecurityProtocol.PLAINTEXT.toString(), Importance.MEDIUM, SECURITY_PROTOCOL_DOC)
                .define(SSL_PROTOCOL_CONFIG, Type.STRING, "TLS", Importance.MEDIUM, SSL_PROTOCOL_DOC)
                .define(SSL_CIPHER_SUITES_CONFIG, Type.LIST, Importance.MEDIUM, SSL_CIPHER_SUITES_DOC, false)
                .define(SSL_ENABLED_PROTOCOLS_CONFIG, Type.LIST, "TLSv1.2, TLSv1.1, TLSv1", Importance.MEDIUM, SSL_ENABLED_PROTOCOLS_DOC)
                .define(SSL_KEYSTORE_TYPE_CONFIG, Type.STRING, Importance.MEDIUM, SSL_KEYSTORE_TYPE_DOC, false)
                .define(SSL_KEYSTORE_LOCATION_CONFIG, Type.STRING, Importance.MEDIUM, SSL_KEYSTORE_LOCATION_DOC, false)
                .define(SSL_KEYSTORE_PASSWORD_CONFIG, Type.STRING, Importance.MEDIUM, SSL_KEYSTORE_PASSWORD_DOC, false)
                .define(SSL_KEY_PASSWORD_CONFIG, Type.STRING, Importance.MEDIUM, SSL_KEY_PASSWORD_DOC, false)
                .define(SSL_TRUSTSTORE_TYPE_CONFIG, Type.STRING, Importance.MEDIUM, SSL_TRUSTSTORE_TYPE_DOC, false)
                .define(SSL_TRUSTSTORE_LOCATION_CONFIG, Type.STRING, Importance.MEDIUM, SSL_TRUSTSTORE_LOCATION_DOC, false)
                .define(SSL_TRUSTSTORE_PASSWORD_CONFIG, Type.STRING, Importance.MEDIUM, SSL_TRUSTSTORE_PASSWORD_DOC, false)
                .define(SSL_KEYMANAGER_ALGORITHM_CONFIG, Type.STRING, Importance.MEDIUM, SSL_KEYMANAGER_ALGORITHM_DOC, false)
                .define(SSL_TRUSTMANAGER_ALGORITHM_CONFIG, Type.STRING, Importance.MEDIUM, SSL_TRUSTMANAGER_ALGORITHM_DOC, false)
                .define(SSL_CLIENT_REQUIRE_CERT_CONFIG, Type.BOOLEAN, false, Importance.MEDIUM, SSL_CLIENT_REQUIRE_CERT_DOC);
    }

    public SecurityConfig(Map<? extends Object, ? extends Object> props) {
        super(CONFIG, props);
    }


}
