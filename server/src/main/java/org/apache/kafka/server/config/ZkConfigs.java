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
package org.apache.kafka.server.config;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public final class ZkConfigs {
    /** ********* Zookeeper Configuration ***********/
    public static final String ZK_CONNECT_CONFIG = "zookeeper.connect";
    public static final String ZK_SESSION_TIMEOUT_MS_CONFIG = "zookeeper.session.timeout.ms";
    public static final String ZK_CONNECTION_TIMEOUT_MS_CONFIG = "zookeeper.connection.timeout.ms";
    public static final String ZK_ENABLE_SECURE_ACLS_CONFIG = "zookeeper.set.acl";
    public static final String ZK_MAX_IN_FLIGHT_REQUESTS_CONFIG = "zookeeper.max.in.flight.requests";
    public static final String ZK_SSL_CLIENT_ENABLE_CONFIG = "zookeeper.ssl.client.enable";
    public static final String ZK_CLIENT_CNXN_SOCKET_CONFIG = "zookeeper.clientCnxnSocket";
    public static final String ZK_SSL_KEY_STORE_LOCATION_CONFIG = "zookeeper.ssl.keystore.location";
    public static final String ZK_SSL_KEY_STORE_PASSWORD_CONFIG = "zookeeper.ssl.keystore.password";
    public static final String ZK_SSL_KEY_STORE_TYPE_CONFIG = "zookeeper.ssl.keystore.type";
    public static final String ZK_SSL_TRUST_STORE_LOCATION_CONFIG = "zookeeper.ssl.truststore.location";
    public static final String ZK_SSL_TRUST_STORE_PASSWORD_CONFIG = "zookeeper.ssl.truststore.password";
    public static final String ZK_SSL_TRUST_STORE_TYPE_CONFIG = "zookeeper.ssl.truststore.type";
    public static final String ZK_SSL_PROTOCOL_CONFIG = "zookeeper.ssl.protocol";
    public static final String ZK_SSL_ENABLED_PROTOCOLS_CONFIG = "zookeeper.ssl.enabled.protocols";
    public static final String ZK_SSL_CIPHER_SUITES_CONFIG = "zookeeper.ssl.cipher.suites";
    public static final String ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG = "zookeeper.ssl.endpoint.identification.algorithm";
    public static final String ZK_SSL_CRL_ENABLE_CONFIG = "zookeeper.ssl.crl.enable";
    public static final String ZK_SSL_OCSP_ENABLE_CONFIG = "zookeeper.ssl.ocsp.enable";

    public static final String ZK_CONNECT_DOC = "Specifies the ZooKeeper connection string in the form <code>hostname:port</code> where host and port are the " +
        "host and port of a ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is " +
        "down you can also specify multiple hosts in the form <code>hostname1:port1,hostname2:port2,hostname3:port3</code>.\n" +
        "The server can also have a ZooKeeper chroot path as part of its ZooKeeper connection string which puts its data under some path in the global ZooKeeper namespace. " +
        "For example to give a chroot path of <code>/chroot/path</code> you would give the connection string as <code>hostname1:port1,hostname2:port2,hostname3:port3/chroot/path</code>.";
    public static final String ZK_SESSION_TIMEOUT_MS_DOC = "Zookeeper session timeout";
    public static final String ZK_CONNECTION_TIMEOUT_MS_DOC = "The max time that the client waits to establish a connection to ZooKeeper. If not set, the value in " + ZK_SESSION_TIMEOUT_MS_CONFIG + " is used";
    public static final String ZK_ENABLE_SECURE_ACLS_DOC = "Set client to use secure ACLs";
    public static final String ZK_MAX_IN_FLIGHT_REQUESTS_DOC = "The maximum number of unacknowledged requests the client will send to ZooKeeper before blocking.";
    public static final String ZK_SSL_CLIENT_ENABLE_DOC;
    public static final String ZK_CLIENT_CNXN_SOCKET_DOC;
    public static final String ZK_SSL_KEY_STORE_LOCATION_DOC;
    public static final String ZK_SSL_KEY_STORE_PASSWORD_DOC;
    public static final String ZK_SSL_KEY_STORE_TYPE_DOC;
    public static final String ZK_SSL_TRUST_STORE_LOCATION_DOC;
    public static final String ZK_SSL_TRUST_STORE_PASSWORD_DOC;
    public static final String ZK_SSL_TRUST_STORE_TYPE_DOC;
    public static final String ZK_SSL_PROTOCOL_DOC;
    public static final String ZK_SSL_ENABLED_PROTOCOLS_DOC;
    public static final String ZK_SSL_CIPHER_SUITES_DOC;
    public static final String ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC;
    public static final String ZK_SSL_CRL_ENABLE_DOC;
    public static final String ZK_SSL_OCSP_ENABLE_DOC;

    // a map from the Kafka config to the corresponding ZooKeeper Java system property
    public static final Map<String, String> ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP;

    public static final int ZK_SESSION_TIMEOUT_MS = 18000;
    public static final boolean ZK_ENABLE_SECURE_ACLS = false;
    public static final int ZK_MAX_IN_FLIGHT_REQUESTS = 10;
    public static final boolean ZK_SSL_CLIENT_ENABLE = false;
    public static final String ZK_SSL_PROTOCOL = "TLSv1.2";
    public static final String ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = "HTTPS";
    public static final boolean ZK_SSL_CRL_ENABLE = false;
    public static final boolean ZK_SSL_OCSP_ENABLE = false;

    // See ZKClientConfig.SECURE_CLIENT
    private static final String SECURE_CLIENT = "zookeeper.client.secure";
    // See ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET
    private static final String ZOOKEEPER_CLIENT_CNXN_SOCKET = "zookeeper.clientCnxnSocket";

    static {
        Map<String, String> zkSslConfigToSystemPropertyMap = new HashMap<>();

        zkSslConfigToSystemPropertyMap.put(ZK_SSL_CLIENT_ENABLE_CONFIG, SECURE_CLIENT);
        zkSslConfigToSystemPropertyMap.put(ZK_CLIENT_CNXN_SOCKET_CONFIG, ZOOKEEPER_CLIENT_CNXN_SOCKET);
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_KEY_STORE_LOCATION_CONFIG, "zookeeper.ssl.keyStore.location");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_KEY_STORE_PASSWORD_CONFIG, "zookeeper.ssl.keyStore.password");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_KEY_STORE_TYPE_CONFIG, "zookeeper.ssl.keyStore.type");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_TRUST_STORE_LOCATION_CONFIG, "zookeeper.ssl.trustStore.location");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_TRUST_STORE_PASSWORD_CONFIG, "zookeeper.ssl.trustStore.password");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_TRUST_STORE_TYPE_CONFIG, "zookeeper.ssl.trustStore.type");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_PROTOCOL_CONFIG, "zookeeper.ssl.protocol");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_ENABLED_PROTOCOLS_CONFIG, "zookeeper.ssl.enabledProtocols");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_CIPHER_SUITES_CONFIG, "zookeeper.ssl.ciphersuites");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "zookeeper.ssl.hostnameVerification");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_CRL_ENABLE_CONFIG, "zookeeper.ssl.crl");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_OCSP_ENABLE_CONFIG, "zookeeper.ssl.ocsp");

        ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP = Collections.unmodifiableMap(zkSslConfigToSystemPropertyMap);

        ZK_SSL_CLIENT_ENABLE_DOC = "Set client to use TLS when connecting to ZooKeeper." +
            " An explicit value overrides any value set via the <code>zookeeper.client.secure</code> system property (note the different name)." +
            " Defaults to false if neither is set; when true, <code>" + ZK_CLIENT_CNXN_SOCKET_CONFIG + "</code> must be set (typically to <code>org.apache.zookeeper.ClientCnxnSocketNetty</code>); other values to set may include " +
            ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.keySet().stream().filter(x -> !x.equals(ZK_SSL_CLIENT_ENABLE_CONFIG) && !x.equals(ZK_CLIENT_CNXN_SOCKET_CONFIG)).sorted().collect(Collectors.joining("<code>", "</code>, <code>", "</code>"));
        ZK_CLIENT_CNXN_SOCKET_DOC = "Typically set to <code>org.apache.zookeeper.ClientCnxnSocketNetty</code> when using TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the same-named <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_CLIENT_CNXN_SOCKET_CONFIG) + "</code> system property.";
        ZK_SSL_KEY_STORE_LOCATION_DOC = "Keystore location when using a client-side certificate with TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_KEY_STORE_LOCATION_CONFIG) + "</code> system property (note the camelCase).";
        ZK_SSL_KEY_STORE_PASSWORD_DOC = "Keystore password when using a client-side certificate with TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_KEY_STORE_PASSWORD_CONFIG) + "</code> system property (note the camelCase)." +
            " Note that ZooKeeper does not support a key password different from the keystore password, so be sure to set the key password in the keystore to be identical to the keystore password; otherwise the connection attempt to Zookeeper will fail.";
        ZK_SSL_KEY_STORE_TYPE_DOC = "Keystore type when using a client-side certificate with TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_KEY_STORE_TYPE_CONFIG) + "</code> system property (note the camelCase)." +
            " The default value of <code>null</code> means the type will be auto-detected based on the filename extension of the keystore.";
        ZK_SSL_TRUST_STORE_LOCATION_DOC = "Truststore location when using TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_TRUST_STORE_LOCATION_CONFIG) + "</code> system property (note the camelCase).";
        ZK_SSL_TRUST_STORE_PASSWORD_DOC = "Truststore password when using TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_TRUST_STORE_PASSWORD_CONFIG) + "</code> system property (note the camelCase).";
        ZK_SSL_TRUST_STORE_TYPE_DOC = "Truststore type when using TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_TRUST_STORE_TYPE_CONFIG) + "</code> system property (note the camelCase)." +
            " The default value of <code>null</code> means the type will be auto-detected based on the filename extension of the truststore.";
        ZK_SSL_PROTOCOL_DOC = "Specifies the protocol to be used in ZooKeeper TLS negotiation." +
            " An explicit value overrides any value set via the same-named <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_PROTOCOL_CONFIG) + "</code> system property.";
        ZK_SSL_ENABLED_PROTOCOLS_DOC = "Specifies the enabled protocol(s) in ZooKeeper TLS negotiation (csv)." +
            " Overrides any explicit value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_ENABLED_PROTOCOLS_CONFIG) + "</code> system property (note the camelCase)." +
            " The default value of <code>null</code> means the enabled protocol will be the value of the <code>" + ZK_SSL_PROTOCOL_CONFIG + "</code> configuration property.";
        ZK_SSL_CIPHER_SUITES_DOC = "Specifies the enabled cipher suites to be used in ZooKeeper TLS negotiation (csv)." +
            " Overrides any explicit value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_CIPHER_SUITES_CONFIG) + "</code> system property (note the single word \"ciphersuites\")." +
            " The default value of <code>null</code> means the list of enabled cipher suites is determined by the Java runtime being used.";
        ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC = "Specifies whether to enable hostname verification in the ZooKeeper TLS negotiation process, with (case-insensitively) \"https\" meaning ZooKeeper hostname verification is enabled and an explicit blank value meaning it is disabled (disabling it is only recommended for testing purposes)." +
            " An explicit value overrides any \"true\" or \"false\" value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG) + "</code> system property (note the different name and values; true implies https and false implies blank).";
        ZK_SSL_CRL_ENABLE_DOC = "Specifies whether to enable Certificate Revocation List in the ZooKeeper TLS protocols." +
            " Overrides any explicit value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_CRL_ENABLE_CONFIG) + "</code> system property (note the shorter name).";
        ZK_SSL_OCSP_ENABLE_DOC = "Specifies whether to enable Online Certificate Status Protocol in the ZooKeeper TLS protocols." +
            " Overrides any explicit value set via the <code>" + ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(ZK_SSL_OCSP_ENABLE_CONFIG) + "</code> system property (note the shorter name).";
    }

}
