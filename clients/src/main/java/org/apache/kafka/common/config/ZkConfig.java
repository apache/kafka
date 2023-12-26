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

package org.apache.kafka.common.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** ********* Zookeeper Configuration ***********/
public class ZkConfig {
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

    // a map from the Kafka config to the corresponding ZooKeeper Java system property
    public static final Map<String, String> ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP;

    static {
        Map<String, String> zkSslConfigToSystemPropertyMap = new HashMap<>();

        zkSslConfigToSystemPropertyMap.put(ZK_SSL_CLIENT_ENABLE_CONFIG, "zookeeper.client.secure");
        zkSslConfigToSystemPropertyMap.put(ZK_CLIENT_CNXN_SOCKET_CONFIG, "zookeeper.clientCnxnSocket");
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
    }
}
