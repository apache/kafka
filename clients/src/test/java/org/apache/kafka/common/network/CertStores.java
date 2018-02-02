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
package org.apache.kafka.common.network;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.test.TestSslUtils;

import java.io.File;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class CertStores {

    private final Map<String, Object> sslConfig;

    public CertStores(boolean server, String hostName) throws Exception {
        this(server, hostName, new TestSslUtils.CertificateBuilder());
    }

    public CertStores(boolean server, String commonName, String sanHostName) throws Exception {
        this(server, commonName, new TestSslUtils.CertificateBuilder().sanDnsName(sanHostName));
    }

    public CertStores(boolean server, String commonName, InetAddress hostAddress) throws Exception {
        this(server, commonName, new TestSslUtils.CertificateBuilder().sanIpAddress(hostAddress));
    }

    private CertStores(boolean server, String commonName, TestSslUtils.CertificateBuilder certBuilder) throws Exception {
        String name = server ? "server" : "client";
        Mode mode = server ? Mode.SERVER : Mode.CLIENT;
        File truststoreFile = File.createTempFile(name + "TS", ".jks");
        sslConfig = TestSslUtils.createSslConfig(!server, true, mode, truststoreFile, name, commonName, certBuilder);
    }

    public Map<String, Object> getTrustingConfig(CertStores truststoreConfig) {
        Map<String, Object> config = new HashMap<>(sslConfig);
        config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreConfig.sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststoreConfig.sslConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, truststoreConfig.sslConfig.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
        return config;
    }

    public Map<String, Object> getUntrustingConfig() {
        return sslConfig;
    }
}