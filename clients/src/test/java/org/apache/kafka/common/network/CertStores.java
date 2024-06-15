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

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestSslUtils;

import java.io.File;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.test.TestSslUtils.SslConfigsBuilder;
import org.apache.kafka.test.TestUtils;

public class CertStores {

    public static final Set<String> KEYSTORE_PROPS = Utils.mkSet(
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            SslConfigs.SSL_KEY_PASSWORD_CONFIG,
            SslConfigs.SSL_KEYSTORE_KEY_CONFIG,
            SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG);

    public static final Set<String> TRUSTSTORE_PROPS = Utils.mkSet(
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG);

    private final Map<String, Object> sslConfig;

    public CertStores(boolean server, String hostName) throws Exception {
        this(server, hostName, new TestSslUtils.CertificateBuilder());
    }

    public CertStores(boolean server, String commonName, String sanHostName) throws Exception {
        this(server, commonName, new TestSslUtils.CertificateBuilder().sanDnsNames(sanHostName));
    }

    private CertStores(boolean server, String commonName, TestSslUtils.CertificateBuilder certBuilder) throws Exception {
        this(server, commonName, "RSA", certBuilder, false);
    }

    private CertStores(boolean server, String commonName, String keyAlgorithm, TestSslUtils.CertificateBuilder certBuilder, boolean usePem) throws Exception {
        String name = server ? "server" : "client";
        Mode mode = server ? Mode.SERVER : Mode.CLIENT;
        File truststoreFile = usePem ? null : TestUtils.tempFile(name + "TS", ".jks");
        sslConfig = new SslConfigsBuilder(mode)
                .useClientCert(!server)
                .certAlias(name)
                .cn(commonName)
                .createNewTrustStore(truststoreFile)
                .certBuilder(certBuilder)
                .algorithm(keyAlgorithm)
                .usePem(usePem)
                .build();
    }


    public Map<String, Object> getTrustingConfig(CertStores truststoreConfig) {
        Map<String, Object> config = new HashMap<>(sslConfig);
        for (String propName : TRUSTSTORE_PROPS) {
            config.put(propName, truststoreConfig.sslConfig.get(propName));
        }
        return config;
    }

    public Map<String, Object> getUntrustingConfig() {
        return sslConfig;
    }

    public Map<String, Object> keyStoreProps() {
        Map<String, Object> props = new HashMap<>();
        for (String propName : KEYSTORE_PROPS) {
            props.put(propName, sslConfig.get(propName));
        }
        return props;
    }

    public Map<String, Object> trustStoreProps() {
        Map<String, Object> props = new HashMap<>();
        for (String propName : TRUSTSTORE_PROPS) {
            props.put(propName, sslConfig.get(propName));
        }
        return props;
    }

    public static class Builder {
        private final boolean isServer;
        private final List<String> sanDns;
        private String cn;
        private InetAddress sanIp;
        private String keyAlgorithm;
        private boolean usePem;

        public Builder(boolean isServer) {
            this.isServer = isServer;
            this.sanDns = new ArrayList<>();
            this.keyAlgorithm = "RSA";
        }

        public Builder cn(String cn) {
            this.cn = cn;
            return this;
        }

        public Builder addHostName(String hostname) {
            this.sanDns.add(hostname);
            return this;
        }

        public Builder hostAddress(InetAddress hostAddress) {
            this.sanIp = hostAddress;
            return this;
        }

        public Builder keyAlgorithm(String keyAlgorithm) {
            this.keyAlgorithm = keyAlgorithm;
            return this;
        }

        public Builder usePem(boolean usePem) {
            this.usePem = usePem;
            return this;
        }

        public CertStores build() throws Exception {
            TestSslUtils.CertificateBuilder certBuilder = new TestSslUtils.CertificateBuilder()
                .sanDnsNames(sanDns.toArray(new String[0]));
            if (sanIp != null)
                certBuilder = certBuilder.sanIpAddress(sanIp);
            return new CertStores(isServer, cn, keyAlgorithm, certBuilder, usePem);
        }
    }
}