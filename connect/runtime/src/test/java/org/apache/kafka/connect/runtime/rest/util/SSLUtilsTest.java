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
package org.apache.kafka.connect.runtime.rest.util;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.CertStores;
import org.apache.kafka.connect.runtime.rest.RestServerConfig;

import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SSLUtilsTest {

    private Map<String, Object> sslConfig;
    private String keystorePath;
    private String truststorePath;
    private Password keystorePassword;
    private Password truststorePassword;

    @BeforeEach
    public void before() throws Exception {
        CertStores serverCertStores = new CertStores(true, "localhost");
        sslConfig = serverCertStores.getUntrustingConfig();
        keystorePath = sslConfig.get("ssl.keystore.location").toString();
        truststorePath = sslConfig.get("ssl.truststore.location").toString();
        keystorePassword = (Password) sslConfig.get("ssl.keystore.password");
        truststorePassword = (Password) sslConfig.get("ssl.keystore.password");
    }

    @Test
    public void testGetOrDefault() {
        String existingKey = "exists";
        String missingKey = "missing";
        String value = "value";
        String defaultValue = "default";
        Map<String, Object> map = new HashMap<>();
        map.put("exists", "value");

        assertEquals(SSLUtils.getOrDefault(map, existingKey, defaultValue), value);
        assertEquals(SSLUtils.getOrDefault(map, missingKey, defaultValue), defaultValue);
    }

    @Test
    public void testCreateServerSideSslContextFactory() throws Exception {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("ssl.keystore.location", keystorePath);
        configMap.put("ssl.keystore.password", keystorePassword.value());
        configMap.put("ssl.key.password", keystorePassword.value());
        configMap.put("ssl.truststore.location", truststorePath);
        configMap.put("ssl.truststore.password", truststorePassword.value());
        configMap.put("ssl.provider", "SunJSSE");
        configMap.put("ssl.cipher.suites", "SSL_RSA_WITH_RC4_128_SHA,SSL_RSA_WITH_RC4_128_MD5");
        configMap.put("ssl.secure.random.implementation", "SHA1PRNG");
        configMap.put("ssl.client.auth", "required");
        configMap.put("ssl.endpoint.identification.algorithm", "HTTPS");
        configMap.put("ssl.keystore.type", "JKS");
        configMap.put("ssl.protocol", "TLS");
        configMap.put("ssl.truststore.type", "JKS");
        configMap.put("ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1");
        configMap.put("ssl.keymanager.algorithm", "SunX509");
        configMap.put("ssl.trustmanager.algorithm", "PKIX");

        RestServerConfig config = RestServerConfig.forPublic(null, configMap);
        SslContextFactory.Server ssl = SSLUtils.createServerSideSslContextFactory(config);

        assertEquals("file://" + keystorePath, ssl.getKeyStorePath());
        assertEquals("file://" + truststorePath, ssl.getTrustStorePath());
        assertEquals("SunJSSE", ssl.getProvider());
        assertArrayEquals(new String[] {"SSL_RSA_WITH_RC4_128_SHA", "SSL_RSA_WITH_RC4_128_MD5"}, ssl.getIncludeCipherSuites());
        assertEquals("SHA1PRNG", ssl.getSecureRandomAlgorithm());
        assertTrue(ssl.getNeedClientAuth());
        assertFalse(ssl.getWantClientAuth());
        assertEquals("JKS", ssl.getKeyStoreType());
        assertEquals("JKS", ssl.getTrustStoreType());
        assertEquals("TLS", ssl.getProtocol());
        assertArrayEquals(new String[] {"TLSv1.2", "TLSv1.1", "TLSv1"}, ssl.getIncludeProtocols());
        assertEquals("SunX509", ssl.getKeyManagerFactoryAlgorithm());
        assertEquals("PKIX", ssl.getTrustManagerFactoryAlgorithm());
    }

    @Test
    public void testCreateClientSideSslContextFactory() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("ssl.keystore.location", keystorePath);
        configMap.put("ssl.keystore.password", keystorePassword.value());
        configMap.put("ssl.key.password", keystorePassword.value());
        configMap.put("ssl.truststore.location", truststorePath);
        configMap.put("ssl.truststore.password", truststorePassword.value());
        configMap.put("ssl.provider", "SunJSSE");
        configMap.put("ssl.cipher.suites", "SSL_RSA_WITH_RC4_128_SHA,SSL_RSA_WITH_RC4_128_MD5");
        configMap.put("ssl.secure.random.implementation", "SHA1PRNG");
        configMap.put("ssl.client.auth", "required");
        configMap.put("ssl.endpoint.identification.algorithm", "HTTPS");
        configMap.put("ssl.keystore.type", "JKS");
        configMap.put("ssl.protocol", "TLS");
        configMap.put("ssl.truststore.type", "JKS");
        configMap.put("ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1");
        configMap.put("ssl.keymanager.algorithm", "SunX509");
        configMap.put("ssl.trustmanager.algorithm", "PKIX");

        RestServerConfig config = RestServerConfig.forPublic(null, configMap);
        SslContextFactory.Client ssl = SSLUtils.createClientSideSslContextFactory(config);

        assertEquals("file://" + keystorePath, ssl.getKeyStorePath());
        assertEquals("file://" + truststorePath, ssl.getTrustStorePath());
        assertEquals("SunJSSE", ssl.getProvider());
        assertArrayEquals(new String[] {"SSL_RSA_WITH_RC4_128_SHA", "SSL_RSA_WITH_RC4_128_MD5"}, ssl.getIncludeCipherSuites());
        assertEquals("SHA1PRNG", ssl.getSecureRandomAlgorithm());
        assertEquals("JKS", ssl.getKeyStoreType());
        assertEquals("JKS", ssl.getTrustStoreType());
        assertEquals("TLS", ssl.getProtocol());
        assertArrayEquals(new String[] {"TLSv1.2", "TLSv1.1", "TLSv1"}, ssl.getIncludeProtocols());
        assertEquals("SunX509", ssl.getKeyManagerFactoryAlgorithm());
        assertEquals("PKIX", ssl.getTrustManagerFactoryAlgorithm());
    }

    @Test
    public void testCreateServerSideSslContextFactoryDefaultValues() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("ssl.keystore.location", keystorePath);
        configMap.put("ssl.keystore.password", keystorePassword.value());
        configMap.put("ssl.key.password", keystorePassword.value());
        configMap.put("ssl.truststore.location", truststorePath);
        configMap.put("ssl.truststore.password", truststorePassword.value());
        configMap.put("ssl.provider", "SunJSSE");
        configMap.put("ssl.cipher.suites", "SSL_RSA_WITH_RC4_128_SHA,SSL_RSA_WITH_RC4_128_MD5");
        configMap.put("ssl.secure.random.implementation", "SHA1PRNG");

        RestServerConfig config = RestServerConfig.forPublic(null, configMap);
        SslContextFactory.Server ssl = SSLUtils.createServerSideSslContextFactory(config);

        assertEquals(SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE, ssl.getKeyStoreType());
        assertEquals(SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, ssl.getTrustStoreType());
        assertEquals(SslConfigs.DEFAULT_SSL_PROTOCOL, ssl.getProtocol());
        assertArrayEquals(Arrays.asList(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS.split("\\s*,\\s*")).toArray(), ssl.getIncludeProtocols());
        assertEquals(SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM, ssl.getKeyManagerFactoryAlgorithm());
        assertEquals(SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM, ssl.getTrustManagerFactoryAlgorithm());
        assertFalse(ssl.getNeedClientAuth());
        assertFalse(ssl.getWantClientAuth());
    }

    @Test
    public void testCreateClientSideSslContextFactoryDefaultValues() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("ssl.keystore.location", keystorePath);
        configMap.put("ssl.keystore.password", keystorePassword.value());
        configMap.put("ssl.key.password", keystorePassword.value());
        configMap.put("ssl.truststore.location", truststorePath);
        configMap.put("ssl.truststore.password", truststorePassword.value());
        configMap.put("ssl.provider", "SunJSSE");
        configMap.put("ssl.cipher.suites", "SSL_RSA_WITH_RC4_128_SHA,SSL_RSA_WITH_RC4_128_MD5");
        configMap.put("ssl.secure.random.implementation", "SHA1PRNG");

        RestServerConfig config = RestServerConfig.forPublic(null, configMap);
        SslContextFactory.Client ssl = SSLUtils.createClientSideSslContextFactory(config);

        assertEquals(SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE, ssl.getKeyStoreType());
        assertEquals(SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, ssl.getTrustStoreType());
        assertEquals(SslConfigs.DEFAULT_SSL_PROTOCOL, ssl.getProtocol());
        assertArrayEquals(Arrays.asList(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS.split("\\s*,\\s*")).toArray(), ssl.getIncludeProtocols());
        assertEquals(SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM, ssl.getKeyManagerFactoryAlgorithm());
        assertEquals(SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM, ssl.getTrustManagerFactoryAlgorithm());
    }
}
