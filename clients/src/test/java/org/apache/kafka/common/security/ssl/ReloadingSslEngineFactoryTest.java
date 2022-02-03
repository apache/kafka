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
package org.apache.kafka.common.security.ssl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.kafka.common.security.ssl.mock.TestSslEngineFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ReloadingSslEngineFactoryTest {

    private final Map<String, Object> config = new HashMap<>();
    private Clock clock;
    private ReloadingSslEngineFactory factory;

    @BeforeEach
    public void setUp() {
        ReloadingSslEngineFactory.clearPatches();
        clock = Mockito.mock(Clock.class);
        config.put(SslConfigs.SSL_RELOADING_ENGINE_FACTORY_CLASS_CONFIG, TestSslEngineFactory.class);
        config.put(SslConfigs.SSL_RELOADING_ENGINE_FACTORY_INTERVAL_CONFIG, 100L);
        config.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
        config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        factory = new ReloadingSslEngineFactory(clock);
    }

    @Test
    public void constructsDefaultDelegate() {
        config.remove(SslConfigs.SSL_RELOADING_ENGINE_FACTORY_CLASS_CONFIG);
        config.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        factory.configure(config);
        assertTrue(factory.delegate instanceof DefaultSslEngineFactory);
    }

    @Test
    public void constructsConfiguredDelegate() {
        factory.configure(config);
        assertTrue(factory.delegate instanceof TestSslEngineFactory);
        verify(((TestSslEngineFactory) factory.delegate).internalMock, times(1)).configure(config);
    }

    @Test
    public void keystoreDelegation() {
        factory.configure(config);
        when(clock.millis()).thenReturn(1L);
        factory.keystore();
        verify(((TestSslEngineFactory) factory.delegate).internalMock, times(1)).keystore();
    }

    @Test
    public void truststoreDelegation() {
        factory.configure(config);
        when(clock.millis()).thenReturn(1L);
        factory.truststore();
        verify(((TestSslEngineFactory) factory.delegate).internalMock, times(1)).truststore();
    }

    @Test
    public void closeDelegation() throws IOException {
        factory.configure(config);
        when(clock.millis()).thenReturn(1L);
        factory.close();
        verify(((TestSslEngineFactory) factory.delegate).internalMock, times(1)).close();
    }

    @Test
    public void reconfigurableConfigsDelegation() {
        factory.configure(config);
        when(clock.millis()).thenReturn(1L);
        factory.reconfigurableConfigs();
        verify(((TestSslEngineFactory) factory.delegate).internalMock, times(1)).reconfigurableConfigs();
    }

    @Test
    public void createClientSslEngineDelegation() {
        factory.configure(config);
        when(clock.millis()).thenReturn(1L);
        factory.createClientSslEngine("host", 1, "endpoint");
        verify(((TestSslEngineFactory) factory.delegate).internalMock, times(1)).
                createClientSslEngine("host", 1, "endpoint");
    }

    @Test
    public void createServerSslEngineDelegation() {
        factory.configure(config);
        when(clock.millis()).thenReturn(1L);
        factory.createServerSslEngine("host", 1);
        verify(((TestSslEngineFactory) factory.delegate).internalMock, times(1)).
                createServerSslEngine("host", 1);
    }

    @Test
    public void reloadOnExpire() {
        factory.configure(config);
        // First delegation will be at T1, second will be at T101.
        // We expect first delegate to expire at T100.
        when(clock.millis()).thenReturn(1L, 101L);

        factory.keystore();
        SslEngineFactory delegate1 = factory.delegate;
        factory.keystore();
        SslEngineFactory delegate2 = factory.delegate;

        assertNotNull(delegate1);
        assertNotNull(delegate2);
        assertNotEquals(delegate1, delegate2);
    }

    @Test
    public void reuseUnexpiredDelegate() {
        factory.configure(config);
        // First delegation will be at T1, second will be at T99.
        // We expect first delegate to expire at T100.
        when(clock.millis()).thenReturn(1L, 99L);

        factory.keystore();
        SslEngineFactory delegate1 = factory.delegate;
        factory.keystore();
        SslEngineFactory delegate2 = factory.delegate;

        assertNotNull(delegate1);
        assertNotNull(delegate2);
        assertEquals(delegate1, delegate2);
    }

    @Test
    public void reloadOnPatch() {
        factory.configure(config);
        when(clock.millis()).thenReturn(1L);

        factory.keystore();
        SslEngineFactory delegate1 = factory.delegate;

        Map<String, Object> patch = new HashMap<>();
        patch.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, new Password("value"));
        ReloadingSslEngineFactory.applyPatch(patch);

        factory.keystore();
        SslEngineFactory delegate2 = factory.delegate;

        Map<String, Object> patchedConfig = new HashMap<>(config);
        patchedConfig.putAll(patch);

        verify(((TestSslEngineFactory) factory.delegate).internalMock, times(1)).
                configure(patchedConfig);

        assertNotNull(delegate1);
        assertNotNull(delegate2);
        assertNotEquals(delegate1, delegate2);
    }

}
