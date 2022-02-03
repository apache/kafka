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

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.security.KeyStore;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLEngine;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an implementation of a {@link SslEngineFactory} that exists to enable the reloading of renewed certificates by
 * producer and consumer clients. It achieves this by recreating an internal delegate {@link SslEngineFactory} when necessary, so
 * that the new delegate instance is able to acquire and use the most recent certificate information.
 *
 * The new {@link SslEngineFactory} delegate will be used to set up the SSL context for any new connections created
 * by Kafka Producer/Consumer instances.
 *
 * Enable this factory by setting kafka config {@link org.apache.kafka.common.config.SslConfigs#SSL_ENGINE_FACTORY_CLASS_CONFIG}
 * to {@link ReloadingSslEngineFactory#getClass()}
 *
 * Specify the delegate {@link SslEngineFactory} by setting kafka config {@link org.apache.kafka.common.config.SslConfigs#SSL_RELOADING_ENGINE_FACTORY_CLASS_CONFIG}
 * to your chosen implementation; this will default to the standard {@link DefaultSslEngineFactory}.
 *
 * The reload interval can be configured using kafka config {@link SslConfigs#SSL_RELOADING_ENGINE_FACTORY_INTERVAL_CONFIG}.
 * The default reload interval is {@link ReloadingSslEngineFactory#DEFAULT_SSL_RELOADING_ENGINE_FACTORY_INTERVAL} (12 hours).
 * With an interval less than zero, config will never automatically reload.
 * With an interval equal to zero, config will reload for every new connection.
 *
 * JKS certificates from the filesystem will be automatically reloaded at the specified interval, with no further intervention (as long as
 * some external actor periodically updates the JKS stores in the correct location.
 *
 * Inline PEM certificates will not be automatically reloaded as these form part of the configuration map. In this case, you (the client)
 * are responsible for calling {@link ReloadingSslEngineFactory#applyPatch(String, Map)} to provide patch the existing configuration.
 * The new patch will be merged with the initial configuration and applied for any new connections.
 * If you need multiple configurations, you can specify the {@code ID} for patching using the configuration option
 * {@link SslConfigs#SSL_RELOADING_ENGINE_FACTORY_ID_CONFIG}, otherwise {@link ReloadingSslEngineFactory#DEFAULT_SSL_RELOADING_ENGINE_FACTORY_ID} will be used.
 *
 * Note that this implementation uses {@link System#currentTimeMillis()} to track expiry, which
 * will cause unpredictable behaviour if time moves backwards.
 *
 * See also <a href="https://issues.apache.org/jira/browse/KAFKA-13293">KAFKA-13293</a>
 *
 * @author Joe Jordan
 * @author Kevin Nguyen
 * @author Elliot West
 */
public class ReloadingSslEngineFactory implements SslEngineFactory {

    public static final Class<? extends SslEngineFactory> DEFAULT_SSL_RELOADING_ENGINE_FACTORY_CLASS = DefaultSslEngineFactory.class;
    public static final String DEFAULT_SSL_RELOADING_ENGINE_FACTORY_ID = ReloadingSslEngineFactory.class.getCanonicalName();
    public static final long DEFAULT_SSL_RELOADING_ENGINE_FACTORY_INTERVAL = Duration.ofHours(12).toMillis();

    private static final Logger LOG = LoggerFactory.getLogger(ReloadingSslEngineFactory.class);
    private static final Map<String, Map<String, Object>> CONFIG_PATCHES = new ConcurrentHashMap<>();

    private final Object lock = new Object();
    private final Clock clock;
    // default for test access
    SslEngineFactory delegate;
    private String id;
    private Long reloadInterval;
    private Map<String, Object> baseConfig;
    private Map<String, Object> configPatch;
    private long expiry;

    public ReloadingSslEngineFactory() {
        this(Clock.systemUTC());
    }

    ReloadingSslEngineFactory(Clock clock) {
        this.clock = clock;
    }

    static void clearPatches() {
        CONFIG_PATCHES.clear();
    }

    public static void applyPatch(Map<String, ?> patch) {
        applyPatch(DEFAULT_SSL_RELOADING_ENGINE_FACTORY_ID, patch);
    }

    public static void applyPatch(String factoryId, Map<String, ?> patch) {
        final ConfigDef configDef = new ConfigDef()
                .withClientSslSupport();
        final Map<String, Object> configPatch = configDef.parse(patch)
                .entrySet()
                .stream()
                .filter(config -> config.getValue() != null)
                .filter(config -> SslConfigs.RECONFIGURABLE_CONFIGS.contains(config.getKey()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        CONFIG_PATCHES.put(factoryId, unmodifiableMap(configPatch));
    }

    private static SslEngineFactory newDelegateSslEngineFactory(Map<String, ?> config) {
        @SuppressWarnings("unchecked")
        Class<? extends SslEngineFactory> sslEngineFactoryClass =
                (Class<? extends SslEngineFactory>) config.get(SslConfigs.SSL_RELOADING_ENGINE_FACTORY_CLASS_CONFIG);
        if (sslEngineFactoryClass == null) {
            sslEngineFactoryClass = DEFAULT_SSL_RELOADING_ENGINE_FACTORY_CLASS;
        }
        return Utils.newInstance(sslEngineFactoryClass);
    }

    private static long reloadInterval(Map<String, ?> config) {
        if (config != null) {
            final Object configuredReloadInterval = config.get(SslConfigs.SSL_RELOADING_ENGINE_FACTORY_INTERVAL_CONFIG);
            if (configuredReloadInterval instanceof Number) {
                return ((Number) configuredReloadInterval).longValue();
            } else if (configuredReloadInterval instanceof String) {
                return Long.parseLong((String) configuredReloadInterval);
            }
        }
        return DEFAULT_SSL_RELOADING_ENGINE_FACTORY_INTERVAL;
    }

    private static String id(Map<String, ?> config) {
        if (config != null) {
            final Object factoryId = config.get(SslConfigs.SSL_RELOADING_ENGINE_FACTORY_ID_CONFIG);
            if (factoryId instanceof String) {
                return (String) factoryId;
            }
        }
        return DEFAULT_SSL_RELOADING_ENGINE_FACTORY_ID;
    }

    @Override
    public void configure(Map<String, ?> config) {
        synchronized (lock) {
            baseConfig = unmodifiableMap(config);
            id = id(baseConfig);
            reloadInterval = reloadInterval(baseConfig);
            renewDelegate();
        }
    }

    private SslEngineFactory delegate() {
        synchronized (lock) {
            if (delegate == null || clock.millis() >= expiry || CONFIG_PATCHES.get(id) != configPatch) {
                renewDelegate();
            }
            return delegate;
        }
    }

    private void renewDelegate() {
        synchronized (lock) {
            final SslEngineFactory newDelegate = newDelegateSslEngineFactory(baseConfig);
            if (baseConfig != null) {
                configPatch = CONFIG_PATCHES.get(id);
                final Map<String, Object> patchedConfig;
                if (configPatch == null || configPatch.isEmpty()) {
                    patchedConfig = baseConfig;
                } else {
                    patchedConfig = new HashMap<>(baseConfig);
                    patchedConfig.putAll(configPatch);
                }
                newDelegate.configure(patchedConfig);
            }
            delegate = newDelegate;
            if (reloadInterval < 0) {
                expiry = Long.MAX_VALUE;
            } else {
                expiry = clock.millis() + reloadInterval;
            }
            LOG.info("Created new {} - valid until {}.", delegate.getClass().getSimpleName(), Instant.ofEpochMilli(expiry));
        }
    }

    @Override
    public SSLEngine createClientSslEngine(String peerHost, int peerPort, String endpointIdentification) {
        return delegate().createClientSslEngine(peerHost, peerPort, endpointIdentification);
    }

    @Override
    public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
        return delegate().createServerSslEngine(peerHost, peerPort);
    }

    @Override
    public boolean shouldBeRebuilt(Map<String, Object> nextConfigs) {
        return delegate().shouldBeRebuilt(nextConfigs);
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return delegate().reconfigurableConfigs();
    }

    @Override
    public KeyStore keystore() {
        return delegate().keystore();
    }

    @Override
    public KeyStore truststore() {
        return delegate().truststore();
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            if (delegate != null) {
                delegate.close();
            }
        }
    }

}
