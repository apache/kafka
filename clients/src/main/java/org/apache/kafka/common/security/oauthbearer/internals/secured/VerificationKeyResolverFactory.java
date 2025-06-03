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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.utils.Time;

import org.jose4j.http.Get;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwx.JsonWebStructure;
import org.jose4j.lang.UnresolvableKeyException;

import java.io.IOException;
import java.net.URL;
import java.security.Key;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;

/**
 * Because a {@link CloseableVerificationKeyResolver} instance can spawn threads and issue
 * HTTP(S) calls ({@link RefreshingHttpsJwksVerificationKeyResolver}), we only want to create
 * a new instance for each particular set of configuration. Because each set of configuration
 * may have multiple instances, we want to reuse the single instance.
 */
public class VerificationKeyResolverFactory {

    private static final Map<VerificationKeyResolverKey, CloseableVerificationKeyResolver> CACHE = new HashMap<>();

    public static synchronized CloseableVerificationKeyResolver get(Map<String, ?> configs,
                                                                    String saslMechanism,
                                                                    List<AppConfigurationEntry> jaasConfigEntries) {
        VerificationKeyResolverKey key = new VerificationKeyResolverKey(configs, saslMechanism, jaasConfigEntries);

        return CACHE.computeIfAbsent(key, k ->
            new RefCountingVerificationKeyResolver(
                create(
                    configs,
                    saslMechanism,
                    jaasConfigEntries
                )
            )
        );
    }

    static CloseableVerificationKeyResolver create(Map<String, ?> configs,
                                                   String saslMechanism,
                                                   List<AppConfigurationEntry> jaasConfigEntries) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        URL jwksEndpointUrl = cu.validateUrl(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL);
        CloseableVerificationKeyResolver resolver;

        if (jwksEndpointUrl.getProtocol().toLowerCase(Locale.ROOT).equals("file")) {
            resolver = new JwksFileVerificationKeyResolver();
        } else {
            long refreshIntervalMs = cu.validateLong(SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, true, 0L);
            JaasOptionsUtils jou = new JaasOptionsUtils(saslMechanism, jaasConfigEntries);
            SSLSocketFactory sslSocketFactory = null;

            if (jou.shouldCreateSSLSocketFactory(jwksEndpointUrl))
                sslSocketFactory = jou.createSSLSocketFactory();

            HttpsJwks httpsJwks = new HttpsJwks(jwksEndpointUrl.toString());
            httpsJwks.setDefaultCacheDuration(refreshIntervalMs);

            if (sslSocketFactory != null) {
                Get get = new Get();
                get.setSslSocketFactory(sslSocketFactory);
                httpsJwks.setSimpleHttpGet(get);
            }

            RefreshingHttpsJwks refreshingHttpsJwks = new RefreshingHttpsJwks(Time.SYSTEM,
                httpsJwks,
                refreshIntervalMs,
                cu.validateLong(SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS),
                cu.validateLong(SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS));
            resolver = new RefreshingHttpsJwksVerificationKeyResolver(refreshingHttpsJwks);
        }

        resolver.configure(configs, saslMechanism, jaasConfigEntries);
        return resolver;
    }

    /**
     * <code>VkrKey</code> is a simple structure which encapsulates the criteria for different
     * sets of configuration. This will allow us to use this object as a key in a {@link Map}
     * to keep a single instance per key.
     */

    private static class VerificationKeyResolverKey {

        private final Map<String, ?> configs;

        private final String saslMechanism;

        private final Map<String, Object> moduleOptions;

        public VerificationKeyResolverKey(Map<String, ?> configs,
                                          String saslMechanism,
                                          List<AppConfigurationEntry> jaasConfigEntries) {
            this.configs = configs;
            this.saslMechanism = saslMechanism;
            this.moduleOptions = JaasOptionsUtils.getOptions(saslMechanism, jaasConfigEntries);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            VerificationKeyResolverKey that = (VerificationKeyResolverKey) o;
            return configs.equals(that.configs) && saslMechanism.equals(that.saslMechanism) && moduleOptions.equals(that.moduleOptions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(configs, saslMechanism, moduleOptions);
        }
    }

    /**
     * <code>RefCountingVerificationKeyResolver</code> allows us to share a single
     * {@link CloseableVerificationKeyResolver} instance between multiple
     * {@link AuthenticateCallbackHandler} instances and perform the lifecycle methods the
     * appropriate number of times.
     */

    private static class RefCountingVerificationKeyResolver implements CloseableVerificationKeyResolver {

        private final CloseableVerificationKeyResolver delegate;

        private final AtomicInteger count = new AtomicInteger(0);

        public RefCountingVerificationKeyResolver(CloseableVerificationKeyResolver delegate) {
            this.delegate = delegate;
        }

        @Override
        public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
            return delegate.resolveKey(jws, nestingContext);
        }

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
            if (count.incrementAndGet() == 1)
                delegate.configure(configs, saslMechanism, jaasConfigEntries);
        }

        @Override
        public void close() throws IOException {
            if (count.decrementAndGet() == 0)
                delegate.close();
        }
    }
}