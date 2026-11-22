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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.security.Key;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

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
 *
 * <p>Each call to {@link #get(Map, String, List)} acquires a reference to the shared resolver: the
 * caller must call {@link CloseableVerificationKeyResolver#configure(Map, String, List)} before use
 * and {@link CloseableVerificationKeyResolver#close()} when finished with it. The underlying resolver
 * is configured by the first {@code configure()} call and closed when the last reference is closed.
 */
public class VerificationKeyResolverFactory {

    private static final Logger LOG = LoggerFactory.getLogger(VerificationKeyResolverFactory.class);

    private static final Map<VerificationKeyResolverKey, RefCountingVerificationKeyResolver> CACHE = new HashMap<>();

    public static CloseableVerificationKeyResolver get(Map<String, ?> configs,
                                                       String saslMechanism,
                                                       List<AppConfigurationEntry> jaasConfigEntries) {
        return get(configs, saslMechanism, jaasConfigEntries, () -> create(configs, saslMechanism, jaasConfigEntries));
    }

    /*
     * Package-visible for testing, so that tests can supply the delegate instead of creating it from the
     * configuration.
     */
    static synchronized CloseableVerificationKeyResolver get(Map<String, ?> configs,
                                                             String saslMechanism,
                                                             List<AppConfigurationEntry> jaasConfigEntries,
                                                             Supplier<CloseableVerificationKeyResolver> delegateSupplier) {
        VerificationKeyResolverKey key = new VerificationKeyResolverKey(configs, saslMechanism, jaasConfigEntries);

        RefCountingVerificationKeyResolver resolver = CACHE.computeIfAbsent(key, k ->
            new RefCountingVerificationKeyResolver(delegateSupplier.get(), k));

        // Acquire the reference while holding the lock, so that another caller closing its own reference
        // cannot close the resolver before this caller uses it.
        resolver.refCount++;
        return resolver;
    }

    /*
     * Releases a reference acquired by get(). Returns true if it was the last reference, in which case the
     * resolver has been removed from the cache and the caller must close it.
     */
    private static synchronized boolean release(RefCountingVerificationKeyResolver resolver) {
        if (--resolver.refCount != 0)
            return false;

        CACHE.remove(resolver.cacheKey, resolver);
        return true;
    }

    static CloseableVerificationKeyResolver create(Map<String, ?> configs,
                                                   String saslMechanism,
                                                   List<AppConfigurationEntry> jaasConfigEntries) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        URL jwksEndpointUrl = cu.validateUrl(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL);

        if (jwksEndpointUrl.getProtocol().toLowerCase(Locale.ROOT).equals("file")) {
            return new JwksFileVerificationKeyResolver();
        } else {
            if (jwksEndpointUrl.getProtocol().equalsIgnoreCase("http"))
                LOG.warn("The {} configuration uses an HTTP URL, so the keys used to verify tokens are retrieved without TLS. " +
                    "Use an HTTPS URL instead.", SASL_OAUTHBEARER_JWKS_ENDPOINT_URL);

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
            return new RefreshingHttpsJwksVerificationKeyResolver(refreshingHttpsJwks);
        }
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

        private final VerificationKeyResolverKey cacheKey;

        // Guarded by the VerificationKeyResolverFactory class lock, so that references are acquired and
        // released atomically with the cache lookup and removal.
        private int refCount;

        // Guarded by this.
        private boolean configured;

        public RefCountingVerificationKeyResolver(CloseableVerificationKeyResolver delegate, VerificationKeyResolverKey cacheKey) {
            this.delegate = delegate;
            this.cacheKey = cacheKey;
        }

        @Override
        public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
            return delegate.resolveKey(jws, nestingContext);
        }

        @Override
        public synchronized void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
            // Only the first call configures the delegate. Concurrent callers wait here until it is done,
            // so none of them uses the delegate before it is configured.
            if (!configured) {
                delegate.configure(configs, saslMechanism, jaasConfigEntries);
                configured = true;
            }
        }

        @Override
        public void close() throws IOException {
            // Releasing the last reference also drops the cache entry, so that a later get() with the same
            // configuration builds a new resolver rather than returning this closed one.
            if (release(this))
                delegate.close();
        }
    }
}