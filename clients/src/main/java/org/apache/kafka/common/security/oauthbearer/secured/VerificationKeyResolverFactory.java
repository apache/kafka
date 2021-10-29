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

package org.apache.kafka.common.security.oauthbearer.secured;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;

import java.net.URL;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import javax.net.ssl.SSLSocketFactory;
import org.apache.kafka.common.utils.Time;
import org.jose4j.http.Get;
import org.jose4j.jwk.HttpsJwks;

public class VerificationKeyResolverFactory {

    /**
     * Create an {@link AccessTokenRetriever} from the given
     * {@link org.apache.kafka.common.config.SaslConfigs}.
     *
     * <b>Note</b>: the returned <code>CloseableVerificationKeyResolver</code> is not
     * initialized here and must be done by the caller.
     *
     * Primarily exposed here for unit testing.
     *
     * @param configs SASL configuration
     *
     * @return Non-<code>null</code> {@link CloseableVerificationKeyResolver}
     */
    public static CloseableVerificationKeyResolver create(Map<String, ?> configs,
        Map<String, Object> jaasConfig) {
        return create(configs, null, jaasConfig);
    }

    public static CloseableVerificationKeyResolver create(Map<String, ?> configs,
        String saslMechanism,
        Map<String, Object> jaasConfig) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        URL jwksEndpointUrl = cu.validateUrl(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL);

        if (jwksEndpointUrl.getProtocol().toLowerCase(Locale.ROOT).equals("file")) {
            Path p = cu.validateFile(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL);
            return new JwksFileVerificationKeyResolver(p);
        } else {
            long refreshIntervalMs = cu.validateLong(SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, true, 0L);
            JaasOptionsUtils jou = new JaasOptionsUtils(jaasConfig);
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

}