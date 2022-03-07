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

import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.SCOPE_CONFIG;

import java.net.URL;
import java.util.Locale;
import java.util.Map;
import javax.net.ssl.SSLSocketFactory;

public class AccessTokenRetrieverFactory  {

    /**
     * Create an {@link AccessTokenRetriever} from the given SASL and JAAS configuration.
     *
     * <b>Note</b>: the returned <code>AccessTokenRetriever</code> is <em>not</em> initialized
     * here and must be done by the caller prior to use.
     *
     * @param configs    SASL configuration
     * @param jaasConfig JAAS configuration
     *
     * @return Non-<code>null</code> {@link AccessTokenRetriever}
     */

    public static AccessTokenRetriever create(Map<String, ?> configs, Map<String, Object> jaasConfig) {
        return create(configs, null, jaasConfig);
    }

    public static AccessTokenRetriever create(Map<String, ?> configs,
        String saslMechanism,
        Map<String, Object> jaasConfig) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        URL tokenEndpointUrl = cu.validateUrl(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

        if (tokenEndpointUrl.getProtocol().toLowerCase(Locale.ROOT).equals("file")) {
            return new FileTokenRetriever(cu.validateFile(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL));
        } else {
            JaasOptionsUtils jou = new JaasOptionsUtils(jaasConfig);
            String clientId = jou.validateString(CLIENT_ID_CONFIG);
            String clientSecret = jou.validateString(CLIENT_SECRET_CONFIG);
            String scope = jou.validateString(SCOPE_CONFIG, false);

            SSLSocketFactory sslSocketFactory = null;

            if (jou.shouldCreateSSLSocketFactory(tokenEndpointUrl))
                sslSocketFactory = jou.createSSLSocketFactory();

            return new HttpAccessTokenRetriever(clientId,
                clientSecret,
                scope,
                sslSocketFactory,
                tokenEndpointUrl.toString(),
                cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MS),
                cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MAX_MS),
                cu.validateInteger(SASL_LOGIN_CONNECT_TIMEOUT_MS, false),
                cu.validateInteger(SASL_LOGIN_READ_TIMEOUT_MS, false));
        }
    }

}