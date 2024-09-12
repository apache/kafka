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

import org.apache.kafka.common.config.SaslConfigs;

import java.net.URL;
import java.util.Locale;
import java.util.Map;

import javax.net.ssl.SSLSocketFactory;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.SCOPE_CONFIG;

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

            boolean urlencodeHeader = validateUrlencodeHeader(cu);

            return new HttpAccessTokenRetriever(clientId,
                clientSecret,
                scope,
                sslSocketFactory,
                tokenEndpointUrl.toString(),
                cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MS),
                cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MAX_MS),
                cu.validateInteger(SASL_LOGIN_CONNECT_TIMEOUT_MS, false),
                cu.validateInteger(SASL_LOGIN_READ_TIMEOUT_MS, false),
                urlencodeHeader);
        }
    }

    /**
     * In some cases, the incoming {@link Map} doesn't contain a value for
     * {@link SaslConfigs#SASL_OAUTHBEARER_HEADER_URLENCODE}. Returning {@code null} from {@link Map#get(Object)}
     * will cause a {@link NullPointerException} when it is later unboxed.
     *
     * <p/>
     *
     * This utility method ensures that we have a non-{@code null} value to use in the
     * {@link HttpAccessTokenRetriever} constructor.
     */
    static boolean validateUrlencodeHeader(ConfigurationUtils configurationUtils) {
        Boolean urlencodeHeader = configurationUtils.validateBoolean(SASL_OAUTHBEARER_HEADER_URLENCODE, false);

        if (urlencodeHeader != null)
            return urlencodeHeader;
        else
            return DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
    }

}