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
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
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

/**
 * {@code DefaultJwtRetriever} instantiates and delegates {@link JwtRetriever} API calls to an embedded implementation
 * based on configuration. If {@link SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL} is configured with a
 * {@code file}-based URL, a {@link FileJwtRetriever} is created and the JWT is expected be contained in the file
 * specified. Otherwise, it's assumed to be an HTTP/HTTPS-based URL, so an {@link HttpJwtRetriever} is created.
 */
public class DefaultJwtRetriever implements JwtRetriever {

    private final Map<String, ?> configs;
    private final String saslMechanism;
    private final Map<String, Object> jaasConfig;

    private JwtRetriever delegate;

    public DefaultJwtRetriever(Map<String, ?> configs, String saslMechanism, Map<String, Object> jaasConfig) {
        this.configs = configs;
        this.saslMechanism = saslMechanism;
        this.jaasConfig = jaasConfig;
    }

    @Override
    public void init() throws IOException {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        URL tokenEndpointUrl = cu.validateUrl(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

        if (tokenEndpointUrl.getProtocol().toLowerCase(Locale.ROOT).equals("file")) {
            delegate = new FileJwtRetriever(cu.validateFile(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL));
        } else {
            JaasOptionsUtils jou = new JaasOptionsUtils(jaasConfig);
            String clientId = jou.validateString(CLIENT_ID_CONFIG);
            String clientSecret = jou.validateString(CLIENT_SECRET_CONFIG);
            String scope = jou.validateString(SCOPE_CONFIG, false);

            SSLSocketFactory sslSocketFactory = null;

            if (jou.shouldCreateSSLSocketFactory(tokenEndpointUrl))
                sslSocketFactory = jou.createSSLSocketFactory();

            boolean urlencodeHeader = validateUrlencodeHeader(cu);

            delegate = new HttpJwtRetriever(clientId,
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

        delegate.init();
    }

    @Override
    public String retrieve() throws IOException {
        if (delegate == null)
            throw new IllegalStateException("JWT retriever delegate is null; please call init() first");

        return delegate.retrieve();
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(delegate, "JWT retriever delegate");
    }

    /**
     * In some cases, the incoming {@link Map} doesn't contain a value for
     * {@link SaslConfigs#SASL_OAUTHBEARER_HEADER_URLENCODE}. Returning {@code null} from {@link Map#get(Object)}
     * will cause a {@link NullPointerException} when it is later unboxed.
     *
     * <p/>
     *
     * This utility method ensures that we have a non-{@code null} value to use in the
     * {@link HttpJwtRetriever} constructor.
     */
    static boolean validateUrlencodeHeader(ConfigurationUtils configurationUtils) {
        Boolean urlencodeHeader = configurationUtils.get(SASL_OAUTHBEARER_HEADER_URLENCODE);

        if (urlencodeHeader != null)
            return urlencodeHeader;
        else
            return DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
    }

    JwtRetriever delegate() {
        return delegate;
    }
}