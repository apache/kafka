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

package org.apache.kafka.common.security.oauthbearer;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.login.AppConfigurationEntry;

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

public class ClientCredentialsJwtRetriever implements JwtRetriever {

    private JwtRetriever delegate;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        URL tokenEndpointUrl = cu.validateUrl(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

        JaasOptionsUtils jou = new JaasOptionsUtils(JaasOptionsUtils.getOptions(saslMechanism, jaasConfigEntries));
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

        delegate.configure(configs, saslMechanism, jaasConfigEntries);
    }

    @Override
    public String retrieve() throws JwtRetrieverException {
        if (delegate == null)
            throw new IllegalStateException("JWT retriever delegate is null; please call configure() first");

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
}