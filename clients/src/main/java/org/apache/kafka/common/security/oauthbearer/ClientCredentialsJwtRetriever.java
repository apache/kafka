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

import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerConfig;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerJaasConfig;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.SslResource;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.SCOPE_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.requireConfigured;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.urlencodeHeader;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.validateUrl;

/**
 * <code>ClientCredentialsJwtRetriever</code> is a {@link JwtRetriever} that requests JWTs from
 * an OAuth/OIDC provider using the OAuth 2.0 <code>client_credentials</code> grant type. This grant
 * type is commonly used for non-interactive "service accounts" where there is no user available to
 * interactively supply credentials.
 *
 * <p/>
 *
 * The Kafka <code>sasl.jaas.config</code> (JAAS configuration) options for this {@code JwtRetriever}
 * includes the following options:
 *
 * <ul>
 *     <li><code>clientId</code>OAuth client ID (required)</li>
 *     <li><code>clientSecret</code>OAuth client secret (required)</li>
 *     <li><code>scope</code>OAuth scope (optional)</li>
 * </ul>
 *
 * <p/>
 *
 * Here's an example of the JAAS configuration for this {@code JwtRetriever}:
 *
 * <code>
 * sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
 *   clientId="foo" \
 *   clientSecret="bar" \
 *   scope="baz" ;
 * </code>
 */
public class ClientCredentialsJwtRetriever implements JwtRetriever {

    private HttpJwtRetriever delegate;
    private Optional<SslResource> sslResource;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        OAuthBearerConfig config = new OAuthBearerConfig(configs, saslMechanism);
        OAuthBearerJaasConfig jaasConfig = new OAuthBearerJaasConfig(saslMechanism, jaasConfigEntries);
        URL tokenEndpointUrl = validateUrl(config, SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);
        String clientId = jaasConfig.getString(CLIENT_ID_CONFIG);
        String clientSecret = jaasConfig.getString(CLIENT_SECRET_CONFIG);
        Optional<String> scope = jaasConfig.maybeGetString(SCOPE_CONFIG);
        sslResource = OAuthBearerUtils.maybeCreateSslResource(tokenEndpointUrl, jaasConfig);
        Optional<SSLSocketFactory> sslSocketFactory = sslResource.map(r -> r.sslContext().getSocketFactory());
        boolean urlencodeHeader = urlencodeHeader(config);

        delegate = new HttpJwtRetriever(
            clientId,
            clientSecret,
            scope,
            sslSocketFactory,
            tokenEndpointUrl.toString(),
            config.getLong(SASL_LOGIN_RETRY_BACKOFF_MS),
            config.getLong(SASL_LOGIN_RETRY_BACKOFF_MAX_MS),
            config.maybeGetInt(SASL_LOGIN_CONNECT_TIMEOUT_MS),
            config.maybeGetInt(SASL_LOGIN_READ_TIMEOUT_MS),
            urlencodeHeader
        );
    }

    @Override
    public String retrieve() throws JwtRetrieverException {
        try {
            return requireConfigured(delegate, () -> "JWT HTTP client", getClass()).retrieve();
        } catch (IOException e) {
            throw new JwtRetrieverException(e);
        }
    }

    @Override
    public void close() throws IOException {
        sslResource.ifPresent(r -> Utils.closeQuietly(r, "SSL resource"));
    }
}