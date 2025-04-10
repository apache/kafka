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
import org.apache.kafka.common.security.oauthbearer.internals.secured.SslResource;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.configOrJaas;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.validateInteger;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.validateUri;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.validateUrlencodeHeader;

/**
 * A {@link JwtRetriever} that will communicate with an OAuth/OIDC provider directly via HTTP to post client
 * credentials using the client ID and client secret values to a publicized token endpoint URL
 * ({@link SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL}).
 */
public class ClientCredentialsJwtRetriever implements JwtRetriever {

    private static final String CLIENT_ID_JAAS = "clientId";
    private static final String CLIENT_SECRET_JAAS = "clientSecret";
    private static final String SCOPE_JAAS = "scope";

    private final Time time;

    private Optional<SslResource> sslResource = Optional.empty();
    private HttpRequestGenerator requestGenerator;
    private long retryBackoffMs;
    private long retryBackoffMaxMs;
    private HttpClient client;

    public ClientCredentialsJwtRetriever() {
        this(Time.SYSTEM);
    }

    public ClientCredentialsJwtRetriever(Time time) {
        this.time = time;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        OAuthBearerConfig oauthConfig = new OAuthBearerConfig(configs, saslMechanism);
        OAuthBearerJaasConfig jaasConfig = new OAuthBearerJaasConfig(saslMechanism, jaasConfigEntries);

        retryBackoffMs = oauthConfig.getLong(SASL_LOGIN_RETRY_BACKOFF_MS);
        retryBackoffMaxMs = oauthConfig.getLong(SASL_LOGIN_RETRY_BACKOFF_MAX_MS);

        URI tokenEndpoint = validateUri(oauthConfig, SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);
        sslResource = OAuthBearerUtils.maybeCreateSslResource(tokenEndpoint, jaasConfig);
        Optional<Integer> connectTimeoutMs = Optional.ofNullable(validateInteger(oauthConfig, SASL_LOGIN_CONNECT_TIMEOUT_MS, false));

        HttpClient.Builder clientBuilder = HttpClient.newBuilder();

        if (connectTimeoutMs.isPresent())
            clientBuilder = clientBuilder.connectTimeout(Duration.ofMillis(connectTimeoutMs.get()));

        if (sslResource.isPresent())
            clientBuilder = clientBuilder.sslContext(sslResource.get().sslContext());

        client = clientBuilder.build();

        String clientId = configOrJaas(
            oauthConfig,
            jaasConfig,
            SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID,
            CLIENT_ID_JAAS,
            true
        );
        String clientSecret = configOrJaas(
            oauthConfig,
            jaasConfig,
            SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET,
            CLIENT_SECRET_JAAS,
            true
        );
        String scope = configOrJaas(
            oauthConfig,
            jaasConfig,
            SASL_OAUTHBEARER_SCOPE,
            SCOPE_JAAS,
            false
        );
        boolean urlencodeHeader = validateUrlencodeHeader(oauthConfig);

        requestGenerator = new ClientCredentialsRequestGenerator(
            tokenEndpoint,
            clientId,
            clientSecret,
            scope,
            urlencodeHeader
        );
    }

    @Override
    public String retrieve() throws JwtRetrieverException {
        HttpRequest request = requestGenerator.generateRequest();
        JwtHttpClient jwtHttpClient = new JwtHttpClient(time);
        HttpResponse.BodyHandler<String> responseBodyHandler = new JwtHttpResponseBodyHandler();

        return jwtHttpClient.request(
            client,
            request,
            responseBodyHandler,
            retryBackoffMs,
            retryBackoffMaxMs
        );
    }

    @Override
    public void close() {
        Utils.closeQuietly(requestGenerator, "requestGenerator");
        sslResource.ifPresent(r -> Utils.closeQuietly(r, "sslResource"));
    }
}
