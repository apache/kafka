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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ClientCredentialsRequestGenerator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpRequestGenerator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtHttpClient;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtHttpResponseBodyHandler;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerConfig;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerJaasConfig;
import org.apache.kafka.common.security.oauthbearer.internals.secured.SslResource;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
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
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.jaasOptions;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.maybeCreateSslResource;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.urlencodeHeader;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.validateUrl;

/**
 * A {@link JwtRetriever} that will communicate with an OAuth/OIDC provider directly via HTTP to post client
 * credentials using the client ID and client secret values to a publicized token endpoint URL
 * ({@link SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL}).
 */
public class ClientCredentialsJwtRetriever implements JwtRetriever {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCredentialsJwtRetriever.class);
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
        OAuthBearerJaasConfig jaasConfig = new OAuthBearerJaasConfig(jaasOptions(saslMechanism, jaasConfigEntries));

        retryBackoffMs = oauthConfig.getLong(SASL_LOGIN_RETRY_BACKOFF_MS);
        retryBackoffMaxMs = oauthConfig.getLong(SASL_LOGIN_RETRY_BACKOFF_MAX_MS);

        URL tokenEndpoint = validateUrl(oauthConfig, SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);
        LOG.debug("Configuring OAuth token endpoint URL: {}", tokenEndpoint);

        sslResource = maybeCreateSslResource(tokenEndpoint, jaasConfig);

        HttpClient.Builder clientBuilder = HttpClient.newBuilder();
        oauthConfig.maybeGetInt(SASL_LOGIN_CONNECT_TIMEOUT_MS).ifPresent(ms -> clientBuilder.connectTimeout(Duration.ofMillis(ms)));
        sslResource.ifPresent(r -> clientBuilder.sslContext(r.sslContext()));
        client = clientBuilder.build();

        String clientId = getConfigOrJaasString(
            oauthConfig,
            jaasConfig,
            SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID,
            CLIENT_ID_JAAS,
            true
        );
        String clientSecret = getConfigOrJaasPassword(
            oauthConfig,
            jaasConfig,
            SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET,
            CLIENT_SECRET_JAAS,
            true
        );
        String scope = getConfigOrJaasString(
            oauthConfig,
            jaasConfig,
            SASL_OAUTHBEARER_SCOPE,
            SCOPE_JAAS,
            false
        );
        boolean urlencodeHeader = urlencodeHeader(oauthConfig);

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

    static String getConfigOrJaasString(OAuthBearerConfig oauthConfig,
                                        OAuthBearerJaasConfig jaasConfig,
                                        String configName,
                                        String jaasName,
                                        boolean isRequired) {
        if (oauthConfig.containsKey(configName))
            return oauthConfig.getString(configName);
        else if (jaasConfig.containsKey(jaasName))
            return jaasConfig.getString(jaasName);
        else if (isRequired)
            throw new ConfigException("Could not find OAuth configuration for " + configName + " or OAuth JAAS configuration for " + jaasName);
        else
            return null;
    }

    static String getConfigOrJaasPassword(OAuthBearerConfig oauthConfig,
                                          OAuthBearerJaasConfig jaasConfig,
                                          String configName,
                                          String jaasName,
                                          boolean isRequired) {
        if (oauthConfig.containsKey(configName))
            return oauthConfig.getPassword(configName);
        else if (jaasConfig.containsKey(jaasName))
            return jaasConfig.getPassword(jaasName);
        else if (isRequired)
            throw new ConfigException("Could not find OAuth configuration for " + configName + " or OAuth JAAS configuration for " + jaasName);
        else
            return null;
    }
}
