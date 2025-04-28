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

import org.apache.kafka.common.security.oauthbearer.internals.secured.DefaultAssertionCreator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.FileAssertionCreator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpRequestGenerator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtBearerRequestGenerator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtHttpClient;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtHttpResponseBodyHandler;
import org.apache.kafka.common.security.oauthbearer.internals.secured.LayeredAssertionJwtTemplate;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerConfig;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerJaasConfig;
import org.apache.kafka.common.security.oauthbearer.internals.secured.SslResource;
import org.apache.kafka.common.security.oauthbearer.internals.secured.StaticAssertionJwtTemplate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_ALGORITHM;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.dynamicAssertionJwtTemplate;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.fileAssertionJwtTemplate;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.jaasOptions;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.maybeCreateSslResource;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.staticAssertionJwtTemplate;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.validateFile;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.validateUrl;

public class JwtBearerJwtRetriever implements JwtRetriever {

    private final Time time;

    private Optional<SslResource> sslResource = Optional.empty();
    private HttpRequestGenerator requestGenerator;
    private long retryBackoffMs;
    private long retryBackoffMaxMs;
    private HttpClient client;

    public JwtBearerJwtRetriever() {
        this(Time.SYSTEM);
    }

    public JwtBearerJwtRetriever(Time time) {
        this.time = time;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        OAuthBearerConfig oauthConfig = new OAuthBearerConfig(configs, saslMechanism);
        OAuthBearerJaasConfig jaasConfig = new OAuthBearerJaasConfig(jaasOptions(saslMechanism, jaasConfigEntries));

        URL tokenEndpoint = validateUrl(oauthConfig, SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);
        Optional<String> scope = oauthConfig.maybeGetString(SASL_OAUTHBEARER_SCOPE);

        retryBackoffMs =  oauthConfig.getLong(SASL_LOGIN_RETRY_BACKOFF_MS);
        retryBackoffMaxMs = oauthConfig.getLong(SASL_LOGIN_RETRY_BACKOFF_MAX_MS);
        sslResource = maybeCreateSslResource(tokenEndpoint, jaasConfig);

        HttpClient.Builder clientBuilder = HttpClient.newBuilder();
        oauthConfig.maybeGetInt(SASL_LOGIN_CONNECT_TIMEOUT_MS).ifPresent(ms -> clientBuilder.connectTimeout(Duration.ofMillis(ms)));
        sslResource.ifPresent(r -> clientBuilder.sslContext(r.sslContext()));
        client = clientBuilder.build();

        AssertionCreator assertionCreator;
        AssertionJwtTemplate assertionJwtTemplate;

        if (oauthConfig.containsKey(SASL_OAUTHBEARER_ASSERTION_FILE)) {
            File assertionFile = validateFile(oauthConfig, SASL_OAUTHBEARER_ASSERTION_FILE);
            assertionCreator = new FileAssertionCreator(assertionFile);
            assertionJwtTemplate = new StaticAssertionJwtTemplate();
        } else {
            String algorithm = oauthConfig.getString(SASL_OAUTHBEARER_ASSERTION_ALGORITHM);
            File privateKeyFile = validateFile(oauthConfig, SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE);
            Optional<String> passphrase = oauthConfig.maybeGetString(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE);
            assertionCreator = new DefaultAssertionCreator(algorithm, privateKeyFile, passphrase);
            List<AssertionJwtTemplate> templates = new ArrayList<>();
            fileAssertionJwtTemplate(oauthConfig).ifPresent(templates::add);
            staticAssertionJwtTemplate(oauthConfig).ifPresent(templates::add);
            templates.add(dynamicAssertionJwtTemplate(oauthConfig, time));
            assertionJwtTemplate = new LayeredAssertionJwtTemplate(templates);
        }

        requestGenerator = new JwtBearerRequestGenerator(
            tokenEndpoint,
            scope,
            assertionCreator,
            assertionJwtTemplate
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
