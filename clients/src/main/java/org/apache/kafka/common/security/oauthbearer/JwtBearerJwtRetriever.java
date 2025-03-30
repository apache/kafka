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

import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpRequestGenerator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtBearerRequestGenerator;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
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
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_ALGORITHM;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;

public class JwtBearerJwtRetriever implements JwtRetriever {

    private final Time time;

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
//        JaasOptionsUtils jou = new JaasOptionsUtils(saslMechanism, jaasConfigEntries);
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);

        retryBackoffMs =  cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MS);
        retryBackoffMaxMs = cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MAX_MS);

//        Optional<SslResource> sslResource = jou.maybeCreateSslResource(url);
        Optional<Integer> connectTimeoutMs = Optional.ofNullable(cu.validateInteger(SASL_LOGIN_CONNECT_TIMEOUT_MS, false));

        HttpClient.Builder clientBuilder = HttpClient.newBuilder();

        if (connectTimeoutMs.isPresent())
            clientBuilder = clientBuilder.connectTimeout(Duration.ofMillis(connectTimeoutMs.get()));

//        if (sslResource.isPresent())
//            clientBuilder = clientBuilder.sslContext(sslResource.get());

        client = clientBuilder.build();

        String algorithm = cu.validateString(SASL_OAUTHBEARER_ASSERTION_ALGORITHM);
        File privateKeyFile = cu.validateFile(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE);
        AssertionCreator assertionCreator = new DefaultAssertionCreator(time, algorithm, privateKeyFile);

        AssertionJwtTemplate assertionJwtTemplate = new AssertionJwtTemplateFile();

        URI tokenEndpoint = cu.validateUri(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

        requestGenerator = new JwtBearerRequestGenerator(
            tokenEndpoint,
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
    }
}
