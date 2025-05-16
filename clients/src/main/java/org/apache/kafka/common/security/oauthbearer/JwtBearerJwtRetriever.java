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

import org.apache.kafka.common.security.oauthbearer.internals.secured.AssertionCreator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AssertionJwtTemplate;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.DefaultAssertionCreator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.FileAssertionCreator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpRequestFormatter;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtBearerRequestFormatter;
import org.apache.kafka.common.security.oauthbearer.internals.secured.LayeredAssertionJwtTemplate;
import org.apache.kafka.common.security.oauthbearer.internals.secured.StaticAssertionJwtTemplate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_ALGORITHM;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.SCOPE_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.AssertionUtils.dynamicAssertionJwtTemplate;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.AssertionUtils.fileAssertionJwtTemplate;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.AssertionUtils.staticAssertionJwtTemplate;

public class JwtBearerJwtRetriever implements JwtRetriever {

    private final Time time;
    private HttpJwtRetriever delegate;
    private AssertionJwtTemplate assertionJwtTemplate;
    private AssertionCreator assertionCreator;

    public JwtBearerJwtRetriever() {
        this(Time.SYSTEM);
    }

    public JwtBearerJwtRetriever(Time time) {
        this.time = time;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        URL tokenEndpointUrl = cu.validateUrl(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);
        String scope = cu.validateString(SCOPE_CONFIG, false);

        JaasOptionsUtils jou = new JaasOptionsUtils(JaasOptionsUtils.getOptions(saslMechanism, jaasConfigEntries));

        SSLSocketFactory sslSocketFactory = null;

        if (jou.shouldCreateSSLSocketFactory(tokenEndpointUrl))
            sslSocketFactory = jou.createSSLSocketFactory();

        if (cu.validateString(SASL_OAUTHBEARER_ASSERTION_FILE, false) != null) {
            File assertionFile = cu.validateFile(SASL_OAUTHBEARER_ASSERTION_FILE).toFile();
            assertionCreator = new FileAssertionCreator(assertionFile);
            assertionJwtTemplate = new StaticAssertionJwtTemplate();
        } else {
            String algorithm = cu.validateString(SASL_OAUTHBEARER_ASSERTION_ALGORITHM);
            File privateKeyFile = cu.validateFile(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE).toFile();
            assertionCreator = new DefaultAssertionCreator(algorithm, privateKeyFile, Optional.empty());
            List<AssertionJwtTemplate> templates = new ArrayList<>();
            fileAssertionJwtTemplate(cu).ifPresent(templates::add);
            staticAssertionJwtTemplate(cu).ifPresent(templates::add);
            templates.add(dynamicAssertionJwtTemplate(cu, time));
            assertionJwtTemplate = new LayeredAssertionJwtTemplate(templates);
        }

        Supplier<String> assertionSupplier = () -> {
            try {
                return assertionCreator.create(assertionJwtTemplate);
            } catch (Exception e) {
                throw new JwtRetrieverException(e);
            }
        };

        HttpRequestFormatter requestFormatter = new JwtBearerRequestFormatter(scope, assertionSupplier);

        delegate = new HttpJwtRetriever(
            requestFormatter,
            sslSocketFactory,
            tokenEndpointUrl.toString(),
            cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MS),
            cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MAX_MS),
            cu.validateInteger(SASL_LOGIN_CONNECT_TIMEOUT_MS, false),
            cu.validateInteger(SASL_LOGIN_READ_TIMEOUT_MS, false)
        );
    }

    @Override
    public String retrieve() throws JwtRetrieverException {
        if (delegate == null)
            throw new IllegalStateException("JWT retriever delegate is null; please call configure() first");

        return delegate.retrieve();
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(assertionCreator, "JWT assertion creator");
        Utils.closeQuietly(assertionJwtTemplate, "JWT assertion template");
    }
}