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
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerConfig;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.protocolMatches;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.requireConfigured;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.validateUrl;

/**
 * {@code DefaultJwtRetriever} instantiates and delegates {@link JwtRetriever} API calls to an embedded implementation
 * based on configuration. If {@link SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL} is configured with a
 * {@code file}-based URL, a {@link FileJwtRetriever} is created and the JWT is expected be contained in the file
 * specified. Otherwise, it's assumed to be an HTTP/HTTPS-based URL, so an {@link HttpJwtRetriever} is created.
 */
public class DefaultJwtRetriever implements JwtRetriever {

    private JwtRetriever delegate;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        OAuthBearerConfig config = new OAuthBearerConfig(configs, saslMechanism);
        URL tokenEndpointUrl = validateUrl(config, SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

        if (protocolMatches(tokenEndpointUrl, "file")) {
            delegate = new FileJwtRetriever();
        } else {
            delegate = new ClientCredentialsJwtRetriever();
        }

        delegate.configure(configs, saslMechanism, jaasConfigEntries);
    }

    @Override
    public String retrieve() throws JwtRetrieverException {
        return requireConfigured(delegate, () -> "JWT retriever delegate", getClass()).retrieve();
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(delegate, "JWT retriever delegate");
    }

    JwtRetriever delegate() {
        return delegate;
    }
}