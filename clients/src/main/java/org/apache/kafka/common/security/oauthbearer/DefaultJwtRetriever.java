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
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.URL;
import java.util.Locale;
import java.util.Map;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;

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
            delegate = new ClientCredentialsJwtRetriever(configs, saslMechanism, jaasConfig);
        }

        delegate.init();
    }

    @Override
    public String retrieve() throws JwtRetrieverException {
        if (delegate == null)
            throw new IllegalStateException("JWT retriever delegate is null; please call init() first");

        return delegate.retrieve();
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(delegate, "JWT retriever delegate");
    }

    JwtRetriever delegate() {
        return delegate;
    }
}