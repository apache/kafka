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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpRequestFormatter;
import org.apache.kafka.common.security.oauthbearer.internals.secured.PrivateKeyRequestFormatter;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionCreator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionJwtTemplate;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.DefaultAssertionCreator;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_ALGORITHM;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE;

/**
 * {@code PrivateKeyJwtRetriever} is a {@link JwtRetriever} that performs the
 * steps to request
 * a JWT from an OAuth/OIDC identity provider using private key JWT
 * authentication. This implementation
 * creates and signs JWT assertions using a private key for client
 * authentication, following
 * the RFC 7523 specification for JWT Profile for OAuth 2.0 Client
 * Authentication.
 *
 * <p/>
 * 
 * This class specifically implements the "private_key_jwt" client
 * authentication
 * method using the {@code client_credentials} grant type with
 * {@code client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer}.
 * It is closely integrated with {@link PrivateKeyRequestFormatter} to format
 * the
 * OAuth request with proper client assertion parameters.
 * 
 * <p/>
 * 
 * Note: This differs from {@link JwtBearerJwtRetriever} which uses the
 * {@code jwt-bearer} grant type and mangages
 * authorization not authentication.
 *
 * <p/>
 *
 * This {@code JwtRetriever} is enabled by specifying its class name in the
 * Kafka configuration.
 * For client use, specify the class name in the
 * <code>sasl.oauthbearer.jwt.retriever.class</code>
 * configuration like so:
 *
 * <pre>
 * sasl.oauthbearer.jwt.retriever.class = org.apache.kafka.common.security.oauthbearer.PrivateKeyJwtRetriever
 * </pre>
 *
 * <p/>
 *
 * If using this {@code JwtRetriever} on the broker side (for inter-broker
 * communication), the configuration
 * should be specified with a listener-based property:
 *
 * <pre>
 * listener.name.&lt;listener name&gt;.oauthbearer.sasl.oauthbearer.jwt.retriever.class=org.apache.kafka.common.security.oauthbearer.PrivateKeyJwtRetriever
 * </pre>
 *
 * <p/>
 *
 * The {@code PrivateKeyJwtRetriever} uses the following configuration options:
 *
 * <ul>
 * <li><code>sasl.oauthbearer.client.credentials.client.id</code></li>
 * <li><code>sasl.oauthbearer.assertion.algorithm</code></li>
 * <li><code>sasl.oauthbearer.assertion.private.key.file</code></li>
 * <li><code>sasl.oauthbearer.assertion.private.key.passphrase</code>
 * (optional)</li>
 * <li><code>sasl.oauthbearer.scope</code> (optional)</li>
 * <li><code>sasl.oauthbearer.token.endpoint.url</code></li>
 * </ul>
 *
 * Please refer to the official Apache Kafka documentation for more information
 * on these, and related configuration.
 *
 * <p/>
 *
 * Note that this implementation does not support the
 * <code>sasl.oauthbearer.assertion.file</code> configuration
 * as it dynamically generates JWT assertions using the provided private key.
 *
 * <p/>
 *
 * Here's an example of the configuration for a Kafka client using private key
 * JWT authentication:
 *
 * <pre>
 * sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;
 *
 * sasl.oauthbearer.client.credentials.client.id=my-service
 * sasl.oauthbearer.assertion.algorithm=RS256
 * sasl.oauthbearer.assertion.private.key.file=/path/to/private-key.pem
 * sasl.oauthbearer.jwt.retriever.class=org.apache.kafka.common.security.oauthbearer.PrivateKeyJwtRetriever
 * sasl.oauthbearer.scope=kafka-access
 * sasl.oauthbearer.token.endpoint.url=https://example.com/oauth2/token
 * </pre>
 */
public class PrivateKeyJwtRetriever implements JwtRetriever {
    private final Time time;
    private HttpJwtRetriever delegate;
    private AssertionJwtTemplate assertionJwtTemplate;
    private AssertionCreator assertionCreator;

    public PrivateKeyJwtRetriever() {
        this(Time.SYSTEM);
    }

    public PrivateKeyJwtRetriever(Time time) {
        this.time = time;
    }

    @Override
    public String retrieve() throws JwtRetrieverException {

        if (delegate == null)
            throw new IllegalStateException(
                    "JWT retriever delegate is null; please call configure() first");

        return delegate.retrieve();
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(assertionCreator, "JWT assertion creator");
        Utils.closeQuietly(assertionJwtTemplate, "JWT assertion template");
        Utils.closeQuietly(delegate, "JWT retriever delegate");
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);

        String scope = cu.validateString(SASL_OAUTHBEARER_SCOPE, false);

        if (cu.validateString(SASL_OAUTHBEARER_ASSERTION_FILE, false) != null) {
            throw new ConfigException(String.format("The OAuth configuration option %s value cannot be used here.",
                    SASL_OAUTHBEARER_ASSERTION_FILE));
        }

        String algorithm = cu.validateString(SASL_OAUTHBEARER_ASSERTION_ALGORITHM);
        File privateKeyFile = cu.validateFile(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE);
        Optional<String> passphrase = cu.containsKey(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE)
                ? Optional.of(cu.validatePassword(
                        SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE))
                : Optional.empty();

        assertionCreator = new DefaultAssertionCreator(algorithm, privateKeyFile, passphrase);
        assertionJwtTemplate = AssertionUtils.layeredAssertionJwtTemplate(cu, time);

        Supplier<String> assertionSupplier = () -> {
            try {
                return assertionCreator.create(assertionJwtTemplate);
            } catch (Exception e) {
                throw new JwtRetrieverException(e);
            }
        };

        Optional<String> clientId = cu.containsKey(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID)
                ? Optional.of(cu.validateString(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID))
                : Optional.empty();
        HttpRequestFormatter requestFormatter = new PrivateKeyRequestFormatter(scope, assertionSupplier,
                clientId);
        delegate = new HttpJwtRetriever(requestFormatter);
        delegate.configure(configs, saslMechanism, jaasConfigEntries);
    }

}
