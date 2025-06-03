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
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpRequestFormatter;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtBearerRequestFormatter;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionCreator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionJwtTemplate;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.DefaultAssertionCreator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.FileAssertionCreator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.StaticAssertionJwtTemplate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_ALGORITHM;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionUtils.layeredAssertionJwtTemplate;

/**
 * {@code JwtBearerJwtRetriever} is a {@link JwtRetriever} that performs the steps to request
 * a JWT from an OAuth/OIDC identity provider using the <code>urn:ietf:params:oauth:grant-type:jwt-bearer</code>
 * grant type. This grant type is used for machine-to-machine "service accounts".
 *
 * <p/>
 *
 * This {@code JwtRetriever} is enabled by specifying its class name in the Kafka configuration.
 * For client use, specify the class name in the <code>sasl.oauthbearer.jwt.retriever.class</code>
 * configuration like so:
 *
 * <pre>
 * sasl.oauthbearer.jwt.retriever.class=org.apache.kafka.common.security.oauthbearer.JwtBearerJwtRetriever
 * </pre>
 *
 * <p/>
 *
 * If using this {@code JwtRetriever} on the broker side (for inter-broker communication), the configuration
 * should be specified with a listener-based property:
 *
 * <pre>
 * listener.name.&lt;listener name&gt;.oauthbearer.sasl.oauthbearer.jwt.retriever.class=org.apache.kafka.common.security.oauthbearer.JwtBearerJwtRetriever
 * </pre>
 *
 * <p/>
 *
 * The {@code JwtBearerJwtRetriever} also uses the following configuration:
 *
 * <ul>
 *     <li><code>sasl.oauthbearer.assertion.algorithm</code></li>
 *     <li><code>sasl.oauthbearer.assertion.claim.aud</code></li>
 *     <li><code>sasl.oauthbearer.assertion.claim.exp.seconds</code></li>
 *     <li><code>sasl.oauthbearer.assertion.claim.iss</code></li>
 *     <li><code>sasl.oauthbearer.assertion.claim.jti.include</code></li>
 *     <li><code>sasl.oauthbearer.assertion.claim.nbf.seconds</code></li>
 *     <li><code>sasl.oauthbearer.assertion.claim.sub</code></li>
 *     <li><code>sasl.oauthbearer.assertion.file</code></li>
 *     <li><code>sasl.oauthbearer.assertion.private.key.file</code></li>
 *     <li><code>sasl.oauthbearer.assertion.private.key.passphrase</code></li>
 *     <li><code>sasl.oauthbearer.assertion.template.file</code></li>
 *     <li><code>sasl.oauthbearer.jwt.retriever.class</code></li>
 *     <li><code>sasl.oauthbearer.scope</code></li>
 *     <li><code>sasl.oauthbearer.token.endpoint.url</code></li>
 * </ul>
 *
 * Please refer to the official Apache Kafka documentation for more information on these, and related, configuration.
 *
 * <p/>
 *
 * Here's an example of the JAAS configuration for a Kafka client:
 *
 * <pre>
 * sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;
 *
 * sasl.oauthbearer.assertion.algorithm=RS256
 * sasl.oauthbearer.assertion.claim.aud=my-application-audience
 * sasl.oauthbearer.assertion.claim.exp.seconds=600
 * sasl.oauthbearer.assertion.claim.iss=my-oauth-issuer
 * sasl.oauthbearer.assertion.claim.jti.include=true
 * sasl.oauthbearer.assertion.claim.nbf.seconds=120
 * sasl.oauthbearer.assertion.claim.sub=kafka-app-1234
 * sasl.oauthbearer.assertion.private.key.file=/path/to/private.key
 * sasl.oauthbearer.assertion.private.key.passphrase=$3cr3+
 * sasl.oauthbearer.assertion.template.file=/path/to/assertion-template.json
 * sasl.oauthbearer.jwt.retriever.class=org.apache.kafka.common.security.oauthbearer.JwtBearerJwtRetriever
 * sasl.oauthbearer.scope=my-application-scope
 * sasl.oauthbearer.token.endpoint.url=https://example.com/oauth2/v1/token
 * </pre>
 */
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

        String scope = cu.validateString(SASL_OAUTHBEARER_SCOPE, false);

        if (cu.validateString(SASL_OAUTHBEARER_ASSERTION_FILE, false) != null) {
            File assertionFile = cu.validateFile(SASL_OAUTHBEARER_ASSERTION_FILE);
            assertionCreator = new FileAssertionCreator(assertionFile);
            assertionJwtTemplate = new StaticAssertionJwtTemplate();
        } else {
            String algorithm = cu.validateString(SASL_OAUTHBEARER_ASSERTION_ALGORITHM);
            File privateKeyFile = cu.validateFile(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE);
            Optional<String> passphrase = cu.containsKey(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE) ?
                Optional.of(cu.validatePassword(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE)) :
                Optional.empty();

            assertionCreator = new DefaultAssertionCreator(algorithm, privateKeyFile, passphrase);
            assertionJwtTemplate = layeredAssertionJwtTemplate(cu, time);
        }

        Supplier<String> assertionSupplier = () -> {
            try {
                return assertionCreator.create(assertionJwtTemplate);
            } catch (Exception e) {
                throw new JwtRetrieverException(e);
            }
        };

        HttpRequestFormatter requestFormatter = new JwtBearerRequestFormatter(scope, assertionSupplier);

        delegate = new HttpJwtRetriever(requestFormatter);
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
        Utils.closeQuietly(assertionCreator, "JWT assertion creator");
        Utils.closeQuietly(assertionJwtTemplate, "JWT assertion template");
        Utils.closeQuietly(delegate, "JWT retriever delegate");
    }
}
