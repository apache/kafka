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

import org.apache.kafka.common.security.oauthbearer.internals.secured.ClientCredentialsRequestFormatterFactory;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpRequestFormatter;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

/**
 * {@code ClientCredentialsJwtRetriever} is a {@link JwtRetriever} that performs the steps to request
 * a JWT from an OAuth/OIDC identity provider using the <code>client_credentials</code> grant type. This
 * grant type is commonly used for non-interactive "service accounts" where there is no user available
 * to interactively supply credentials.
 *
 * <p/>
 *
 * This {@code JwtRetriever} is enabled by specifying its class name in the Kafka configuration.
 * For client use, specify the class name in the <code>sasl.oauthbearer.jwt.retriever.class</code>
 * configuration like so:
 *
 * <pre>
 * sasl.oauthbearer.jwt.retriever.class=org.apache.kafka.common.security.oauthbearer.ClientCredentialsJwtRetriever
 * </pre>
 *
 * <p/>
 *
 * If using this {@code JwtRetriever} on the broker side (for inter-broker communication), the configuration
 * should be specified with a listener-based property:
 *
 * <pre>
 * listener.name.&lt;listener name&gt;.oauthbearer.sasl.oauthbearer.jwt.retriever.class=org.apache.kafka.common.security.oauthbearer.ClientCredentialsJwtRetriever
 * </pre>
 *
 * <p/>
 *
 * The {@code ClientCredentialsJwtRetriever} also uses the following configuration:
 *
 * <ul>
 *     <li><code>sasl.oauthbearer.client.credentials.client.id</code></li>
 *     <li><code>sasl.oauthbearer.client.credentials.client.secret</code></li>
 *     <li><code>sasl.oauthbearer.scope</code></li>
 *     <li><code>sasl.oauthbearer.token.endpoint.url</code></li>
 * </ul>
 *
 * Please refer to the official Apache Kafka documentation for more information on these, and related configuration.
 *
 * <p/>
 *
 * Previous versions of this implementation used <code>sasl.jaas.config</code> to specify attributes such
 * as <code>clientId</code>, <code>clientSecret</code>, and <code>scope</code>. These will still work, but
 * if the configuration for each of these is specified, it will be used instead of the JAAS option.
 *
 * <p/>
 *
 * Here's an example of the JAAS configuration for a Kafka client:
 *
 * <pre>
 * sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;
 *
 * sasl.oauthbearer.client.credentials.client.id=jdoe
 * sasl.oauthbearer.client.credentials.client.secret=$3cr3+
 * sasl.oauthbearer.jwt.retriever.class=org.apache.kafka.common.security.oauthbearer.ClientCredentialsJwtRetriever
 * sasl.oauthbearer.scope=my-application-scope
 * sasl.oauthbearer.token.endpoint.url=https://example.com/oauth2/v1/token
 * </pre>
 */
public class ClientCredentialsJwtRetriever implements JwtRetriever {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCredentialsJwtRetriever.class);

    private final Time time;
    private HttpJwtRetriever delegate;

    public ClientCredentialsJwtRetriever() {
        this(Time.SYSTEM);
    }

    public ClientCredentialsJwtRetriever(Time time) {
        this.time = time;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        JaasOptionsUtils jou = new JaasOptionsUtils(saslMechanism, jaasConfigEntries);

        HttpRequestFormatter requestFormatter = ClientCredentialsRequestFormatterFactory.create(cu, jou, time);
        delegate = new HttpJwtRetriever(requestFormatter);

        LOG.debug("Created instance of {} as delegate", delegate.getClass().getName());
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
        Utils.closeQuietly(delegate, "JWT retriever delegate");
    }
}
