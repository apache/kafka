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

import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtBearerRequestGenerator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_GRANT_TYPE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_GRANT_TYPE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.protocolMatches;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.validateUrl;

public class DefaultJwtRetriever implements JwtRetriever {

    private final Time time;

    private JwtRetriever delegate;

    public DefaultJwtRetriever() {
        this(Time.SYSTEM);
    }

    public DefaultJwtRetriever(Time time) {
        this.time = time;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        OAuthBearerConfig oauthConfig = new OAuthBearerConfig(configs, saslMechanism);
        URL tokenEndpoint = validateUrl(oauthConfig, SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

        if (protocolMatches(tokenEndpoint, "file")) {
            delegate = new FileJwtRetriever();
        } else {
            String grantType = oauthConfig.maybeGetString(SASL_OAUTHBEARER_GRANT_TYPE).orElse(DEFAULT_SASL_OAUTHBEARER_GRANT_TYPE);

            if (grantType.equalsIgnoreCase(JwtBearerRequestGenerator.GRANT_TYPE)) {
                delegate = new JwtBearerJwtRetriever(time);
            } else {
                delegate = new ClientCredentialsJwtRetriever(time);
            }
        }

        delegate.configure(configs, saslMechanism, jaasConfigEntries);
    }

    @Override
    public String retrieve() throws JwtRetrieverException {
        return Objects.requireNonNull(delegate).retrieve();
    }

    @Override
    public void close() {
        Utils.closeQuietly(delegate, "delegate");
    }

    public JwtRetriever delegate() {
        return delegate;
    }
}
