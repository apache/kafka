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
package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_GRANT_TYPE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;

public class DefaultAccessTokenRetriever implements AccessTokenRetriever {

    private AccessTokenRetriever delegate;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        URL tokenEndpointUrl = cu.validateUrl(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

        if (tokenEndpointUrl.getProtocol().toLowerCase(Locale.ROOT).equals("file")) {
            delegate = new FileAccessTokenRetriever();
        } else {
            String grantType = cu.validateString(SASL_OAUTHBEARER_TOKEN_ENDPOINT_GRANT_TYPE, false);

            if (grantType != null && grantType.equalsIgnoreCase(JwtBearerAccessTokenRetriever.GRANT_TYPE)) {
                delegate = new JwtBearerAccessTokenRetriever();
            } else {
                delegate = new ClientCredentialsAccessTokenRetriever();
            }
        }

        delegate.configure(configs, saslMechanism, jaasConfigEntries);
    }

    @Override
    public String retrieve() throws IOException {
        return Objects.requireNonNull(delegate).retrieve();
    }

    @Override
    public void close() {
        Utils.closeQuietly(delegate, "delegate");
    }

    public AccessTokenRetriever delegate() {
        return delegate;
    }
}
