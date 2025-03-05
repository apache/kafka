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
import org.apache.kafka.common.security.oauthbearer.internals.secured.ClientCredentialsRequestFormatter;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE;

/**
 * {@code ClientCredentialsAccessTokenRetriever} is an {@link AccessTokenRetriever} that will
 * communicate with an OAuth/OIDC provider directly via HTTP to post client credentials using
 * the JAAS {@code clientId} and {@code clientSecret} values to a publicized token endpoint URL
 * ({@link SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL}).
 *
 * @see AccessTokenRetriever
 * @see SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL
 */
public class ClientCredentialsAccessTokenRetriever extends HttpAccessTokenRetriever {

    private static final String CLIENT_ID_CONFIG = "clientId";
    private static final String CLIENT_SECRET_CONFIG = "clientSecret";
    private static final String SCOPE_CONFIG = "scope";

    private HttpRequestFormatter requestFormatter;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        super.configure(configs, saslMechanism, jaasConfigEntries);

        JaasOptionsUtils jou = new JaasOptionsUtils(saslMechanism, jaasConfigEntries);
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        String clientId = jou.validateString(CLIENT_ID_CONFIG);
        String clientSecret = jou.validateString(CLIENT_SECRET_CONFIG);
        String scope = jou.validateString(SCOPE_CONFIG, false);
        boolean urlencodeHeader = validateUrlencodeHeader(cu);
        requestFormatter = new ClientCredentialsRequestFormatter(clientId, clientSecret, scope, urlencodeHeader);
    }

    @Override
    protected HttpRequestFormatter requestFormatter() {
        return requestFormatter;
    }

    /**
     * In some cases, the incoming {@link Map} doesn't contain a value for
     * {@link SaslConfigs#SASL_OAUTHBEARER_HEADER_URLENCODE}. Returning {@code null} from {@link Map#get(Object)}
     * will cause a {@link NullPointerException} when it is later unboxed.
     *
     * <p/>
     *
     * This utility method ensures that we have a non-{@code null} value to use in the
     * {@link ClientCredentialsAccessTokenRetriever} constructor.
     */
    public static boolean validateUrlencodeHeader(ConfigurationUtils configurationUtils) {
        Boolean urlencodeHeader = configurationUtils.validateBoolean(SASL_OAUTHBEARER_HEADER_URLENCODE, false);
        return Objects.requireNonNullElse(urlencodeHeader, DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE);
    }

}
