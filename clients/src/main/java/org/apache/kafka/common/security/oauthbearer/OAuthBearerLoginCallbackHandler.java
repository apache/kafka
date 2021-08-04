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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuthBearerLoginCallbackHandler implements AuthenticateCallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(OAuthBearerLoginCallbackHandler.class);

    private String clientId;

    private String clientSecret;

    private String issuerUri;

    private String scope;

    @Override
    public void configure(final Map<String, ?> configs, final String saslMechanism,
        final List<AppConfigurationEntry> jaasConfigEntries) {
        for (AppConfigurationEntry ace : jaasConfigEntries) {
            this.clientId = (String) ace.getOptions().get("clientId");
            this.clientSecret = (String) ace.getOptions().get("clientSecret");
            this.issuerUri = (String) ace.getOptions().get("issuerUri");
            this.scope = (String) ace.getOptions().get("scope");
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void handle(final Callback[] callbacks)
        throws IOException, UnsupportedCallbackException {
        if (callbacks != null && callbacks.length > 0) {
            for (Callback callback : callbacks) {
                if (callback instanceof OAuthBearerTokenCallback)
                    handle((OAuthBearerTokenCallback) callback);
            }
        } else {
            log.warn("Nope - no callbacks provided");
        }
    }

    private void handle(OAuthBearerTokenCallback callback) throws IOException {
        try {
            String tokenEndpointUrl = OAuthBearerUtils.getTokenEndpoint(issuerUri);
            log.warn("handle - tokenEndpointUrl: {}", tokenEndpointUrl);

            String accessToken = OAuthBearerUtils.getAccessToken(tokenEndpointUrl, clientId, clientSecret, scope);
            log.warn("handle - accessToken: {}", accessToken);

            OAuthBearerToken token = OAuthBearerUtils.parseAndValidateToken(accessToken);
            log.warn("handle - token: {}", token);

            callback.token(token);
        } catch (Exception e) {
            callback.error("nyi", e.getMessage(), "https://www.example.com");
        }
    }

}
