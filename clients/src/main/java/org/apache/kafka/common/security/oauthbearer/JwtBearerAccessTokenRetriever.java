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

import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpRequestFormatter;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtBearerRequestFormatter;
import org.apache.kafka.common.utils.Time;

import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

public class JwtBearerAccessTokenRetriever extends HttpAccessTokenRetriever {

    // The private key ID of the private key used to sign the JWT token sent to the token endpoint. This will
    // be added as a header in the JWT token sent to the token endpoint.
    private static final String TOKEN_ENDPOINT_PRIVATE_KEY_ID = "privateKeyId";

    // The private key used to sign the JWT token sent to the token endpoint. This must be in PEM format without
    // the header and footer.
    private static final String TOKEN_ENDPOINT_PRIVATE_KEY_SECRET = "privateKeySecret";

    // The algorithm used to sign the JWT token sent to the token endpoint.
    private static final String TOKEN_ENDPOINT_SIGNING_ALGO = "tokenSigningAlgo";

    // The subject of the JWT token sent to the token endpoint.
    private static final String TOKEN_SUBJECT = "tokenSubject";

    // The issuer of the JWT token sent to the token endpoint.
    private static final String TOKEN_ISSUER = "tokenIssuer";

    // The audience of the JWT token sent to the token endpoint.
    private static final String TOKEN_AUDIENCE = "tokenAudience";

    // The target audience of the JWT token sent to the token endpoint.
    private static final String TOKEN_TARGET_AUDIENCE = "tokenTargetAudience";

    private final Time time;

    private JwtBearerRequestFormatter requestFormatter;

    public JwtBearerAccessTokenRetriever() {
        this(Time.SYSTEM);
    }

    public JwtBearerAccessTokenRetriever(Time time) {
        this.time = time;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        super.configure(configs, saslMechanism, jaasConfigEntries);

        JaasOptionsUtils jou = new JaasOptionsUtils(saslMechanism, jaasConfigEntries);
        String privateKeyId = jou.validateString(TOKEN_ENDPOINT_PRIVATE_KEY_ID);
        String privateKeySecret = jou.validateString(TOKEN_ENDPOINT_PRIVATE_KEY_SECRET);
        String tokenSigningAlgo = jou.validateString(TOKEN_ENDPOINT_SIGNING_ALGO);
        String tokenSubject = jou.validateString(TOKEN_SUBJECT);
        String tokenIssuer = jou.validateString(TOKEN_ISSUER);
        String tokenAudience = jou.validateString(TOKEN_AUDIENCE);
        String tokenTargetAudience = jou.validateString(TOKEN_TARGET_AUDIENCE, false);

        requestFormatter = new JwtBearerRequestFormatter(
            time,
            privateKeyId,
            privateKeySecret,
            tokenSigningAlgo,
            tokenSubject,
            tokenIssuer,
            tokenAudience,
            tokenTargetAudience
        );
    }

    @Override
    protected HttpRequestFormatter requestFormatter() {
        return requestFormatter;
    }
}
