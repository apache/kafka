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

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;

/**
 * Implementation of {@link AccessTokenValidator} that is used
 * by the broker to perform more extensive validation of the JWT access token that is received
 * from the client, but ultimately from posting the client credentials to the OAuth/OIDC provider's
 * token endpoint.
 *
 * The validation steps performed (primary by the jose4j library) are:
 *
 * <ol>
 *     <li>
 *         Basic structural validation of the <code>b64token</code> value as defined in
 *         <a href="https://tools.ietf.org/html/rfc6750#section-2.1">RFC 6750 Section 2.1</a>
 *     </li>
 *     <li>Basic conversion of the token into an in-memory data structure</li>
 *     <li>
 *         Presence of scope, <code>exp</code>, subject, <code>iss</code>, and
 *         <code>iat</code> claims
 *     </li>
 *     <li>
 *         Signature matching validation against the <code>kid</code> and those provided by
 *         the OAuth/OIDC provider's JWKS
 *     </li>
 * </ol>
 */

public class DefaultAccessTokenValidator implements AccessTokenValidator {

    protected AccessTokenValidator delegate;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        AccessTokenValidator validator;

        if (configs.get(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL) != null)
            validator = new ValidatorAccessTokenValidator();
        else
            validator = new LoginAccessTokenValidator();

        configure(validator, configs, saslMechanism, jaasConfigEntries);
    }

    void configure(AccessTokenValidator validator,
                   Map<String, ?> configs,
                   String saslMechanism,
                   List<AppConfigurationEntry> jaasConfigEntries) {
        delegate = validator;
        delegate.configure(configs, saslMechanism, jaasConfigEntries);
    }

    @Override
    public OAuthBearerToken validate(String accessToken) throws ValidateException {
        return Objects.requireNonNull(delegate).validate(accessToken);
    }

    @Override
    public void close() {
        Utils.closeQuietly(delegate, "delegate");
    }

    AccessTokenValidator delegate() {
        return delegate;
    }
}
