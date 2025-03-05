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

import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;

/**
 *
 */

public class DefaultAccessTokenValidator implements AccessTokenValidator {

    private AccessTokenValidator delegate;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        AccessTokenValidator validator;

        if (configs.get(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL) != null)
            validator = new BrokerAccessTokenValidator();
        else
            validator = new ClientAccessTokenValidator();

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
    public OAuthBearerToken validate(String accessToken) throws InvalidJwtException {
        return Objects.requireNonNull(delegate).validate(accessToken);
    }

    @Override
    public void close() {
        Utils.closeQuietly(delegate, "delegate");
    }
}
