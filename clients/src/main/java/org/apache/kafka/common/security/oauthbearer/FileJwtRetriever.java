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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerConfig;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.requireConfigured;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.validateFileUrl;

/**
 * <code>FileJwtRetriever</code> is an {@link JwtRetriever} that will load the contents
 * of a file, interpreting them as a JWT access key in the serialized form.
 *
 * @see JwtRetriever
 */

public class FileJwtRetriever implements JwtRetriever {

    private String jwt;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        OAuthBearerConfig config = new OAuthBearerConfig(configs, saslMechanism);
        String fileName = validateFileUrl(config, SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL).getPath();

        try {
            this.jwt = Utils.readFileAsString(fileName);

            // always non-null; to remove any newline chars or backend will report err
            this.jwt = this.jwt.trim();
        } catch (IOException e) {
            throw new ConfigException(
                String.format(
                    "The OAuth configuration %s contains a file (%s) that could not be loaded: %s",
                    SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL,
                    fileName,
                    e.getMessage()
                )
            );
        }
    }

    @Override
    public String retrieve() throws JwtRetrieverException {
        return requireConfigured(jwt, () -> "JWT", getClass());
    }

}
