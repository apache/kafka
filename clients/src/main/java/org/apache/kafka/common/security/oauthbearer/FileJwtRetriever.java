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

import org.apache.kafka.common.KafkaException;

import java.io.File;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.CachedFile.staticCacheRefreshPolicy;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.validateFileUrl;

/**
 * A {@link JwtRetriever} that will load the contents of a file, interpreting them as a JWT in serialized form.
 */
public class FileJwtRetriever implements JwtRetriever {

    private CachedFile<String> jwtFile;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        OAuthBearerConfig oauthConfig = new OAuthBearerConfig(configs, saslMechanism);
        File fileName = validateFileUrl(oauthConfig, SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

        try {
            // always non-null; to remove any newline chars or backend will report err
            jwtFile = new CachedFile<>(
                fileName,
                (file, contents) -> contents.trim(),
                staticCacheRefreshPolicy()
            );
        } catch (Exception e) {
            throw new KafkaException("An error occurred reading the OAuth JWT from " + fileName);
        }
    }

    @Override
    public String retrieve() throws JwtRetrieverException {
        if (jwtFile == null)
            throw new JwtRetrieverException("JWT is null; please call configure() first");

        return jwtFile.transformed();
    }
}
