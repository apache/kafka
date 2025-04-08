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
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.RefreshingCachedFile;

import java.io.File;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;

/**
 * A {@link JwtRetriever} that will load the contents of a file, interpreting them as a JWT in serialized form.
 */
public class FileJwtRetriever implements JwtRetriever {

    private RefreshingCachedFile<String> jwtFile;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        File fileName = cu.validateFile(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

        try {
            // always non-null; to remove any newline chars or backend will report err
            jwtFile = new RefreshingCachedFile<>(
                fileName,
                (file, contents) -> contents.trim()
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
