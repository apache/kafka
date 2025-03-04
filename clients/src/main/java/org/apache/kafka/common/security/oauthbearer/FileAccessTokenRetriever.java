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
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;

/**
 * <code>FileAccessTokenRetriever</code> is an {@link AccessTokenRetriever} that will load the contents of a file,
 * interpreting them as a JWT access key in the serialized form.
 *
 * @see AccessTokenRetriever
 */
public class FileAccessTokenRetriever implements AccessTokenRetriever {

    private String accessToken;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        File accessTokenFileName = cu.validateFile(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

        try {
            String fileContents = Utils.readFileAsString(accessTokenFileName.getPath());
            // always non-null; to remove any newline chars or backend will report err
            accessToken = fileContents.trim();
        } catch (Exception e) {
            throw new KafkaException("An error occurred reading the OAuth token from " + accessTokenFileName);
        }
    }

    @Override
    public String retrieve() throws IOException {
        return Objects.requireNonNull(accessToken, "Access token is null; please call configure() first");
    }
}
