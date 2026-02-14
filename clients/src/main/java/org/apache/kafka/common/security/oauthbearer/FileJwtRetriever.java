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

import org.apache.kafka.common.security.oauthbearer.internals.secured.CachedFile;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;

import java.io.File;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.CachedFile.RefreshPolicy.lastModifiedPolicy;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.CachedFile.STRING_JSON_VALIDATING_TRANSFORMER;

/**
 * <code>FileJwtRetriever</code> is an {@link JwtRetriever} that will load the contents
 * of a file, interpreting them as a JWT access key in the serialized form.
 */
public class FileJwtRetriever implements JwtRetriever {

    private CachedFile<String> jwtFile;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        File file = cu.validateFileUrl(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);
        jwtFile = new CachedFile<>(file, STRING_JSON_VALIDATING_TRANSFORMER, lastModifiedPolicy());
    }

    @Override
    public String retrieve() throws JwtRetrieverException {
        if (jwtFile == null)
            throw new IllegalStateException("JWT is null; please call configure() first");

        try {
            return jwtFile.transformed();
        } catch (Exception e) {
            throw new JwtRetrieverException(e);
        }
    }
}
