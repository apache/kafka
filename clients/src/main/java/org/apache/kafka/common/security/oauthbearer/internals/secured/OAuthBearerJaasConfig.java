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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.jaasOptions;

/**
 * <code>OAuthBearerJaasConfig</code> is a utility class to perform logic for the JAAS options and
 * is separated out here for easier, more direct testing.
 */
public class OAuthBearerJaasConfig extends OAuthBearerAbstractConfig {

    private final Map<String, Object> options;

    public OAuthBearerJaasConfig(Map<String, Object> options) {
        this.options = Collections.unmodifiableMap(options);
    }

    public OAuthBearerJaasConfig(String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        this.options = jaasOptions(saslMechanism, jaasConfigEntries);
    }

    public Map<String, Object> options() {
        return options;
    }

    @Override
    public String getString(String key) {
        String s = get(key);

        if (Utils.isBlank(s)) {
            throw new ConfigException("No value was found for the OAuth option " + key + " in " + SASL_JAAS_CONFIG);
        } else {
            return s.trim();
        }
    }

    @Override
    public String getPassword(String key) {
        return getString(key);
    }

    @Override
    public boolean containsKey(String key) {
        return options.get(key) != null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        T value = (T) options.get(key);

        if (value != null)
            return value;

        throw new ConfigException("No value was found for the OAuth option " + key + " in " + SASL_JAAS_CONFIG);
    }
}