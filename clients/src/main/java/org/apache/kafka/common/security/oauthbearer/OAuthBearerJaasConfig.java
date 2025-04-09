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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.security.auth.login.AppConfigurationEntry;

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
        this.options = create(saslMechanism, jaasConfigEntries);
    }

    public Map<String, Object> options() {
        return options;
    }

    public static Map<String, Object> create(String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism))
            throw new ConfigException(String.format("Unexpected SASL mechanism: %s", saslMechanism));

        if (Objects.requireNonNull(jaasConfigEntries).size() != 1 || jaasConfigEntries.get(0) == null)
            throw new ConfigException(String.format("Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)", jaasConfigEntries.size()));

        return Collections.unmodifiableMap(jaasConfigEntries.get(0).getOptions());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String name) {
        return (T) options.get(name);
    }
}
