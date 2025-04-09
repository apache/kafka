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

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

/**
 * <code>OAuthBearerConfig</code> is a utility class to perform basic configuration-related
 * logic and is separated out here for easier, more direct testing.
 */
public class OAuthBearerConfig extends OAuthBearerAbstractConfig {

    private final Map<String, ?> configs;

    private final String prefix;

    public OAuthBearerConfig(Map<String, ?> configs, String saslMechanism) {
        this.configs = configs;

        if (!Utils.isBlank(saslMechanism))
            this.prefix = ListenerName.saslMechanismPrefix(saslMechanism.trim());
        else
            this.prefix = null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String name) {
        T value = (T) configs.get(prefix + name);

        if (value != null)
            return value;

        return (T) configs.get(name);
    }
}
