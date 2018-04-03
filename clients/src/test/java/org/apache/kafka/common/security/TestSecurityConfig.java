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
package org.apache.kafka.common.security;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;

import java.util.Map;

public class TestSecurityConfig extends AbstractConfig {
    private static final ConfigDef CONFIG = new ConfigDef()
            .define(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, Type.STRING, null, Importance.MEDIUM,
                    BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC)
            .define(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, Type.LIST,
                    BrokerSecurityConfigs.DEFAULT_SASL_ENABLED_MECHANISMS,
                    Importance.MEDIUM, BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC)
            .define(BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS, Type.CLASS,
                    null,
                    Importance.MEDIUM, BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC)
            .define(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, Type.CLASS,
                    null, Importance.MEDIUM, BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC)
            .withClientSslSupport()
            .withClientSaslSupport();

    public TestSecurityConfig(Map<?, ?> originals) {
        super(CONFIG, originals, false);
    }
}
