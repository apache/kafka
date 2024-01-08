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
package org.apache.kafka.server;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Properties;

public class DynamicBrokerConfig {
    public static Properties resolveVariableConfigs(Properties propsOriginal) {
        Properties props = new Properties();
        AbstractConfig config = new AbstractConfig(new ConfigDef(), propsOriginal, false);
        config.originals().forEach((key, value) -> {
            if (!key.startsWith(AbstractConfig.CONFIG_PROVIDERS_CONFIG)) {
                props.put(key, value);
            }
        });
        return props;
    }
}
