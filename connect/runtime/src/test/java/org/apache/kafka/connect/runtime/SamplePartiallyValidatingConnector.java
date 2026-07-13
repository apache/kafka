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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import java.util.List;
import java.util.Map;

/**
 * The purpose of this connector is to check that the error validation process can handle partially validated configuration.
 * This connector will validate only a subset of the overall configuration.
 * Debezium (and maybe others) have a concept of deprecated field that are not validated.
 */
public class SamplePartiallyValidatingConnector extends SampleSourceConnector {

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("required", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "required docs")
                .define("optional", ConfigDef.Type.STRING, "defaultVal", ConfigDef.Importance.HIGH, "optional docs");
    }

    /**
     * Skips validating "optional", so it produces no ConfigValue.
     */
    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        // do not validate "optional" on purpose
        ConfigDef configDef = new ConfigDef()
                .define("required", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "required docs");
        List<ConfigValue> configValues = configDef
                .validate(connectorConfigs);
        return new Config(configValues);
    }
}
