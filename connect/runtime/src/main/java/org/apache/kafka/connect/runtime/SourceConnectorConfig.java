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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.isolation.Plugins;

import java.util.Map;

public class SourceConnectorConfig extends ConnectorConfig {

    private static ConfigDef config = ConnectorConfig.configDef();

    public static ConfigDef configDef() {
        return config;
    }

    public SourceConnectorConfig(Plugins plugins, Map<String, String> props) {
        super(plugins, config, props);
    }

    public static void main(String[] args) {
        System.out.println(config.toHtmlTable());
    }
}
