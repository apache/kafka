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
package org.apache.kafka.connect.converters;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.storage.ConverterConfig;

import java.util.Map;

/**
 * Configuration options for instances of {@link LongConverter}, {@link IntegerConverter}, {@link ShortConverter}, {@link DoubleConverter},
 * and {@link FloatConverter} instances.
 */
public class NumberConverterConfig extends ConverterConfig {

    private final static ConfigDef CONFIG = ConverterConfig.newConfigDef();

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public NumberConverterConfig(Map<String, ?> props) {
        super(CONFIG, props);
    }
}
