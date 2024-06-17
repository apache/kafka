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
package org.apache.kafka.connect.storage;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Configuration options for {@link StringConverter} instances.
 */
public class StringConverterConfig extends ConverterConfig {

    public static final String ENCODING_CONFIG = "converter.encoding";
    public static final String ENCODING_DEFAULT = StandardCharsets.UTF_8.name();
    private static final String ENCODING_DOC = "The name of the Java character set to use for encoding strings as byte arrays.";
    private static final String ENCODING_DISPLAY = "Encoding";

    private final static ConfigDef CONFIG;

    static {
        CONFIG = ConverterConfig.newConfigDef();
        CONFIG.define(ENCODING_CONFIG, Type.STRING, ENCODING_DEFAULT, Importance.HIGH, ENCODING_DOC, null, -1, Width.MEDIUM,
                      ENCODING_DISPLAY);
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public StringConverterConfig(Map<String, ?> props) {
        super(CONFIG, props);
    }

    /**
     * Get the string encoding.
     *
     * @return the encoding; never null
     */
    public String encoding() {
        return getString(ENCODING_CONFIG);
    }
}
