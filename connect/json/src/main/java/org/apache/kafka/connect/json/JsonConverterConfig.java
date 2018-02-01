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
package org.apache.kafka.connect.json;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.storage.ConverterConfig;

import java.util.Map;

/**
 * Configuration options for {@link JsonConverter} instances.
 */
public class JsonConverterConfig extends ConverterConfig {

    public static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";
    public static final boolean SCHEMAS_ENABLE_DEFAULT = true;
    private static final String SCHEMAS_ENABLE_DOC = "Include schemas within each of the serialized values and keys.";
    private static final String SCHEMAS_ENABLE_DISPLAY = "Enable Schemas";

    public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
    public static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
    private static final String SCHEMAS_CACHE_SIZE_DOC = "The maximum number of schemas that can be cached in this converter instance.";
    private static final String SCHEMAS_CACHE_SIZE_DISPLAY = "Schema Cache Size";

    private final static ConfigDef CONFIG;

    static {
        String group = "Schemas";
        int orderInGroup = 0;
        CONFIG = ConverterConfig.newConfigDef();
        CONFIG.define(SCHEMAS_ENABLE_CONFIG, Type.BOOLEAN, SCHEMAS_ENABLE_DEFAULT, Importance.HIGH, SCHEMAS_ENABLE_DOC, group,
                      orderInGroup++, Width.MEDIUM, SCHEMAS_ENABLE_DISPLAY);
        CONFIG.define(SCHEMAS_CACHE_SIZE_CONFIG, Type.INT, SCHEMAS_CACHE_SIZE_DEFAULT, Importance.HIGH, SCHEMAS_CACHE_SIZE_DOC, group,
                      orderInGroup++, Width.MEDIUM, SCHEMAS_CACHE_SIZE_DISPLAY);
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public JsonConverterConfig(Map<String, ?> props) {
        super(CONFIG, props);
    }

    /**
     * Return whether schemas are enabled.
     *
     * @return true if enabled, or false otherwise
     */
    public boolean schemasEnabled() {
        return getBoolean(SCHEMAS_ENABLE_CONFIG);
    }

    /**
     * Get the cache size.
     *
     * @return the cache size
     */
    public int schemaCacheSize() {
        return getInt(SCHEMAS_CACHE_SIZE_CONFIG);
    }
}
