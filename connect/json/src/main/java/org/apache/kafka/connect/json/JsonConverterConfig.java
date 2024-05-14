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

import java.util.Locale;
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

    public static final String DECIMAL_FORMAT_CONFIG = "decimal.format";
    public static final String DECIMAL_FORMAT_DEFAULT = DecimalFormat.BASE64.name();
    private static final String DECIMAL_FORMAT_DOC = "Controls which format this converter will serialize decimals in."
        + " This value is case insensitive and can be either 'BASE64' (default) or 'NUMERIC'";
    private static final String DECIMAL_FORMAT_DISPLAY = "Decimal Format";

    public static final String REPLACE_NULL_WITH_DEFAULT_CONFIG = "replace.null.with.default";
    public static final boolean REPLACE_NULL_WITH_DEFAULT_DEFAULT = true;
    private static final String REPLACE_NULL_WITH_DEFAULT_DOC = "Whether to replace fields that have a default value and that are null to the default value. When set to true, the default value is used, otherwise null is used.";
    private static final String REPLACE_NULL_WITH_DEFAULT_DISPLAY = "Replace null with default";

    private final static ConfigDef CONFIG;

    static {
        String group = "Schemas";
        int orderInGroup = 0;
        CONFIG = ConverterConfig.newConfigDef();
        CONFIG.define(SCHEMAS_ENABLE_CONFIG, Type.BOOLEAN, SCHEMAS_ENABLE_DEFAULT, Importance.HIGH, SCHEMAS_ENABLE_DOC, group,
                      orderInGroup++, Width.MEDIUM, SCHEMAS_ENABLE_DISPLAY);
        CONFIG.define(SCHEMAS_CACHE_SIZE_CONFIG, Type.INT, SCHEMAS_CACHE_SIZE_DEFAULT, Importance.HIGH, SCHEMAS_CACHE_SIZE_DOC, group,
                      orderInGroup++, Width.MEDIUM, SCHEMAS_CACHE_SIZE_DISPLAY);

        group = "Serialization";
        orderInGroup = 0;
        CONFIG.define(
            DECIMAL_FORMAT_CONFIG, Type.STRING, DECIMAL_FORMAT_DEFAULT,
            ConfigDef.CaseInsensitiveValidString.in(
                DecimalFormat.BASE64.name(),
                DecimalFormat.NUMERIC.name()),
            Importance.LOW, DECIMAL_FORMAT_DOC, group, orderInGroup++,
            Width.MEDIUM, DECIMAL_FORMAT_DISPLAY);
        CONFIG.define(
                REPLACE_NULL_WITH_DEFAULT_CONFIG, Type.BOOLEAN, REPLACE_NULL_WITH_DEFAULT_DEFAULT,
                Importance.LOW, REPLACE_NULL_WITH_DEFAULT_DOC, group, orderInGroup++,
                Width.MEDIUM, REPLACE_NULL_WITH_DEFAULT_DISPLAY);
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    // cached config values
    private final boolean schemasEnabled;
    private final int schemaCacheSize;
    private final DecimalFormat decimalFormat;
    private final boolean replaceNullWithDefault;

    @SuppressWarnings("this-escape")
    public JsonConverterConfig(Map<String, ?> props) {
        super(CONFIG, props);
        this.schemasEnabled = getBoolean(SCHEMAS_ENABLE_CONFIG);
        this.schemaCacheSize = getInt(SCHEMAS_CACHE_SIZE_CONFIG);
        this.decimalFormat = DecimalFormat.valueOf(getString(DECIMAL_FORMAT_CONFIG).toUpperCase(Locale.ROOT));
        this.replaceNullWithDefault = getBoolean(REPLACE_NULL_WITH_DEFAULT_CONFIG);
    }

    /**
     * Return whether schemas are enabled.
     *
     * @return true if enabled, or false otherwise
     */
    public boolean schemasEnabled() {
        return schemasEnabled;
    }

    /**
     * Get the cache size.
     *
     * @return the cache size
     */
    public int schemaCacheSize() {
        return schemaCacheSize;
    }

    /**
     * Get the serialization format for decimal types.
     *
     * @return the decimal serialization format
     */
    public DecimalFormat decimalFormat() {
        return decimalFormat;
    }

    /**
     * Whether to replace null values with the default value when serializing and deserializing fields
     * @return true if we want to use the default value
     */
    public boolean replaceNullWithDefault() {
        return replaceNullWithDefault;
    }

}
