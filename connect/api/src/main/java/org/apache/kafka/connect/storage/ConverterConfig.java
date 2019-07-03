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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * Abstract class that defines the configuration options for {@link Converter} and {@link HeaderConverter} instances.
 */
public abstract class ConverterConfig extends AbstractConfig {

    public static final String TYPE_CONFIG = "converter.type";
    private static final String TYPE_DOC = "How this converter will be used.";

    /**
     * Create a new {@link ConfigDef} instance containing the configurations defined by ConverterConfig. This can be called by subclasses.
     *
     * @return the ConfigDef; never null
     */
    public static ConfigDef newConfigDef() {
        return new ConfigDef().define(TYPE_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                                      in(ConverterType.KEY.getName(), ConverterType.VALUE.getName(), ConverterType.HEADER.getName()),
                                      Importance.LOW, TYPE_DOC);
    }

    protected ConverterConfig(ConfigDef configDef, Map<String, ?> props) {
        super(configDef, props, true);
    }

    /**
     * Get the type of converter as defined by the {@link #TYPE_CONFIG} configuration.
     * @return the converter type; never null
     */
    public ConverterType type() {
        return ConverterType.withName(getString(TYPE_CONFIG));
    }
}
