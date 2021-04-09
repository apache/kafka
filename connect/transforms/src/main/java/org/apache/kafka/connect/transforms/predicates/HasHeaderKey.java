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
package org.apache.kafka.connect.transforms.predicates;

import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * A predicate which is true for records with at least one header with the configured name.
 * @param <R> The type of connect record.
 */
public class HasHeaderKey<R extends ConnectRecord<R>> implements Predicate<R> {

    private static final String NAME_CONFIG = "name";
    public static final String OVERVIEW_DOC = "A predicate which is true for records with at least one header with the configured name.";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
            new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM,
            "The header name.");
    private String name;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public boolean test(R record) {
        Iterator<Header> headerIterator = record.headers().allWithName(name);
        return headerIterator != null && headerIterator.hasNext();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.name = new SimpleConfig(config(), configs).getString(NAME_CONFIG);
    }

    @Override
    public String toString() {
        return "HasHeaderKey{" +
                "name='" + name + '\'' +
                '}';
    }
}
