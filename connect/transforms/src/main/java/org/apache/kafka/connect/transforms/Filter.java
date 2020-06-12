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
package org.apache.kafka.connect.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;

/**
 * Drops all records, filtering them from subsequent transformations in the chain.
 * This is intended to be used conditionally to filter out records matching (or not matching)
 * a particular {@link org.apache.kafka.connect.transforms.predicates.Predicate}.
 * @param <R> The type of record.
 */
public class Filter<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Drops all records, filtering them from subsequent transformations in the chain. " +
            "This is intended to be used conditionally to filter out records matching (or not matching) " +
            "a particular Predicate.";
    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    @Override
    public R apply(R record) {
        return null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
