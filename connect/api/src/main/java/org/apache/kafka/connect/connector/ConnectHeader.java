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
package org.apache.kafka.connect.connector;

import org.apache.kafka.connect.data.Schema;

/**
 * <p>
 * Class for headers containing data to be copied to/from Kafka. This corresponds closely to
 * Kafka's Header classes, and holds the data that may be used by both
 * sources and sinks (key, valueSchema, value).
 * </p>
 */
public class ConnectHeader {
    
    private final String key;
    private final Schema valueSchema;
    private final Object value;

    public ConnectHeader(String key, Schema valueSchema, Object value) {
        this.key = key;
        this.valueSchema = valueSchema;
        this.value = value;
    }

    public String key() {
        return key;
    }

    public Object value() {
        return value;
    }

    public Schema valueSchema() {
        return valueSchema;
    }
    
}