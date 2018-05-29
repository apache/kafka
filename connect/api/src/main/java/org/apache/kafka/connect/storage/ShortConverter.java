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

import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.connect.data.Schema;

/**
 * {@link Converter} and {@link HeaderConverter} implementation that only supports serializing to and deserializing from short values.
 * It does support handling nulls. When converting from bytes to Kafka Connect format, the converter will always return an
 * optional INT16 schema.
 * <p>
 * This implementation currently does nothing with the topic names or header names.
 */
public class ShortConverter extends NumberConverter<Short> {

    public ShortConverter() {
        super("short", Schema.OPTIONAL_INT16_SCHEMA, new ShortSerializer(), new ShortDeserializer());
    }
}
