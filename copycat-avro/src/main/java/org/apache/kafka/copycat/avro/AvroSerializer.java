/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.avro;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import org.apache.kafka.copycat.errors.CopycatRuntimeException;
import org.apache.kafka.copycat.storage.OffsetSerializer;

import java.util.Map;

public class AvroSerializer extends AbstractKafkaAvroSerializer implements OffsetSerializer<Object> {

    private boolean isKey;


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        Object url = configs.get(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
        if (url == null) {
            throw new CopycatRuntimeException("Missing Schema registry url!");
        }
        Object maxSchemaObject = configs.get(
                AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG);
        if (maxSchemaObject == null) {
            schemaRegistry = new CachedSchemaRegistryClient(
                    (String) url, AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);
        } else {
            schemaRegistry = new CachedSchemaRegistryClient(
                    (String) url, (Integer) maxSchemaObject);
        }
    }

    @Override
    public byte[] serializeOffset(String connector, Object data) {
        String subject;
        if (isKey) {
            subject = connector + "-key";
        } else {
            subject = connector + "-value";
        }
        return serializeImpl(subject, data);
    }

    @Override
    public void close() {

    }
}
