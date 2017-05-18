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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.BooleanDeserializer;
import org.apache.kafka.common.serialization.BooleanSerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;

public class PrimativeSubjectConverter implements SubjectConverter {
 
    
    private static Map<Schema, Serializer> schemaSerializerMap = new HashMap<>();
    private static Map<Schema, Deserializer> schemaDeserializerMap = new HashMap<>();

    static {
        register(Schema.BYTES_SCHEMA, new ByteArraySerializer(), new ByteArrayDeserializer());
        register(Schema.INT8_SCHEMA, new ShortSerializer(), new ShortDeserializer());
        register(Schema.INT16_SCHEMA, new IntegerSerializer(), new IntegerDeserializer());
        register(Schema.INT32_SCHEMA, new IntegerSerializer(), new IntegerDeserializer());
        register(Schema.INT64_SCHEMA, new LongSerializer(), new LongDeserializer());
        register(Schema.FLOAT32_SCHEMA, new FloatSerializer(), new FloatDeserializer());
        register(Schema.FLOAT64_SCHEMA, new DoubleSerializer(), new DoubleDeserializer());
        register(Schema.BOOLEAN_SCHEMA, new BooleanSerializer(), new BooleanDeserializer());
        register(Schema.STRING_SCHEMA, new StringSerializer(), new StringDeserializer());
    }
    
    private static void register(Schema schema, Serializer serializer, Deserializer deserializer){
        schemaSerializerMap.put(schema, serializer);
        schemaDeserializerMap.put(schema, deserializer);
    }
    
    private SubjectSchemaRepo subjectSchemaRepo = new MemorySubjectSchemaRepo();
    private Schema defaultSchema = Schema.BYTES_SCHEMA;
    

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        for(String configKey : configs.keySet()) {
            if (configKey.startsWith("converter.subject.")) {
                String subject = configKey.substring("converter.subject.".length());
                Schema.Type type = Schema.Type.valueOf(((String) configs.get(configKey)).toUpperCase());
                if (type != null && type.isPrimitive()) {
                    subjectSchemaRepo.register(subject, SchemaBuilder.type(type).build());
                }
            }
        }
    }

    @Override
    public byte[] fromConnectData(String topic, String subject, Schema schema, Object value) {
        Schema registeredSchema = subjectSchemaRepo.schema(subject);
        if (registeredSchema == null){
            subjectSchemaRepo.register(subject, schema);
        } else if (!schema.type().equals(registeredSchema.type())) {
            throw new SerializationException("schema type for subject does not match registered schema type subject=" + subject);
        } else if (!schema.type().isPrimitive()) {
            throw new SerializationException("only primitive types are supported subject=" + subject);
        }
        return schemaSerializerMap.get(schema).serialize(topic, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, String subject, byte[] value) {
        Schema schema = subjectSchemaRepo.schema(subject);
        if (schema == null){
            schema = defaultSchema;
        }
        Deserializer deserializer = schemaDeserializerMap.get(schema);
        Object object = deserializer.deserialize(topic, value);
        return new SchemaAndValue(schema, object);
    }
    
}
