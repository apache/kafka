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

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.apache.kafka.copycat.data.GenericRecord;
import org.apache.kafka.copycat.data.GenericRecordBuilder;
import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.data.SchemaBuilder;
import org.apache.kafka.copycat.errors.CopycatRuntimeException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Utilities for converting between our runtime data format and Avro, and (de)serializing that data.
 */
public class AvroData {
    private static final Map<String, org.apache.avro.Schema> PRIMITIVE_SCHEMAS;

    static {
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        PRIMITIVE_SCHEMAS = new HashMap<String, org.apache.avro.Schema>();
        PRIMITIVE_SCHEMAS.put("Null", createPrimitiveSchema(parser, "null"));
        PRIMITIVE_SCHEMAS.put("Boolean", createPrimitiveSchema(parser, "boolean"));
        PRIMITIVE_SCHEMAS.put("Integer", createPrimitiveSchema(parser, "int"));
        PRIMITIVE_SCHEMAS.put("Long", createPrimitiveSchema(parser, "long"));
        PRIMITIVE_SCHEMAS.put("Float", createPrimitiveSchema(parser, "float"));
        PRIMITIVE_SCHEMAS.put("Double", createPrimitiveSchema(parser, "double"));
        PRIMITIVE_SCHEMAS.put("String", createPrimitiveSchema(parser, "string"));
        PRIMITIVE_SCHEMAS.put("Bytes", createPrimitiveSchema(parser, "bytes"));
    }

    private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();
    private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();

    /**
     * Convert this object, in the org.apache.kafka.copycat.data format, into an Avro object.
     */
    public static Object convertToAvro(Object value) {
        if (value == null || value instanceof Byte || value instanceof Short ||
                value instanceof Integer || value instanceof Long ||
                value instanceof Float || value instanceof Double ||
                value instanceof Boolean ||
                value instanceof byte[] || value instanceof ByteBuffer ||
                value instanceof CharSequence) {
            // Note that using CharSequence allows Utf8 -- either copycat or Avro variants -- to pass
            // through, but this should be fine as long as they are always handled as CharSequences
            return value;
        } else if (value instanceof GenericRecord) {
            GenericRecord recordValue = (GenericRecord) value;
            org.apache.avro.Schema avroSchema = asAvroSchema(recordValue.getSchema());
            org.apache.avro.generic.GenericRecordBuilder builder
                    = new org.apache.avro.generic.GenericRecordBuilder(avroSchema);
            for (Schema.Field field : recordValue.getSchema().getFields()) {
                builder.set(field.name(), convertToAvro(recordValue.get(field.name())));
            }
            return builder.build();
        } else if (value instanceof Collection) {
            Collection collection = (Collection) value;
            List<Object> converted = new ArrayList<Object>(collection.size());
            for (Object elem : collection) {
                converted.add(convertToAvro(elem));
            }
            return converted;
        } else if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) value;
            Map<String, Object> converted = new TreeMap<String, Object>();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                converted.put(entry.getKey(), convertToAvro(entry.getValue()));
            }
            return converted;
        }
        // Fixed and enum are handled by byte[] and String

        throw new RuntimeException("Couldn't convert " + value + " to Avro.");
    }

    /**
     * Serialize this object, in org.apache.kafka.copycat.data format, to Avro's binary encoding
     */
    public static ByteBuffer serializeToAvro(Object value) {
        Object asAvro = convertToAvro(value);

        if (value == null) {
            return null;
        }

        try {
            org.apache.avro.Schema schema = getSchema(asAvro);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = ENCODER_FACTORY.directBinaryEncoder(out, null);
            DatumWriter<Object> writer;
            writer = new GenericDatumWriter<Object>(schema);
            writer.write(asAvro, encoder);
            encoder.flush();
            return ByteBuffer.wrap(out.toByteArray());
        } catch (IOException e) {
            throw new CopycatRuntimeException("Couldn't serialize object to Avro", e);
        }
    }


    /**
     * Convert the given object, in Avro format, into an org.apache.kafka.copycat.data object.
     */
    public static Object convertFromAvro(Object value) {
        if (value == null || value instanceof Byte || value instanceof Short ||
                value instanceof Integer || value instanceof Long ||
                value instanceof Float || value instanceof Double ||
                value instanceof Boolean ||
                value instanceof byte[] || value instanceof ByteBuffer) {
            return value;
        } else if (value instanceof CharSequence) {
            // We need to be careful about CharSequences. This could be a String or a Utf8. If we passed
            // Utf8 values directly through using Avro's implementation, equality wouldn't work.
            if (value instanceof org.apache.avro.util.Utf8) {
                return value.toString();
            } else {
                return value;
            }
        } else if (value instanceof org.apache.avro.generic.GenericRecord) {
            org.apache.avro.generic.GenericRecord recordValue
                    = (org.apache.avro.generic.GenericRecord) value;
            Schema copycatSchema = asCopycatSchema(recordValue.getSchema());
            GenericRecordBuilder builder = new GenericRecordBuilder(copycatSchema);
            for (org.apache.avro.Schema.Field field : recordValue.getSchema().getFields()) {
                builder.set(field.name(), convertFromAvro(recordValue.get(field.name())));
            }
            return builder.build();
        } else if (value instanceof Collection) {
            Collection collection = (Collection) value;
            List<Object> converted = new ArrayList<Object>(collection.size());
            for (Object elem : collection) {
                converted.add(convertFromAvro(elem));
            }
            return converted;
        } else if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) value;
            Map<String, Object> converted = new TreeMap<String, Object>();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                converted.put(entry.getKey(), convertFromAvro(entry.getValue()));
            }
            return converted;
        }
        // Fixed and enum are handled by byte[] and String

        throw new RuntimeException("Couldn't convert " + value + " from Avro.");
    }

    /**
     * Deserialize and convert the provided binary Avro-serialized bytes into
     * org.apache.kafka.copycat.data format.
     */
    public static Object deserializeFromAvro(ByteBuffer serialized, Schema schema) {
        org.apache.avro.Schema avroSchema = asAvroSchema(schema);
        DatumReader reader = new GenericDatumReader(avroSchema);
        try {
            Object deserialized =
                    reader.read(null, DECODER_FACTORY.binaryDecoder(serialized.array(), null));
            return convertFromAvro(deserialized);
        } catch (IOException e) {
            throw new CopycatRuntimeException("Couldn't deserialize object from Avro", e);
        }
    }


    private static org.apache.avro.Schema createPrimitiveSchema(
            org.apache.avro.Schema.Parser parser, String type) {
        String schemaString = String.format("{\"type\" : \"%s\"}", type);
        return parser.parse(schemaString);
    }

    private static org.apache.avro.Schema getSchema(Object object) {
        if (object == null) {
            return PRIMITIVE_SCHEMAS.get("Null");
        } else if (object instanceof Boolean) {
            return PRIMITIVE_SCHEMAS.get("Boolean");
        } else if (object instanceof Integer) {
            return PRIMITIVE_SCHEMAS.get("Integer");
        } else if (object instanceof Long) {
            return PRIMITIVE_SCHEMAS.get("Long");
        } else if (object instanceof Float) {
            return PRIMITIVE_SCHEMAS.get("Float");
        } else if (object instanceof Double) {
            return PRIMITIVE_SCHEMAS.get("Double");
        } else if (object instanceof CharSequence) {
            return PRIMITIVE_SCHEMAS.get("String");
        } else if (object instanceof byte[] || object instanceof ByteBuffer) {
            return PRIMITIVE_SCHEMAS.get("Bytes");
        } else if (object instanceof org.apache.avro.generic.GenericContainer) {
            return ((org.apache.avro.generic.GenericContainer) object).getSchema();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported Avro type: " + object.getClass());
        }
    }


    // Implementation note -- I considered trying to unify asAvroSchema and asCopycatSchema through
    // a generic implementation. In practice this probably won't be worth the relatively minor
    // amount of code duplication avoided since a) the generic version is messy and b) the Copycat
    // type system and Avro type system are likely to diverge anyway, which would eventually
    // require splitting them up.

    public static org.apache.avro.Schema asAvroSchema(Schema schema) {
        switch (schema.getType()) {
            case BOOLEAN:
                return org.apache.avro.SchemaBuilder.builder().booleanType();
            case BYTES:
                return org.apache.avro.SchemaBuilder.builder().bytesType();
            case DOUBLE:
                return org.apache.avro.SchemaBuilder.builder().doubleType();
            case FLOAT:
                return org.apache.avro.SchemaBuilder.builder().floatType();
            case INT:
                return org.apache.avro.SchemaBuilder.builder().intType();
            case LONG:
                return org.apache.avro.SchemaBuilder.builder().longType();
            case NULL:
                return org.apache.avro.SchemaBuilder.builder().nullType();
            case STRING:
                return org.apache.avro.SchemaBuilder.builder().stringType();

            case RECORD: {
                List<org.apache.avro.Schema.Field> fields = new ArrayList<org.apache.avro.Schema.Field>();
                for (Schema.Field field : schema.getFields()) {
                    // NOTE: Order ignored
                    // TODO: Providing a default value would require translating since Avro's is a JsonNode
                    fields.add(new org.apache.avro.Schema.Field(
                            field.name(), asAvroSchema(field.schema()), field.doc(), null));
                }

                org.apache.avro.Schema result = org.apache.avro.Schema.createRecord(
                        schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
                result.setFields(fields);
                return result;
            }

            case UNION: {
                List<org.apache.avro.Schema> unionTypes = new ArrayList<org.apache.avro.Schema>();
                for (Schema origType : schema.getTypes()) {
                    unionTypes.add(asAvroSchema(origType));
                }
                return org.apache.avro.Schema.createUnion(unionTypes);
            }

            case ARRAY:
                return org.apache.avro.Schema.createArray(asAvroSchema(schema.getElementType()));

            case ENUM:
                return org.apache.avro.Schema.createEnum(schema.getName(), schema.getDoc(), schema
                        .getNamespace(), schema.getEnumSymbols());

            case FIXED:
                return org.apache.avro.Schema.createFixed(schema.getName(), schema.getDoc(), schema
                        .getNamespace(), schema.getFixedSize());

            case MAP:
                return org.apache.avro.Schema.createMap(asAvroSchema(schema.getValueType()));

            default:
                throw new CopycatRuntimeException("Couldn't translate unsupported schema type "
                        + schema.getType().getName() + ".");
        }
    }


    public static Schema asCopycatSchema(org.apache.avro.Schema schema) {
        switch (schema.getType()) {
            case BOOLEAN:
                return SchemaBuilder.builder().booleanType();
            case BYTES:
                return SchemaBuilder.builder().bytesType();
            case DOUBLE:
                return SchemaBuilder.builder().doubleType();
            case FLOAT:
                return SchemaBuilder.builder().floatType();
            case INT:
                return SchemaBuilder.builder().intType();
            case LONG:
                return SchemaBuilder.builder().longType();
            case NULL:
                return SchemaBuilder.builder().nullType();
            case STRING:
                return SchemaBuilder.builder().stringType();

            case RECORD: {
                List<Schema.Field> fields = new ArrayList<Schema.Field>();
                for (org.apache.avro.Schema.Field field : schema.getFields()) {
                    // NOTE: Order ignored
                    // TODO: Providing a default value would require translating since Avro's is a JsonNode
                    fields.add(new Schema.Field(
                            field.name(), asCopycatSchema(field.schema()), field.doc(), null));
                }

                Schema result = Schema.createRecord(
                        schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
                result.setFields(fields);
                return result;
            }

            case UNION: {
                List<Schema> unionTypes = new ArrayList<Schema>();
                for (org.apache.avro.Schema origType : schema.getTypes()) {
                    unionTypes.add(asCopycatSchema(origType));
                }
                return Schema.createUnion(unionTypes);
            }

            case ARRAY:
                return Schema.createArray(asCopycatSchema(schema.getElementType()));

            case ENUM:
                return Schema.createEnum(schema.getName(), schema.getDoc(), schema
                        .getNamespace(), schema.getEnumSymbols());

            case FIXED:
                return Schema.createFixed(schema.getName(), schema.getDoc(), schema
                        .getNamespace(), schema.getFixedSize());

            case MAP:
                return Schema.createMap(asCopycatSchema(schema.getValueType()));

            default:
                throw new CopycatRuntimeException("Couldn't translate unsupported schema type "
                        + schema.getType().getName() + ".");
        }
    }

}
