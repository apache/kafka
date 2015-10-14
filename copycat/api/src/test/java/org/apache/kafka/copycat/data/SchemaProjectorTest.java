/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package org.apache.kafka.copycat.data;

import org.apache.kafka.copycat.data.Schema.Type;
import org.apache.kafka.copycat.errors.DataException;
import org.apache.kafka.copycat.errors.SchemaProjectorException;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SchemaProjectorTest {

    @Test
    public void testPrimitiveTypeProjection() throws Exception {
        Object projected;
        projected = SchemaProjector.project(Schema.BOOLEAN_SCHEMA, Schema.BOOLEAN_SCHEMA, false);
        assertEquals(false, projected);

        byte[] bytes = {(byte) 1, (byte) 2};
        projected  = SchemaProjector.project(Schema.BYTES_SCHEMA, Schema.BYTES_SCHEMA, bytes);
        assertEquals(bytes, projected);

        projected = SchemaProjector.project(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA, "abc");
        assertEquals("abc", projected);

        try {
            SchemaProjector.project(Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.BOOLEAN_SCHEMA, false);
            fail("Cannot project optional schema to schema with no default value.");
        } catch (DataException e) {
            // expected
        }

        try {
            SchemaProjector.project(Schema.OPTIONAL_BYTES_SCHEMA, Schema.BYTES_SCHEMA, bytes);
            fail("Cannot project optional schema to schema with no default value.");
        } catch (DataException e) {
            // expected
        }

        try {
            SchemaProjector.project(Schema.OPTIONAL_STRING_SCHEMA, Schema.STRING_SCHEMA, "abc");
            fail("Cannot project optional schema to schema with no default value.");
        } catch (DataException e) {
            // expected
        }
    }

    @Test
    public void testNumericTypeProjection() throws Exception {
        Schema[] promotableSchemas = {Schema.INT8_SCHEMA, Schema.INT16_SCHEMA, Schema.INT32_SCHEMA, Schema.INT64_SCHEMA, Schema.FLOAT32_SCHEMA, Schema.FLOAT64_SCHEMA};
        Object promoted;
        for (int i = 0; i < promotableSchemas.length; ++i) {
            Schema writerSchema = promotableSchemas[i];
            for (int j = i; j < promotableSchemas.length; ++j) {
                Schema readerSchema = promotableSchemas[j];
                promoted = SchemaProjector.project(writerSchema, readerSchema, 255);
                assertEquals(255, promoted);
            }
        }

        Schema[] nonPromotableSchemas = {Schema.BOOLEAN_SCHEMA, Schema.BYTES_SCHEMA, Schema.STRING_SCHEMA};
        for (Schema promotableSchema: promotableSchemas) {
            for (Schema nonPromotableSchema: nonPromotableSchemas) {
                Object dummy = new Object();
                try {
                    SchemaProjector.project(promotableSchema, nonPromotableSchema, dummy);
                    fail("Cannot promote " +  promotableSchema.type() + " to " + nonPromotableSchema.type());
                } catch (DataException e) {
                    // expected
                }
            }
        }
    }

    @Test
    public void testPrimitiveOptionalProjection() throws Exception {

        verifyOptionalProjection(Schema.OPTIONAL_BOOLEAN_SCHEMA, Type.BOOLEAN, false, true, true);
        verifyOptionalProjection(Schema.OPTIONAL_BOOLEAN_SCHEMA, Type.BOOLEAN, false, true, false);

        byte[] bytes = {(byte) 1, (byte) 2};
        byte[] defaultBytes = {(byte) 3, (byte) 4};
        verifyOptionalProjection(Schema.OPTIONAL_BYTES_SCHEMA, Type.BYTES, bytes, defaultBytes, true);
        verifyOptionalProjection(Schema.OPTIONAL_BYTES_SCHEMA, Type.BYTES, bytes, defaultBytes, false);

        verifyOptionalProjection(Schema.OPTIONAL_STRING_SCHEMA, Type.STRING, "abc", "def", true);
        verifyOptionalProjection(Schema.OPTIONAL_STRING_SCHEMA, Type.STRING, "abc", "def", false);

        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT8, (byte) 12, (byte) 127, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT8, (byte) 12, (byte) 127, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT16, (byte) 12, (short) 127, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT16, (byte) 12, (short) 127, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT32, (byte) 12, 12789, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT32, (byte) 12, 12789, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT64, (byte) 12, 127890L, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT64, (byte) 12, 127890L, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.FLOAT32, (byte) 12, 3.45F, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.FLOAT32, (byte) 12, 3.45F, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.FLOAT64, (byte) 12, 3.4567, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.FLOAT64, (byte) 12, 3.4567, false);

        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.INT16, (short) 12, (short) 127, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.INT16, (short) 12, (short) 127, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.INT32, (short) 12, 12789, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.INT32, (short) 12, 12789, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.INT64, (short) 12, 127890L, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.INT64, (short) 12, 127890L, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.FLOAT32, (short) 12, 3.45F, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.FLOAT32, (short) 12, 3.45F, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.FLOAT64, (short) 12, 3.4567, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.FLOAT64, (short) 12, 3.4567, false);

        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.INT32, 12, 12789, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.INT32, 12, 12789, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.INT64, 12, 127890L, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.INT64, 12, 127890L, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.FLOAT32, 12, 3.45F, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.FLOAT32, 12, 3.45F, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.FLOAT64, 12, 3.4567, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.FLOAT64, 12, 3.4567, false);

        verifyOptionalProjection(Schema.OPTIONAL_INT64_SCHEMA, Type.INT64, 12L, 127890L, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT64_SCHEMA, Type.INT64, 12L, 127890L, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT64_SCHEMA, Type.FLOAT32, 12L, 3.45F, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT64_SCHEMA, Type.FLOAT32, 12L, 3.45F, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT64_SCHEMA, Type.FLOAT64, 12L, 3.4567, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT64_SCHEMA, Type.FLOAT64, 12L, 3.4567, false);

        verifyOptionalProjection(Schema.OPTIONAL_FLOAT32_SCHEMA, Type.FLOAT32, 12.345F, 3.45F, true);
        verifyOptionalProjection(Schema.OPTIONAL_FLOAT32_SCHEMA, Type.FLOAT32, 12.345F, 3.45F, false);
        verifyOptionalProjection(Schema.OPTIONAL_FLOAT32_SCHEMA, Type.FLOAT64, 12.345F, 3.4567, true);
        verifyOptionalProjection(Schema.OPTIONAL_FLOAT32_SCHEMA, Type.FLOAT64, 12.345F, 3.4567, false);

        verifyOptionalProjection(Schema.OPTIONAL_FLOAT32_SCHEMA, Type.FLOAT64, 12.345, 3.4567, true);
        verifyOptionalProjection(Schema.OPTIONAL_FLOAT32_SCHEMA, Type.FLOAT64, 12.345, 3.4567, false);
    }

    @Test
    public void testStructAddField() throws Exception {
        Schema writerSchema = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .build();
        Struct writerStruct = new Struct(writerSchema);
        writerStruct.put("field", 1);

        Schema readerSchema = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .field("field2", SchemaBuilder.int32().defaultValue(123).build())
                .build();

        Struct readerStruct = (Struct) SchemaProjector.project(writerSchema, readerSchema, writerStruct);


        assertEquals(1, (int) readerStruct.getInt32("field"));
        assertEquals(123, (int) readerStruct.getInt32("field2"));

        Schema incompatibleReaderSchema = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .build();

        try {
            SchemaProjector.project(writerSchema, incompatibleReaderSchema, writerStruct);
            fail("Incompatible schema.");
        } catch (DataException e) {
            // expected
        }
    }

    @Test
    public void testStructRemoveField() throws Exception {
        Schema writerSchema = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .build();

        Schema readerSchema = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .build();

        Struct writerStruct = new Struct(writerSchema);
        writerStruct.put("field", 1);
        writerStruct.put("field2", 234);

        Struct readerStruct = (Struct) SchemaProjector.project(writerSchema, readerSchema, writerStruct);

        assertEquals(1, readerStruct.get("field"));
        try {
            readerStruct.get("field2");
            fail("field2 is not part of the projected struct");
        } catch (DataException e) {
            // expected
        }
    }

    @Test
    public void testNestedSchemaProjection() throws Exception {
        Schema writerFlatSchema = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .build();
        Schema readerFlatSchema = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .field("field2", SchemaBuilder.int32().defaultValue(123).build())
                .build();
        Schema writerNestedSchema = SchemaBuilder.struct()
                .field("first", Schema.INT32_SCHEMA)
                .field("second", Schema.STRING_SCHEMA)
                .field("array", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
                .field("map", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
                .field("nested", writerFlatSchema)
                .build();
        Schema readerNestedSchema = SchemaBuilder.struct()
                .field("first", Schema.INT32_SCHEMA)
                .field("second", Schema.STRING_SCHEMA)
                .field("array", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
                .field("map", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
                .field("nested", readerFlatSchema)
                .build();

        Struct writerFlatStruct = new Struct(writerFlatSchema);
        writerFlatStruct.put("field", 113);

        Struct writerNestedStruct = new Struct(writerNestedSchema);
        writerNestedStruct.put("first", 1);
        writerNestedStruct.put("second", "abc");
        writerNestedStruct.put("array", Arrays.asList(1, 2));
        writerNestedStruct.put("map", Collections.singletonMap(5, "def"));
        writerNestedStruct.put("nested", writerFlatStruct);

        Struct readerNestedStruct = (Struct) SchemaProjector.project(writerNestedSchema, readerNestedSchema, writerNestedStruct);
        assertEquals(1, readerNestedStruct.get("first"));
        assertEquals("abc", readerNestedStruct.get("second"));
        assertEquals(Arrays.asList(1, 2), (List<?>) readerNestedStruct.get("array"));
        assertEquals(Collections.singletonMap(5, "def"), (Map<?, ?>) readerNestedStruct.get("map"));

        Struct projectedStruct = (Struct) readerNestedStruct.get("nested");
        assertEquals(113, projectedStruct.get("field"));
        assertEquals(123, projectedStruct.get("field2"));
    }

    @Test
    public void testLogicalTypeProjection() throws Exception {
        Schema[] logicalTypeSchemas = {Decimal.schema(2), Date.SCHEMA, Time.SCHEMA, Timestamp.SCHEMA};
        Object projected;

        BigDecimal testDecimal = new BigDecimal(new BigInteger("156"), 2);
        projected = SchemaProjector.project(Decimal.schema(2), Decimal.schema(2), testDecimal);
        assertEquals(testDecimal, projected);

        projected = SchemaProjector.project(Date.SCHEMA, Date.SCHEMA, 1000);
        assertEquals(1000, projected);

        projected = SchemaProjector.project(Time.SCHEMA, Time.SCHEMA, 231);
        assertEquals(231, projected);

        projected = SchemaProjector.project(Timestamp.SCHEMA, Timestamp.SCHEMA, 34567);
        assertEquals(34567, projected);

        Object dummy = new Object();
        Schema namedSchame = SchemaBuilder.int32().name("invalidLogicalTypeName").build();
        for (Schema logicalTypeSchema: logicalTypeSchemas) {
            try {
                SchemaProjector.project(logicalTypeSchema, Schema.BOOLEAN_SCHEMA, dummy);
                fail("Cannot project logical types to non-logical types.");
            } catch (SchemaProjectorException e) {
                // expected
            }

            try {
                SchemaProjector.project(logicalTypeSchema, namedSchame, dummy);
                fail("Reader name is not a valid logical type name.");
            } catch (SchemaProjectorException e) {
                // expected
            }

            try {
                SchemaProjector.project(Schema.BOOLEAN_SCHEMA, logicalTypeSchema, dummy);
                fail("Cannot project non-logical types to logical types.");
            } catch (SchemaProjectorException e) {
                // expected
            }
        }
    }

    @Test
    public void testArrayProjection() throws Exception {
        Schema writer = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
        Schema identityReader = SchemaBuilder.array(Schema.INT32_SCHEMA).build();

        Object projected = SchemaProjector.project(writer, identityReader, Arrays.asList(1, 2, 3));
        assertEquals(Arrays.asList(1, 2, 3), (List<?>) projected);

        List<Integer> defaultArray = Arrays.asList(1, 2, 3);
        List<Integer> array = Arrays.asList(4, 5);

        Schema optionalWriter = SchemaBuilder.array(Schema.INT32_SCHEMA).optional().build();
        Schema reader = SchemaBuilder.array(Schema.INT32_SCHEMA).defaultValue(Arrays.asList(1, 2, 3)).build();

        projected = SchemaProjector.project(optionalWriter, reader, array);
        assertEquals(array, (List<?>) projected);

        projected = SchemaProjector.project(optionalWriter, reader, null);
        assertEquals(defaultArray, (List<?>) projected);

        Schema promotedReader = SchemaBuilder.array(Schema.INT64_SCHEMA).defaultValue(Arrays.asList(1L, 2L, 3L)).build();
        projected = SchemaProjector.project(optionalWriter, promotedReader, array);
        assertEquals(array, (List<?>) projected);
        projected = SchemaProjector.project(optionalWriter, promotedReader, null);
        assertEquals(Arrays.asList(1L, 2L, 3L), (List<?>) projected);


        Schema noDefaultValueReader = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
        try {
            SchemaProjector.project(optionalWriter, noDefaultValueReader, array);
            fail("Reader does not provide a default value.");
        } catch (SchemaProjectorException e) {
            // expected
        }

        Schema nonPromotableReader = SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build();
        try {
            SchemaProjector.project(optionalWriter, nonPromotableReader, array);
            fail("Neither writer type matches reader type nor writer type can be promoted to reader type");
        } catch (SchemaProjectorException e) {
            // expected
        }
    }

    @Test
    public void testMapProjection() throws Exception {
        Schema writer = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).optional().build();

        Schema reader = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).defaultValue(Collections.singletonMap(1, 2)).build();
        Object projected = SchemaProjector.project(writer, reader, Collections.singletonMap(3, 4));
        assertEquals(Collections.singletonMap(3, 4), (Map<?, ?>) projected);
        projected = SchemaProjector.project(writer, reader, null);
        assertEquals(Collections.singletonMap(1, 2), (Map<?, ?>) projected);

        Schema promotedReader = SchemaBuilder.map(Schema.INT64_SCHEMA, Schema.FLOAT32_SCHEMA).defaultValue(
                Collections.singletonMap(3L, 4.5F)).build();
        projected = SchemaProjector.project(writer, promotedReader, Collections.singletonMap(3, 4));
        assertEquals(Collections.singletonMap(3, 4), (Map<?, ?>) projected);
        projected = SchemaProjector.project(writer, promotedReader, null);
        assertEquals(Collections.singletonMap(3L, 4.5F), (Map<?, ?>) projected);

        Schema noDefaultValueReader = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build();
        try {
            SchemaProjector.project(writer, noDefaultValueReader, null);
            fail("Reader does not provide a default value.");
        } catch (SchemaProjectorException e) {
            // expected
        }

        Schema nonPromotableReader = SchemaBuilder.map(Schema.BOOLEAN_SCHEMA, Schema.STRING_SCHEMA).build();
        try {
            SchemaProjector.project(writer, nonPromotableReader, null);
            fail("Neither writer type matches reader type nor writer type can be promoted to reader type");
        } catch (SchemaProjectorException e) {
            // expected
        }
    }

    private void verifyOptionalProjection(Schema writer, Type readerType, Object value, Object defaultValue, boolean optional) {
        Schema reader = null;
        assert writer.isOptional();
        switch (readerType) {
            case INT8:
                if (optional) {
                    reader = SchemaBuilder.int8().optional().defaultValue(defaultValue).build();
                } else {
                    reader = SchemaBuilder.int8().defaultValue(defaultValue).build();
                }
                break;
            case INT16:
                if (optional) {
                    reader = SchemaBuilder.int16().optional().defaultValue(defaultValue).build();
                } else {
                    reader = SchemaBuilder.int16().defaultValue(defaultValue).build();
                }
                break;
            case INT32:
                if (optional) {
                    reader = SchemaBuilder.int32().optional().defaultValue(defaultValue).build();
                } else {
                    reader = SchemaBuilder.int32().defaultValue(defaultValue).build();
                }
                break;
            case INT64:
                if (optional) {
                    reader = SchemaBuilder.int64().optional().defaultValue(defaultValue).build();
                } else {
                    reader = SchemaBuilder.int64().defaultValue(defaultValue).build();
                }
                break;
            case FLOAT32:
                if (optional) {
                    reader = SchemaBuilder.float32().optional().defaultValue(defaultValue).build();
                } else {
                    reader = SchemaBuilder.float32().defaultValue(defaultValue).build();
                }
                break;
            case FLOAT64:
                if (optional) {
                    reader = SchemaBuilder.float64().optional().defaultValue(defaultValue).build();
                } else {
                    reader = SchemaBuilder.float64().defaultValue(defaultValue).build();
                }
                break;
            case BOOLEAN:
                if (optional) {
                    reader = SchemaBuilder.bool().optional().defaultValue(defaultValue).build();
                } else {
                    reader = SchemaBuilder.bool().defaultValue(defaultValue).build();
                }
                break;
            case BYTES:
                if (optional) {
                    reader = SchemaBuilder.bytes().optional().defaultValue(defaultValue).build();
                } else {
                    reader = SchemaBuilder.bytes().defaultValue(defaultValue).build();
                }
                break;
            case STRING:
                if (optional) {
                    reader = SchemaBuilder.string().optional().defaultValue(defaultValue).build();
                } else {
                    reader = SchemaBuilder.string().defaultValue(defaultValue).build();
                }
                break;
            default:
                break;
        }
        Object projected = SchemaProjector.project(writer, reader, value);
        assertEquals(value, projected);
        projected = SchemaProjector.project(writer, reader, null);
        if (optional) {
            assertEquals(null, projected);
        } else {
            assertEquals(defaultValue, projected);
        }
    }
}
