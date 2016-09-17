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

package org.apache.kafka.connect.data;

import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SchemaProjectorTest {

    @Test
    public void testPrimitiveTypeProjection() throws Exception {
        Object projected;
        projected = SchemaProjector.project(Schema.BOOLEAN_SCHEMA, false, Schema.BOOLEAN_SCHEMA);
        assertEquals(false, projected);

        byte[] bytes = {(byte) 1, (byte) 2};
        projected  = SchemaProjector.project(Schema.BYTES_SCHEMA, bytes, Schema.BYTES_SCHEMA);
        assertEquals(bytes, projected);

        projected = SchemaProjector.project(Schema.STRING_SCHEMA, "abc", Schema.STRING_SCHEMA);
        assertEquals("abc", projected);

        projected = SchemaProjector.project(Schema.BOOLEAN_SCHEMA, false, Schema.OPTIONAL_BOOLEAN_SCHEMA);
        assertEquals(false, projected);

        projected  = SchemaProjector.project(Schema.BYTES_SCHEMA, bytes, Schema.OPTIONAL_BYTES_SCHEMA);
        assertEquals(bytes, projected);

        projected = SchemaProjector.project(Schema.STRING_SCHEMA, "abc", Schema.OPTIONAL_STRING_SCHEMA);
        assertEquals("abc", projected);

        try {
            SchemaProjector.project(Schema.OPTIONAL_BOOLEAN_SCHEMA, false, Schema.BOOLEAN_SCHEMA);
            fail("Cannot project optional schema to schema with no default value.");
        } catch (DataException e) {
            // expected
        }

        try {
            SchemaProjector.project(Schema.OPTIONAL_BYTES_SCHEMA, bytes, Schema.BYTES_SCHEMA);
            fail("Cannot project optional schema to schema with no default value.");
        } catch (DataException e) {
            // expected
        }

        try {
            SchemaProjector.project(Schema.OPTIONAL_STRING_SCHEMA, "abc", Schema.STRING_SCHEMA);
            fail("Cannot project optional schema to schema with no default value.");
        } catch (DataException e) {
            // expected
        }
    }

    @Test
    public void testNumericTypeProjection() throws Exception {
        Schema[] promotableSchemas = {Schema.INT8_SCHEMA, Schema.INT16_SCHEMA, Schema.INT32_SCHEMA, Schema.INT64_SCHEMA, Schema.FLOAT32_SCHEMA, Schema.FLOAT64_SCHEMA};
        Schema[] promotableOptionalSchemas = {Schema.OPTIONAL_INT8_SCHEMA, Schema.OPTIONAL_INT16_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA,
                                              Schema.OPTIONAL_FLOAT32_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA};

        Object[] values = {(byte) 127, (short) 255, 32767, 327890L, 1.2F, 1.2345};
        Map<Object, List<?>> expectedProjected = new HashMap<>();
        expectedProjected.put(values[0], Arrays.asList((byte) 127, (short) 127, 127, 127L, 127.F, 127.));
        expectedProjected.put(values[1], Arrays.asList((short) 255, 255, 255L, 255.F, 255.));
        expectedProjected.put(values[2], Arrays.asList(32767, 32767L, 32767.F, 32767.));
        expectedProjected.put(values[3], Arrays.asList(327890L, 327890.F, 327890.));
        expectedProjected.put(values[4], Arrays.asList(1.2F, 1.2));
        expectedProjected.put(values[5], Arrays.asList(1.2345));

        Object promoted;
        for (int i = 0; i < promotableSchemas.length; ++i) {
            Schema source = promotableSchemas[i];
            List<?> expected = expectedProjected.get(values[i]);
            for (int j = i; j < promotableSchemas.length; ++j) {
                Schema target = promotableSchemas[j];
                promoted = SchemaProjector.project(source, values[i], target);
                if (target.type() == Type.FLOAT64) {
                    assertEquals((Double) (expected.get(j - i)), (double) promoted, 1e-6);
                } else {
                    assertEquals(expected.get(j - i), promoted);
                }
            }
            for (int j = i; j < promotableOptionalSchemas.length;  ++j) {
                Schema target = promotableOptionalSchemas[j];
                promoted = SchemaProjector.project(source, values[i], target);
                if (target.type() == Type.FLOAT64) {
                    assertEquals((Double) (expected.get(j - i)), (double) promoted, 1e-6);
                } else {
                    assertEquals(expected.get(j - i), promoted);
                }
            }
        }

        for (int i = 0; i < promotableOptionalSchemas.length; ++i) {
            Schema source = promotableSchemas[i];
            List<?> expected = expectedProjected.get(values[i]);
            for (int j = i; j < promotableOptionalSchemas.length;  ++j) {
                Schema target = promotableOptionalSchemas[j];
                promoted = SchemaProjector.project(source, values[i], target);
                if (target.type() == Type.FLOAT64) {
                    assertEquals((Double) (expected.get(j - i)), (double) promoted, 1e-6);
                } else {
                    assertEquals(expected.get(j - i), promoted);
                }
            }
        }

        Schema[] nonPromotableSchemas = {Schema.BOOLEAN_SCHEMA, Schema.BYTES_SCHEMA, Schema.STRING_SCHEMA};
        for (Schema promotableSchema: promotableSchemas) {
            for (Schema nonPromotableSchema: nonPromotableSchemas) {
                Object dummy = new Object();
                try {
                    SchemaProjector.project(promotableSchema, dummy, nonPromotableSchema);
                    fail("Cannot promote " +  promotableSchema.type() + " to " + nonPromotableSchema.type());
                } catch (DataException e) {
                    // expected
                }
            }
        }
    }

    @Test
    public void testPrimitiveOptionalProjection() throws Exception {
        verifyOptionalProjection(Schema.OPTIONAL_BOOLEAN_SCHEMA, Type.BOOLEAN, false, true, false, true);
        verifyOptionalProjection(Schema.OPTIONAL_BOOLEAN_SCHEMA, Type.BOOLEAN, false, true, false, false);

        byte[] bytes = {(byte) 1, (byte) 2};
        byte[] defaultBytes = {(byte) 3, (byte) 4};
        verifyOptionalProjection(Schema.OPTIONAL_BYTES_SCHEMA, Type.BYTES, bytes, defaultBytes, bytes, true);
        verifyOptionalProjection(Schema.OPTIONAL_BYTES_SCHEMA, Type.BYTES, bytes, defaultBytes, bytes, false);

        verifyOptionalProjection(Schema.OPTIONAL_STRING_SCHEMA, Type.STRING, "abc", "def", "abc", true);
        verifyOptionalProjection(Schema.OPTIONAL_STRING_SCHEMA, Type.STRING, "abc", "def", "abc", false);

        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT8, (byte) 12, (byte) 127, (byte) 12, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT8, (byte) 12, (byte) 127, (byte) 12, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT16, (byte) 12, (short) 127, (short) 12, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT16, (byte) 12, (short) 127, (short) 12, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT32, (byte) 12, 12789, 12, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT32, (byte) 12, 12789, 12, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT64, (byte) 12, 127890L, 12L, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.INT64, (byte) 12, 127890L, 12L, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.FLOAT32, (byte) 12, 3.45F, 12.F, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.FLOAT32, (byte) 12, 3.45F, 12.F, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.FLOAT64, (byte) 12, 3.4567, 12., true);
        verifyOptionalProjection(Schema.OPTIONAL_INT8_SCHEMA, Type.FLOAT64, (byte) 12, 3.4567, 12., false);

        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.INT16, (short) 12, (short) 127, (short) 12, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.INT16, (short) 12, (short) 127, (short) 12, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.INT32, (short) 12, 12789, 12, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.INT32, (short) 12, 12789, 12, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.INT64, (short) 12, 127890L, 12L, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.INT64, (short) 12, 127890L, 12L, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.FLOAT32, (short) 12, 3.45F, 12.F, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.FLOAT32, (short) 12, 3.45F, 12.F, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.FLOAT64, (short) 12, 3.4567, 12., true);
        verifyOptionalProjection(Schema.OPTIONAL_INT16_SCHEMA, Type.FLOAT64, (short) 12, 3.4567, 12., false);

        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.INT32, 12, 12789, 12, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.INT32, 12, 12789, 12, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.INT64, 12, 127890L, 12L, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.INT64, 12, 127890L, 12L, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.FLOAT32, 12, 3.45F, 12.F, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.FLOAT32, 12, 3.45F, 12.F, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.FLOAT64, 12, 3.4567, 12., true);
        verifyOptionalProjection(Schema.OPTIONAL_INT32_SCHEMA, Type.FLOAT64, 12, 3.4567, 12., false);

        verifyOptionalProjection(Schema.OPTIONAL_INT64_SCHEMA, Type.INT64, 12L, 127890L, 12L, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT64_SCHEMA, Type.INT64, 12L, 127890L, 12L, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT64_SCHEMA, Type.FLOAT32, 12L, 3.45F, 12.F, true);
        verifyOptionalProjection(Schema.OPTIONAL_INT64_SCHEMA, Type.FLOAT32, 12L, 3.45F, 12.F, false);
        verifyOptionalProjection(Schema.OPTIONAL_INT64_SCHEMA, Type.FLOAT64, 12L, 3.4567, 12., true);
        verifyOptionalProjection(Schema.OPTIONAL_INT64_SCHEMA, Type.FLOAT64, 12L, 3.4567, 12., false);

        verifyOptionalProjection(Schema.OPTIONAL_FLOAT32_SCHEMA, Type.FLOAT32, 12.345F, 3.45F, 12.345F, true);
        verifyOptionalProjection(Schema.OPTIONAL_FLOAT32_SCHEMA, Type.FLOAT32, 12.345F, 3.45F, 12.345F, false);
        verifyOptionalProjection(Schema.OPTIONAL_FLOAT32_SCHEMA, Type.FLOAT64, 12.345F, 3.4567, 12.345, true);
        verifyOptionalProjection(Schema.OPTIONAL_FLOAT32_SCHEMA, Type.FLOAT64, 12.345F, 3.4567, 12.345, false);

        verifyOptionalProjection(Schema.OPTIONAL_FLOAT32_SCHEMA, Type.FLOAT64, 12.345, 3.4567, 12.345, true);
        verifyOptionalProjection(Schema.OPTIONAL_FLOAT32_SCHEMA, Type.FLOAT64, 12.345, 3.4567, 12.345, false);
    }

    @Test
    public void testStructAddField() throws Exception {
        Schema source = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .build();
        Struct sourceStruct = new Struct(source);
        sourceStruct.put("field", 1);

        Schema target = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .field("field2", SchemaBuilder.int32().defaultValue(123).build())
                .build();

        Struct targetStruct = (Struct) SchemaProjector.project(source, sourceStruct, target);


        assertEquals(1, (int) targetStruct.getInt32("field"));
        assertEquals(123, (int) targetStruct.getInt32("field2"));

        Schema incompatibleTargetSchema = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .build();

        try {
            SchemaProjector.project(source, sourceStruct, incompatibleTargetSchema);
            fail("Incompatible schema.");
        } catch (DataException e) {
            // expected
        }
    }

    @Test
    public void testStructRemoveField() throws Exception {
        Schema source = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .build();
        Struct sourceStruct = new Struct(source);
        sourceStruct.put("field", 1);
        sourceStruct.put("field2", 234);

        Schema target = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .build();
        Struct targetStruct = (Struct) SchemaProjector.project(source, sourceStruct, target);

        assertEquals(1, targetStruct.get("field"));
        try {
            targetStruct.get("field2");
            fail("field2 is not part of the projected struct");
        } catch (DataException e) {
            // expected
        }
    }

    @Test
    public void testStructDefaultValue() throws Exception {
        Schema source = SchemaBuilder.struct().optional()
                .field("field", Schema.INT32_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .build();

        SchemaBuilder builder = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA);

        Struct defaultStruct = new Struct(builder).put("field", 12).put("field2", 345);
        builder.defaultValue(defaultStruct);
        Schema target = builder.build();

        Object projected = SchemaProjector.project(source, null, target);
        assertEquals(defaultStruct, projected);

        Struct sourceStruct = new Struct(source).put("field", 45).put("field2", 678);
        Struct targetStruct = (Struct) SchemaProjector.project(source, sourceStruct, target);

        assertEquals(sourceStruct.get("field"), targetStruct.get("field"));
        assertEquals(sourceStruct.get("field2"), targetStruct.get("field2"));
    }

    @Test
    public void testNestedSchemaProjection() throws Exception {
        Schema sourceFlatSchema = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .build();
        Schema targetFlatSchema = SchemaBuilder.struct()
                .field("field", Schema.INT32_SCHEMA)
                .field("field2", SchemaBuilder.int32().defaultValue(123).build())
                .build();
        Schema sourceNestedSchema = SchemaBuilder.struct()
                .field("first", Schema.INT32_SCHEMA)
                .field("second", Schema.STRING_SCHEMA)
                .field("array", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
                .field("map", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
                .field("nested", sourceFlatSchema)
                .build();
        Schema targetNestedSchema = SchemaBuilder.struct()
                .field("first", Schema.INT32_SCHEMA)
                .field("second", Schema.STRING_SCHEMA)
                .field("array", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
                .field("map", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
                .field("nested", targetFlatSchema)
                .build();

        Struct sourceFlatStruct = new Struct(sourceFlatSchema);
        sourceFlatStruct.put("field", 113);

        Struct sourceNestedStruct = new Struct(sourceNestedSchema);
        sourceNestedStruct.put("first", 1);
        sourceNestedStruct.put("second", "abc");
        sourceNestedStruct.put("array", Arrays.asList(1, 2));
        sourceNestedStruct.put("map", Collections.singletonMap(5, "def"));
        sourceNestedStruct.put("nested", sourceFlatStruct);

        Struct targetNestedStruct = (Struct) SchemaProjector.project(sourceNestedSchema, sourceNestedStruct,
                                                                     targetNestedSchema);
        assertEquals(1, targetNestedStruct.get("first"));
        assertEquals("abc", targetNestedStruct.get("second"));
        assertEquals(Arrays.asList(1, 2), (List<Integer>) targetNestedStruct.get("array"));
        assertEquals(Collections.singletonMap(5, "def"), (Map<Integer, String>) targetNestedStruct.get("map"));

        Struct projectedStruct = (Struct) targetNestedStruct.get("nested");
        assertEquals(113, projectedStruct.get("field"));
        assertEquals(123, projectedStruct.get("field2"));
    }

    @Test
    public void testLogicalTypeProjection() throws Exception {
        Schema[] logicalTypeSchemas = {Decimal.schema(2), Date.SCHEMA, Time.SCHEMA, Timestamp.SCHEMA};
        Object projected;

        BigDecimal testDecimal = new BigDecimal(new BigInteger("156"), 2);
        projected = SchemaProjector.project(Decimal.schema(2), testDecimal, Decimal.schema(2));
        assertEquals(testDecimal, projected);

        projected = SchemaProjector.project(Date.SCHEMA, 1000, Date.SCHEMA);
        assertEquals(1000, projected);

        projected = SchemaProjector.project(Time.SCHEMA, 231, Time.SCHEMA);
        assertEquals(231, projected);

        projected = SchemaProjector.project(Timestamp.SCHEMA, 34567L, Timestamp.SCHEMA);
        assertEquals(34567L, projected);

        Schema namedSchema = SchemaBuilder.int32().name("invalidLogicalTypeName").build();
        for (Schema logicalTypeSchema: logicalTypeSchemas) {
            try {
                SchemaProjector.project(logicalTypeSchema, null, Schema.BOOLEAN_SCHEMA);
                fail("Cannot project logical types to non-logical types.");
            } catch (SchemaProjectorException e) {
                // expected
            }

            try {
                SchemaProjector.project(logicalTypeSchema, null, namedSchema);
                fail("Reader name is not a valid logical type name.");
            } catch (SchemaProjectorException e) {
                // expected
            }

            try {
                SchemaProjector.project(Schema.BOOLEAN_SCHEMA, null, logicalTypeSchema);
                fail("Cannot project non-logical types to logical types.");
            } catch (SchemaProjectorException e) {
                // expected
            }
        }
    }

    @Test
    public void testArrayProjection() throws Exception {
        Schema source = SchemaBuilder.array(Schema.INT32_SCHEMA).build();

        Object projected = SchemaProjector.project(source, Arrays.asList(1, 2, 3), source);
        assertEquals(Arrays.asList(1, 2, 3), (List<Integer>) projected);

        Schema optionalSource = SchemaBuilder.array(Schema.INT32_SCHEMA).optional().build();
        Schema target = SchemaBuilder.array(Schema.INT32_SCHEMA).defaultValue(Arrays.asList(1, 2, 3)).build();
        projected = SchemaProjector.project(optionalSource, Arrays.asList(4, 5), target);
        assertEquals(Arrays.asList(4, 5), (List<Integer>) projected);
        projected = SchemaProjector.project(optionalSource, null, target);
        assertEquals(Arrays.asList(1, 2, 3), (List<Integer>) projected);

        Schema promotedTarget = SchemaBuilder.array(Schema.INT64_SCHEMA).defaultValue(Arrays.asList(1L, 2L, 3L)).build();
        projected = SchemaProjector.project(optionalSource, Arrays.asList(4, 5), promotedTarget);
        List<Long> expectedProjected = Arrays.asList(4L, 5L);
        assertEquals(expectedProjected, (List<Long>) projected);
        projected = SchemaProjector.project(optionalSource, null, promotedTarget);
        assertEquals(Arrays.asList(1L, 2L, 3L), (List<Long>) projected);

        Schema noDefaultValueTarget = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
        try {
            SchemaProjector.project(optionalSource, null, noDefaultValueTarget);
            fail("Target schema does not provide a default value.");
        } catch (SchemaProjectorException e) {
            // expected
        }

        Schema nonPromotableTarget = SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build();
        try {
            SchemaProjector.project(optionalSource, null, nonPromotableTarget);
            fail("Neither source type matches target type nor source type can be promoted to target type");
        } catch (SchemaProjectorException e) {
            // expected
        }
    }

    @Test
    public void testMapProjection() throws Exception {
        Schema source = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).optional().build();

        Schema target = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).defaultValue(Collections.singletonMap(1, 2)).build();
        Object projected = SchemaProjector.project(source, Collections.singletonMap(3, 4), target);
        assertEquals(Collections.singletonMap(3, 4), (Map<Integer, Integer>) projected);
        projected = SchemaProjector.project(source, null, target);
        assertEquals(Collections.singletonMap(1, 2), (Map<Integer, Integer>) projected);

        Schema promotedTarget = SchemaBuilder.map(Schema.INT64_SCHEMA, Schema.FLOAT32_SCHEMA).defaultValue(
                Collections.singletonMap(3L, 4.5F)).build();
        projected = SchemaProjector.project(source, Collections.singletonMap(3, 4), promotedTarget);
        assertEquals(Collections.singletonMap(3L, 4.F), (Map<Long, Float>) projected);
        projected = SchemaProjector.project(source, null, promotedTarget);
        assertEquals(Collections.singletonMap(3L, 4.5F), (Map<Long, Float>) projected);

        Schema noDefaultValueTarget = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build();
        try {
            SchemaProjector.project(source, null, noDefaultValueTarget);
            fail("Reader does not provide a default value.");
        } catch (SchemaProjectorException e) {
            // expected
        }

        Schema nonPromotableTarget = SchemaBuilder.map(Schema.BOOLEAN_SCHEMA, Schema.STRING_SCHEMA).build();
        try {
            SchemaProjector.project(source, null, nonPromotableTarget);
            fail("Neither source type matches target type nor source type can be promoted to target type");
        } catch (SchemaProjectorException e) {
            // expected
        }
    }

    @Test
    public void testMaybeCompatible() throws Exception {
        Schema source = SchemaBuilder.int32().name("source").build();
        Schema target = SchemaBuilder.int32().name("target").build();

        try {
            SchemaProjector.project(source, 12, target);
            fail("Source name and target name mismatch.");
        } catch (SchemaProjectorException e) {
            // expected
        }

        Schema targetWithParameters = SchemaBuilder.int32().parameters(Collections.singletonMap("key", "value"));
        try {
            SchemaProjector.project(source, 34, targetWithParameters);
            fail("Source parameters and target parameters mismatch.");
        } catch (SchemaProjectorException e) {
            // expected
        }
    }

    @Test
    public void testProjectMissingDefaultValuedStructField() {
        final Schema source = SchemaBuilder.struct().build();
        final Schema target = SchemaBuilder.struct().field("id", SchemaBuilder.int64().defaultValue(42L).build()).build();
        assertEquals(42L, (long) ((Struct) SchemaProjector.project(source, new Struct(source), target)).getInt64("id"));
    }

    @Test
    public void testProjectMissingOptionalStructField() {
        final Schema source = SchemaBuilder.struct().build();
        final Schema target = SchemaBuilder.struct().field("id", SchemaBuilder.OPTIONAL_INT64_SCHEMA).build();
        assertEquals(null, ((Struct) SchemaProjector.project(source, new Struct(source), target)).getInt64("id"));
    }

    @Test(expected = SchemaProjectorException.class)
    public void testProjectMissingRequiredField() {
        final Schema source = SchemaBuilder.struct().build();
        final Schema target = SchemaBuilder.struct().field("id", SchemaBuilder.INT64_SCHEMA).build();
        SchemaProjector.project(source, new Struct(source), target);
    }

    private void verifyOptionalProjection(Schema source, Type targetType, Object value, Object defaultValue, Object expectedProjected, boolean optional) {
        Schema target;
        assert source.isOptional();
        assert value != null;
        if (optional) {
            target = SchemaBuilder.type(targetType).optional().defaultValue(defaultValue).build();
        } else {
            target = SchemaBuilder.type(targetType).defaultValue(defaultValue).build();
        }
        Object projected = SchemaProjector.project(source, value, target);
        if (targetType == Type.FLOAT64) {
            assertEquals((double) expectedProjected, (double) projected, 1e-6);
        } else {
            assertEquals(expectedProjected, projected);
        }

        projected = SchemaProjector.project(source, null, target);
        if (optional) {
            assertEquals(null, projected);
        } else {
            assertEquals(defaultValue, projected);
        }
    }
}
