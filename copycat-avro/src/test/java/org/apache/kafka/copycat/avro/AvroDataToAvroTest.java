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

import org.apache.kafka.copycat.data.GenericRecord;
import org.apache.kafka.copycat.data.GenericRecordBuilder;
import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

// Tests AvroData's conversion of Copycat -> Avro
public class AvroDataToAvroTest {

    // All the primitive types are pass-through

    @Test
    public void testNull() {
        assertNull(AvroData.convertToAvro(null));
    }

    @Test
    public void testBoolean() {
        Assert.assertEquals(true, AvroData.convertToAvro(true));
    }

    @Test
    public void testInteger() {
        Assert.assertEquals(12, AvroData.convertToAvro(12));
    }

    @Test
    public void testLong() {
        Assert.assertEquals(12L, AvroData.convertToAvro(12L));
    }

    @Test
    public void testFloat() {
        Assert.assertEquals(12.2f, AvroData.convertToAvro(12.2f));
    }

    @Test
    public void testDouble() {
        Assert.assertEquals(12.2, AvroData.convertToAvro(12.2));
    }

    @Test
    public void testBytes() {
        Object converted = AvroData.convertToAvro("foo".getBytes());
        assertTrue(converted instanceof byte[]);
        assertEquals(ByteBuffer.wrap("foo".getBytes()), ByteBuffer.wrap((byte[]) converted));

        Assert.assertEquals(ByteBuffer.wrap("foo".getBytes()),
                AvroData.convertToAvro(ByteBuffer.wrap("foo".getBytes())));
    }

    @Test
    public void testString() {
        Assert.assertEquals("string", AvroData.convertToAvro("string"));
    }

    @Test
    public void testComplex() {
        Schema schema = SchemaBuilder.record("record").fields()
                .name("null").type().nullType().noDefault()
                .requiredBoolean("boolean")
                .requiredInt("int")
                .requiredLong("long")
                .requiredFloat("float")
                .requiredDouble("double")
                .requiredBytes("bytes")
                .requiredString("string")
                .name("union").type().unionOf().nullType().and().intType().endUnion().noDefault()
                .name("array").type().array().items().intType().noDefault()
                .name("map").type().map().values().intType().noDefault()
                .name("fixed").type().fixed("fixed").size(3).noDefault()
                .name("enum").type().enumeration("enum").symbols("one", "two").noDefault()
                .endRecord();
        GenericRecord record = new GenericRecordBuilder(schema)
                .set("null", null)
                .set("boolean", true)
                .set("int", 12)
                .set("long", 12L)
                .set("float", 12.2f)
                .set("double", 12.2)
                .set("bytes", ByteBuffer.wrap("foo".getBytes()))
                .set("string", "string-value")
                .set("union", 12)
                .set("array", Arrays.asList(1, 2, 3))
                .set("map", Collections.singletonMap("field", 1))
                .set("fixed", ByteBuffer.wrap("foo".getBytes()))
                .set("enum", "one")
                .build();

        Object convertedRecord = AvroData.convertToAvro(record);

        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("record").fields()
                .name("null").type().nullType().noDefault()
                .requiredBoolean("boolean")
                .requiredInt("int")
                .requiredLong("long")
                .requiredFloat("float")
                .requiredDouble("double")
                .requiredBytes("bytes")
                .requiredString("string")
                .name("union").type().unionOf().nullType().and().intType().endUnion().noDefault()
                .name("array").type().array().items().intType().noDefault()
                .name("map").type().map().values().intType().noDefault()
                .name("fixed").type().fixed("fixed").size(3).noDefault()
                .name("enum").type().enumeration("enum").symbols("one", "two").noDefault()
                .endRecord();
        org.apache.avro.generic.GenericRecord avroRecord
                = new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
                .set("null", null)
                .set("boolean", true)
                .set("int", 12)
                .set("long", 12L)
                .set("float", 12.2f)
                .set("double", 12.2)
                .set("bytes", ByteBuffer.wrap("foo".getBytes()))
                .set("string", "string-value")
                .set("union", 12)
                .set("array", Arrays.asList(1, 2, 3))
                .set("map", Collections.singletonMap("field", 1))
                .set("fixed", ByteBuffer.wrap("foo".getBytes()))
                .set("enum", "one")
                .build();

        assertEquals(avroSchema, ((org.apache.avro.generic.GenericRecord) convertedRecord).getSchema());
        assertEquals(avroRecord, convertedRecord);
    }

}
