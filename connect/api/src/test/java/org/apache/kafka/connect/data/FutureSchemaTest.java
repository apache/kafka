/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.data;


import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class FutureSchemaTest {

    @Test
    public void testEquality() {
        Schema s1 = new FutureSchema("name", true, null);
        Schema s2 = new FutureSchema("name", true, null);
        Schema otherName = new FutureSchema("otherName", true, null);
        Schema otherOptional = new FutureSchema("name", false, null);
        Schema otherDefault = new FutureSchema("name", false, "defaultName");

        assertEquals(s1, s2);
        assertNotEquals(s1, otherName);
        assertNotEquals(s1, otherOptional);
        assertNotEquals(s1, otherDefault);
    }

    @Test
    public void testResolution() {
        Schema futureS1 = new FutureSchema("s1", false, null);
        Schema futureS1Opt = new FutureSchema("s1", true, null);
        Schema futureS1Default = new FutureSchema("s1", false, "default");
        Schema futureS2Opt = new FutureSchema("s2", true, null);

        Schema s1 = new ConnectSchema(Schema.Type.STRUCT, false, null, "s1", null, null);
        Schema s1Opt = new ConnectSchema(Schema.Type.STRUCT, true, null, "s1", null, null);
        Schema s1Default = new ConnectSchema(Schema.Type.STRUCT, false, "default", "s1", null, null);
        Schema s2Opt = new ConnectSchema(Schema.Type.STRUCT, true, null, "s2", null, null);

        futureS1.resolve(Arrays.asList(s1));
        futureS1Opt.resolve(Arrays.asList(s1Opt));
        futureS1Default.resolve(Arrays.asList(s1Default));
        futureS2Opt.resolve(Arrays.asList(s2Opt));

        assertEquals(futureS1, s1);
        assertEquals(futureS1Opt, s1Opt);
        assertEquals(futureS1Default, s1Default);
        assertEquals(futureS2Opt, s2Opt);
    }

    @Test(expected = DataException.class)
    public void testResolutionFailed() {
        Schema futureS1 = new FutureSchema("s1", false, null);
        Schema futureS1Opt = new FutureSchema("s1", true, null);
        Schema futureS2Opt = new FutureSchema("s2", true, null);

        Schema s1 = new ConnectSchema(Schema.Type.STRUCT, false, null, "s1", null, null);
        Schema s1Opt = new ConnectSchema(Schema.Type.STRUCT, true, null, "s1", null, null);
        Schema s2Opt = new ConnectSchema(Schema.Type.STRUCT, true, null, "s2", null, null);

        futureS1.resolve(Arrays.asList(s2Opt));
        futureS1Opt.resolve(Arrays.asList(s2Opt));
        futureS2Opt.resolve(Arrays.asList(s1, s1Opt));

        assertNotEquals(futureS1, s1);
        assertNotEquals(futureS1Opt, s1Opt);
        assertNotEquals(futureS2Opt, s2Opt);

        futureS1.type();
    }

    @Test
    public void testCyclicEquality() {
        ConnectSchema s1 = new ConnectSchema(Schema.Type.STRUCT, true, null, "node", null, null, null,
                Arrays.asList(new Field("value", 0, SchemaBuilder.int8().build()),
                        new Field("next", 1, new FutureSchema("node", true, null))), null, null);

        ConnectSchema s2 = new ConnectSchema(Schema.Type.STRUCT, true, null, "node", null, null, null,
                Arrays.asList(new Field("value", 0, SchemaBuilder.int8().build()),
                        new Field("next", 1, new FutureSchema("node", true, null))), null, null);

        // Double nesting is equivalent, just inefficient.
        ConnectSchema s3 = new ConnectSchema(Schema.Type.STRUCT, true, null, "node", null, null, null,
                Arrays.asList(new Field("value", 0, SchemaBuilder.int8().build()),
                        new Field("next", 1, s2)), null, null);

        assertEquals(s1, s2);
        assertEquals(s1, s3);
    }

    @Test
    public void testCyclicInequality() {
        ConnectSchema s1 = new ConnectSchema(Schema.Type.STRUCT, true, null, "node", null, null, null,
                Arrays.asList(new Field("value", 0, SchemaBuilder.int8().build()),
                        new Field("next", 1, new FutureSchema("node", true, null))), null, null);

        ConnectSchema differentField = new ConnectSchema(Schema.Type.STRUCT, true, null, "node", null, null, null,
                Arrays.asList(new Field("different", 0, SchemaBuilder.int8().build()),
                        new Field("next", 1, new FutureSchema("node", true, null))), null, null);

        ConnectSchema differentNode = new ConnectSchema(Schema.Type.STRUCT, true, null, "node", null, null, null,
                Arrays.asList(new Field("value", 0, SchemaBuilder.int8().build()),
                        new Field("next", 1, new FutureSchema("different", true, null))), null, null);

        ConnectSchema differentStructure = new ConnectSchema(Schema.Type.STRUCT, true, null, "different", null, null, null,
                Arrays.asList(new Field("value", 0, SchemaBuilder.int8().build()),
                        new Field("next", 1, differentNode)), null, null);


        assertNotEquals(s1, differentField);
        assertNotEquals(s1, differentStructure);
    }

    @Test
    public void testNameAccessUnresolvedSchema() {
        assertEquals(new FutureSchema("test", true, null).name(), "test");
    }

    @Test
    public void testIsOptionalAccessUnresolvedSchema() {
        assert new FutureSchema("test", true, null).isOptional();
        assert !(new FutureSchema("test", false, null).isOptional());
    }

    @Test
    public void testDefaultValueAccessUnresolvedSchema() {
        assertEquals(new FutureSchema("test", true, "default").defaultValue(), "default");
    }

    @Test(expected = DataException.class)
    public void testTypeAccessUnresolvedSchema() {
        new FutureSchema("test", true, null).type();
    }

    @Test(expected = DataException.class)
    public void testVersionAccessUnresolvedSchema() {
        new FutureSchema("test", true, null).version();
    }

    @Test(expected = DataException.class)
    public void testDocAccessUnresolvedSchema() {
        new FutureSchema("test", true, null).doc();
    }

    @Test(expected = DataException.class)
    public void testParametersAccessUnresolvedSchema() {
        new FutureSchema("test", true, null).parameters();
    }

    @Test(expected = DataException.class)
    public void testKeySchemaAccessUnresolvedSchema() {
        new FutureSchema("test", true, null).keySchema();
    }

    @Test(expected = DataException.class)
    public void testValueSchemaAccessUnresolvedSchema() {
        new FutureSchema("test", true, null).valueSchema();
    }

    @Test(expected = DataException.class)
    public void testFieldsAccessUnresolvedSchema() {
        new FutureSchema("test", true, null).fields();
    }

    @Test(expected = DataException.class)
    public void testFieldAccessUnresolvedSchema() {
        new FutureSchema("test", true, null).field("field");
    }
}
