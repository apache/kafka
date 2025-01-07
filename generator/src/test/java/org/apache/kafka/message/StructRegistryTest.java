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

package org.apache.kafka.message;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(120)
public class StructRegistryTest {

    @Test
    public void testCommonStructs() throws Exception {
        MessageSpec testMessageSpec = MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"LeaderAndIsrRequest\",",
                "  \"validVersions\": \"0-4\",",
                "  \"deprecatedVersions\": \"0-1\",",
                "  \"flexibleVersions\": \"0+\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"int32\", \"versions\": \"0+\" },",
                "    { \"name\": \"field2\", \"type\": \"[]TestCommonStruct\", \"versions\": \"1+\" },",
                "    { \"name\": \"field3\", \"type\": \"[]TestInlineStruct\", \"versions\": \"0+\", ",
                "    \"fields\": [",
                "      { \"name\": \"inlineField1\", \"type\": \"int64\", \"versions\": \"0+\" }",
                "    ]}",
                "  ],",
                "  \"commonStructs\": [",
                "    { \"name\": \"TestCommonStruct\", \"versions\": \"0+\", \"fields\": [",
                "      { \"name\": \"commonField1\", \"type\": \"int64\", \"versions\": \"0+\" }",
                "    ]}",
                "  ]",
                "}")), MessageSpec.class);
        StructRegistry structRegistry = new StructRegistry();
        structRegistry.register(testMessageSpec);
        assertEquals(structRegistry.commonStructNames(), Collections.singleton("TestCommonStruct"));
        assertFalse(structRegistry.isStructArrayWithKeys(testMessageSpec.fields().get(1)));
        assertFalse(structRegistry.isStructArrayWithKeys(testMessageSpec.fields().get(2)));
        assertTrue(structRegistry.commonStructs().hasNext());
        assertEquals(structRegistry.commonStructs().next().name(), "TestCommonStruct");
    }

    @Test
    public void testReSpecifiedCommonStructError() throws Exception {
        MessageSpec testMessageSpec = MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"LeaderAndIsrRequest\",",
                "  \"validVersions\": \"0-2\",",
                "  \"flexibleVersions\": \"0+\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"int32\", \"versions\": \"0+\" },",
                "    { \"name\": \"field2\", \"type\": \"[]TestCommonStruct\", \"versions\": \"0+\", ",
                "    \"fields\": [",
                "      { \"name\": \"inlineField1\", \"type\": \"int64\", \"versions\": \"0+\" }",
                "    ]}",
                "  ],",
                "  \"commonStructs\": [",
                "    { \"name\": \"TestCommonStruct\", \"versions\": \"0+\", \"fields\": [",
                "      { \"name\": \"commonField1\", \"type\": \"int64\", \"versions\": \"0+\" }",
                "    ]}",
                "  ]",
                "}")), MessageSpec.class);
        StructRegistry structRegistry = new StructRegistry();
        try {
            structRegistry.register(testMessageSpec);
            fail("Expected StructRegistry#registry to fail");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Can't re-specify the common struct TestCommonStruct " +
                    "as an inline struct."));
        }
    }

    @Test
    public void testDuplicateCommonStructError() throws Exception {
        MessageSpec testMessageSpec = MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"LeaderAndIsrRequest\",",
                "  \"validVersions\": \"0-2\",",
                "  \"flexibleVersions\": \"0+\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"int32\", \"versions\": \"0+\" }",
                "  ],",
                "  \"commonStructs\": [",
                "    { \"name\": \"TestCommonStruct\", \"versions\": \"0+\", \"fields\": [",
                "      { \"name\": \"commonField1\", \"type\": \"int64\", \"versions\": \"0+\" }",
                "    ]},",
                "    { \"name\": \"TestCommonStruct\", \"versions\": \"0+\", \"fields\": [",
                "      { \"name\": \"commonField1\", \"type\": \"int64\", \"versions\": \"0+\" }",
                "    ]}",
                "  ]",
                "}")), MessageSpec.class);
        StructRegistry structRegistry = new StructRegistry();
        try {
            structRegistry.register(testMessageSpec);
            fail("Expected StructRegistry#registry to fail");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Common struct TestCommonStruct was specified twice."));
        }
    }

    @Test
    public void testSingleStruct() throws Exception {
        MessageSpec testMessageSpec = MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"LeaderAndIsrRequest\",",
                "  \"validVersions\": \"0-2\",",
                "  \"flexibleVersions\": \"0+\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"int32\", \"versions\": \"0+\" },",
                "    { \"name\": \"field2\", \"type\": \"TestInlineStruct\", \"versions\": \"0+\", ",
                "    \"fields\": [",
                "      { \"name\": \"inlineField1\", \"type\": \"int64\", \"versions\": \"0+\" }",
                "    ]}",
                "  ]",
                "}")), MessageSpec.class);
        StructRegistry structRegistry = new StructRegistry();
        structRegistry.register(testMessageSpec);

        FieldSpec field2 = testMessageSpec.fields().get(1);
        assertTrue(field2.type().isStruct());
        assertEquals(field2.type().toString(), "TestInlineStruct");
        assertEquals(field2.name(), "field2");

        assertEquals(structRegistry.findStruct(field2).name(), "TestInlineStruct");
        assertFalse(structRegistry.isStructArrayWithKeys(field2));
    }
}
