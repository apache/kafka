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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MessageDataGeneratorTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testNullDefaults() throws Exception {
        MessageSpec testMessageSpec = MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"FooBar\",",
                "  \"validVersions\": \"0-2\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"int32\", \"versions\": \"0+\" },",
                "    { \"name\": \"field2\", \"type\": \"[]TestStruct\", \"versions\": \"1+\", ",
                "    \"nullableVersions\": \"1+\", \"default\": \"null\", \"fields\": [",
                "      { \"name\": \"field1\", \"type\": \"int32\", \"versions\": \"0+\" }",
                "    ]},",
                "    { \"name\": \"field3\", \"type\": \"bytes\", \"versions\": \"2+\", ",
                "      \"nullableVersions\": \"2+\", \"default\": \"null\" }",
                "  ]",
                "}")), MessageSpec.class);
        new MessageDataGenerator("org.apache.kafka.common.message").generate(testMessageSpec);
    }

    @Test
    public void testInvalidNullDefaultForInt() throws Exception {
        MessageSpec testMessageSpec = MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
            "{",
            "  \"type\": \"request\",",
            "  \"name\": \"FooBar\",",
            "  \"validVersions\": \"0-2\",",
            "  \"fields\": [",
            "    { \"name\": \"field1\", \"type\": \"int32\", \"versions\": \"0+\", \"default\": \"null\" }",
            "  ]",
            "}")), MessageSpec.class);
        try {
            new MessageDataGenerator("org.apache.kafka.common.message").generate(testMessageSpec);
            fail("Expected MessageDataGenerator#generate to fail");
        } catch (Throwable e) {
            assertTrue("Invalid error message: " + e.getMessage(),
                    e.getMessage().contains("Invalid default for int32"));
        }
    }

    @Test
    public void testInvalidNullDefaultForPotentiallyNonNullableArray() throws Exception {
        MessageSpec testMessageSpec = MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"FooBar\",",
                "  \"validVersions\": \"0-2\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"[]int32\", \"versions\": \"0+\", \"nullableVersions\": \"1+\", ",
                "    \"default\": \"null\" }",
                "  ]",
                "}")), MessageSpec.class);
        try {
            new MessageDataGenerator("org.apache.kafka.common.message").generate(testMessageSpec);
            fail("Expected MessageDataGenerator#generate to fail");
        } catch (RuntimeException e) {
            assertTrue("Invalid error message: " + e.getMessage(),
                    e.getMessage().contains("not all versions of this field are nullable"));
        }
    }

    @Test
    public void testInvalidFieldName() {
        try {
            MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"FooBar\",",
                "  \"validVersions\": \"0-2\",",
                "  \"fields\": [",
                "    { \"name\": \"_badName\", \"type\": \"[]int32\", \"versions\": \"0+\" }",
                "  ]",
                "}")), MessageSpec.class);
            fail("Expected MessageDataGenerator constructor to fail");
        } catch (Exception e) {
            assertTrue("Invalid error message: " + e.getMessage(),
                e.getMessage().contains("Invalid field name"));
        }
    }

    @Test
    public void testInvalidTagWithoutTaggedVersions() {
        try {
            MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"FooBar\",",
                "  \"validVersions\": \"0-2\",",
                "  \"flexibleVersions\": \"0+\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"int32\", \"versions\": \"0+\", \"tag\": 0 }",
                "  ]",
                "}")), MessageSpec.class);
            fail("Expected the MessageSpec constructor to fail");
        } catch (Exception e) {
            assertTrue("Invalid error message: " + e.getMessage(),
                e.getMessage().contains("If a tag is specified, taggedVersions must be specified as well."));
        }
    }

    @Test
    public void testInvalidNegativeTag() {
        try {
            MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"FooBar\",",
                "  \"validVersions\": \"0-2\",",
                "  \"flexibleVersions\": \"0+\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"int32\", \"versions\": \"0+\", ",
                "        \"tag\": -1, \"taggedVersions\": \"0+\" }",
                "  ]",
                "}")), MessageSpec.class);
            fail("Expected the MessageSpec constructor to fail");
        } catch (Exception e) {
            assertTrue("Invalid error message: " + e.getMessage(),
                e.getMessage().contains("Tags cannot be negative"));
        }
    }

    @Test
    public void testInvalidFlexibleVersionsRange() {
        try {
            MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"FooBar\",",
                "  \"validVersions\": \"0-2\",",
                "  \"flexibleVersions\": \"0-2\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"int32\", \"versions\": \"0+\" }",
                "  ]",
                "}")), MessageSpec.class);
            fail("Expected the MessageSpec constructor to fail");
        } catch (Exception e) {
            assertTrue("Invalid error message: " + e.getMessage(),
                e.getMessage().contains("flexibleVersions must be either none, or an open-ended range"));
        }
    }

    @Test
    public void testInvalidSometimesNullableTaggedField() {
        try {
            MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"FooBar\",",
                "  \"validVersions\": \"0-2\",",
                "  \"flexibleVersions\": \"0+\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"string\", \"versions\": \"0+\", ",
                "        \"tag\": 0, \"taggedVersions\": \"0+\", \"nullableVersions\": \"1+\" }",
                "  ]",
                "}")), MessageSpec.class);
            fail("Expected the MessageSpec constructor to fail");
        } catch (Exception e) {
            assertTrue("Invalid error message: " + e.getMessage(),
                e.getMessage().contains("Either all tagged versions must be nullable, or none must be"));
        }
    }

    @Test
    public void testInvalidTaggedVersionsNotASubetOfVersions() {
        try {
            MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"FooBar\",",
                "  \"validVersions\": \"0-2\",",
                "  \"flexibleVersions\": \"0+\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"string\", \"versions\": \"0-2\", ",
                "        \"tag\": 0, \"taggedVersions\": \"1+\" }",
                "  ]",
                "}")), MessageSpec.class);
            fail("Expected the MessageSpec constructor to fail");
        } catch (Exception e) {
            assertTrue("Invalid error message: " + e.getMessage(),
                e.getMessage().contains("taggedVersions must be a subset of versions"));
        }
    }

    @Test
    public void testInvalidTaggedVersionsWithoutTag() {
        try {
            MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"FooBar\",",
                "  \"validVersions\": \"0-2\",",
                "  \"flexibleVersions\": \"0+\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"string\", \"versions\": \"0+\", ",
                "        \"taggedVersions\": \"1+\" }",
                "  ]",
                "}")), MessageSpec.class);
            fail("Expected the MessageSpec constructor to fail");
        } catch (Exception e) {
            assertTrue("Invalid error message: " + e.getMessage(),
                e.getMessage().contains("Please specify a tag, or remove the taggedVersions"));
        }
    }

    @Test
    public void testInvalidTaggedVersionsRange() {
        try {
            MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"FooBar\",",
                "  \"validVersions\": \"0-2\",",
                "  \"flexibleVersions\": \"0+\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"string\", \"versions\": \"0+\", ",
                "        \"tag\": 0, \"taggedVersions\": \"1-2\" }",
                "  ]",
                "}")), MessageSpec.class);
            fail("Expected the MessageSpec constructor to fail");
        } catch (Exception e) {
            assertTrue("Invalid error message: " + e.getMessage(),
                e.getMessage().contains("taggedVersions must be either none, " +
                    "or an open-ended range"));
        }
    }

    @Test
    public void testDuplicateTags() {
        try {
            MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"type\": \"request\",",
                "  \"name\": \"FooBar\",",
                "  \"validVersions\": \"0-2\",",
                "  \"flexibleVersions\": \"0+\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"string\", \"versions\": \"0+\", ",
                "        \"tag\": 0, \"taggedVersions\": \"0+\" },",
                "    { \"name\": \"field2\", \"type\": \"int64\", \"versions\": \"0+\", ",
                "        \"tag\": 0, \"taggedVersions\": \"0+\" }",
                "  ]",
                "}")), MessageSpec.class);
            fail("Expected the MessageSpec constructor to fail");
        } catch (Exception e) {
            assertTrue("Invalid error message: " + e.getMessage(),
                e.getMessage().contains("duplicate tag"));
        }
    }
}
