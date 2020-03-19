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
import java.util.Collections;

import static org.junit.Assert.assertThrows;
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
        new MessageDataGenerator("org.apache.kafka.common.message", Collections.emptyMap()).generate(testMessageSpec);
    }

    private void assertStringContains(String substring, String value) {
        assertTrue("Expected string to contain '" + substring + "', but it was " + value,
            value.contains(substring));
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
        assertStringContains("Invalid default for int32",
            assertThrows(RuntimeException.class, () -> {
                new MessageDataGenerator("org.apache.kafka.common.message", Collections.emptyMap()).generate(testMessageSpec);
            }).getMessage());
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

        assertStringContains("not all versions of this field are nullable",
            assertThrows(RuntimeException.class, () -> {
                new MessageDataGenerator("org.apache.kafka.common.message", Collections.emptyMap()).generate(testMessageSpec);
            }).getMessage());
    }

    /**
     * Test attempting to create a field with an invalid name.  The name is
     * invalid because it starts with an underscore.
     */
    @Test
    public void testInvalidFieldName() {
        assertStringContains("Invalid field name",
            assertThrows(Throwable.class, () -> {
                MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                    "{",
                    "  \"type\": \"request\",",
                    "  \"name\": \"FooBar\",",
                    "  \"validVersions\": \"0-2\",",
                    "  \"fields\": [",
                    "    { \"name\": \"_badName\", \"type\": \"[]int32\", \"versions\": \"0+\" }",
                    "  ]",
                    "}")), MessageSpec.class);
            }).getMessage());
    }

    @Test
    public void testInvalidTagWithoutTaggedVersions() {
        assertStringContains("If a tag is specified, taggedVersions must be specified as well.",
            assertThrows(Throwable.class, () -> {
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
            }).getMessage());
    }

    @Test
    public void testInvalidNegativeTag() {
        assertStringContains("Tags cannot be negative",
            assertThrows(Throwable.class, () -> {
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
            }).getMessage());
    }

    @Test
    public void testInvalidFlexibleVersionsRange() {
        assertStringContains("flexibleVersions must be either none, or an open-ended range",
            assertThrows(Throwable.class, () -> {
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
            }).getMessage());
    }

    @Test
    public void testInvalidSometimesNullableTaggedField() {
        assertStringContains("Either all tagged versions must be nullable, or none must be",
            assertThrows(Throwable.class, () -> {
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
            }).getMessage());
    }

    @Test
    public void testInvalidTaggedVersionsNotASubetOfVersions() {
        assertStringContains("taggedVersions must be a subset of versions",
            assertThrows(Throwable.class, () -> {
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
            }).getMessage());
    }

    @Test
    public void testInvalidTaggedVersionsWithoutTag() {
        assertStringContains("Please specify a tag, or remove the taggedVersions",
            assertThrows(Throwable.class, () -> {
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
            }).getMessage());
    }

    @Test
    public void testInvalidTaggedVersionsRange() {
        assertStringContains("taggedVersions must be either none, or an open-ended range",
            assertThrows(Throwable.class, () -> {
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
            }).getMessage());
    }

    @Test
    public void testDuplicateTags() {
        assertStringContains("duplicate tag",
            assertThrows(Throwable.class, () -> {
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
            }).getMessage());
    }
}
