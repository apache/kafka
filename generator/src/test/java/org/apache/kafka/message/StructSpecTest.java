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

import com.fasterxml.jackson.databind.exc.ValueInstantiationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(120)
public class StructSpecTest {
    @Test
    public void testNamesMustBeUnique() {
        assertEquals("In LeaderAndIsrRequest, field field1 has a duplicate name field1. All field names must be unique.",
            assertThrows(ValueInstantiationException.class,
                () -> MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                    "{",
                    "  \"type\": \"request\",",
                    "  \"name\": \"LeaderAndIsrRequest\",",
                    "  \"validVersions\": \"0-4\",",
                    "  \"deprecatedVersions\": \"0-1\",",
                    "  \"flexibleVersions\": \"0+\",",
                    "  \"fields\": [",
                    "    { \"name\": \"field1\", \"type\": \"int32\", \"versions\": \"0+\" },",
                    "    { \"name\": \"field1\", \"type\": \"[]int64\", \"versions\": \"1+\" }",
                    "  ]",
                    "}")), MessageSpec.class)).getCause().getMessage());
    }

    @Test
    public void testTagsMustBeUnique() {
        assertEquals("In LeaderAndIsrRequest, field field2 has a duplicate tag ID 0. All tags IDs must be unique.",
            assertThrows(ValueInstantiationException.class,
                () -> MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                    "{",
                    "  \"type\": \"request\",",
                    "  \"name\": \"LeaderAndIsrRequest\",",
                    "  \"validVersions\": \"0-4\",",
                    "  \"deprecatedVersions\": \"0-1\",",
                    "  \"flexibleVersions\": \"0+\",",
                    "  \"fields\": [",
                    "    { \"name\": \"field1\", \"type\": \"int32\", \"versions\": \"0+\", ",
                    "        \"taggedVersions\": \"0+\", \"tag\": 0},",
                    "    { \"name\": \"field2\", \"type\": \"[]int64\", \"versions\": \"0+\", ",
                    "        \"taggedVersions\": \"0+\", \"tag\": 0 }",
                    "  ]",
                    "}")), MessageSpec.class)).getCause().getMessage());
    }
}
