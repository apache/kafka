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

package org.apache.kafka.message.checker;

import org.apache.kafka.message.MessageSpec;
import org.apache.kafka.message.MessageSpecType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;

import static org.apache.kafka.message.checker.CheckerTestUtils.field;
import static org.apache.kafka.message.checker.CheckerTestUtils.fieldWithDefaults;
import static org.apache.kafka.message.checker.CheckerTestUtils.fieldWithNulls;
import static org.apache.kafka.message.checker.CheckerTestUtils.toMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(40)
public class UnifierTest {
    @Test
    public void testAddNewField() throws Exception {
        new Unifier(toMessage("{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0', 'flexibleVersions': '0+', " +
                "'fields': [" +
                "{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}" +
                "]}"),
            toMessage("{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-1', 'flexibleVersions': '0+', " +
                "'fields': [" +
                "{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                "{'name': 'ControlerId', 'type': 'int32', 'versions': '1+'}" +
                "]}")).unify();
    }

    static final MessageSpec TOP_LEVEL_MESSAGE_1 = new MessageSpec("TopLevelMessage",
            "0-2", null, null, null, MessageSpecType.DATA, Collections.emptyList(),
            "0+", Collections.emptyList(), false);

    static final MessageSpec TOP_LEVEL_MESSAGE_2 = new MessageSpec("TopLevelMessage",
            "0-4", null, null, null, MessageSpecType.DATA, Collections.emptyList(),
            "0+", Collections.emptyList(), false);

    static final MessageSpec TOP_LEVEL_MESSAGE_2_DROPPING_V0 = new MessageSpec("TopLevelMessage",
            "1-4", null, null, null, MessageSpecType.DATA, Collections.emptyList(),
            "0+", Collections.emptyList(), false);

    @Test
    public void testFieldTypesDoNotMatch() throws Exception {
        assertEquals("Field type for field2 foo is int8, but field type for field1 foo is int16",
            assertThrows(UnificationException.class,
                () -> new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
                    field("foo", "0+", "int16"),
                    field("foo", "0+", "int8"))).getMessage());
    }

    @Test
    public void testFieldTypesMatch() throws Exception {
        new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
            field("foo", "0+", "int16"),
            field("foo", "0+", "int16"));
    }

    @Test
    public void testArrayElementTypesDoNotMatch() throws Exception {
        assertEquals("Field type for field2 foo is []int8, but field type for field1 foo is []int16",
            assertThrows(UnificationException.class,
                () -> new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
                    field("foo", "0+", "[]int16"),
                    field("foo", "0+", "[]int8"))).getMessage());
    }

    @Test
    public void testArrayFieldTypesMatch() throws Exception {
        new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
            field("foo", "0+", "[]int16"),
            field("foo", "0+", "[]int16"));
    }

    @Test
    public void testMaximumValidVersionForField2IsLowerThanField1() throws Exception {
        assertEquals("Maximum effective valid version for field2 foo, '1' cannot be lower than the " +
                "maximum effective valid version for field1 foo, '2'",
            assertThrows(UnificationException.class,
                () -> new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
                    field("foo", "0-2", "int64"),
                    field("foo", "0-1", "int64"))).getMessage());
    }

    @Test
    public void testMaximumValidVersionForIsReasonable1() throws Exception {
        new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
            field("foo", "0-1", "int64"),
            field("foo", "0-2", "int64"));
    }

    @Test
    public void testMaximumValidVersionForIsReasonable2() throws Exception {
        new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
            field("foo", "0+", "int64"), // effective max is 2, because TOP_LEVEL_MESSAGE_1 supports 0-2
            field("foo", "0-3", "int64"));
    }

    @Test
    public void testMinimumValidVersionForField2IsLowerThanField1() throws Exception {
        assertEquals("Minimum effective valid version for field2 foo, '0' cannot be different than the " +
                "minimum effective valid version for field1 foo, '1'",
            assertThrows(UnificationException.class,
                () -> new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
                    field("foo", "1+", "int64"),
                    field("foo", "0+", "int64"))).getMessage());
    }

    @Test
    public void testMinimumValidVersionForField2IsLowerThanField1Again() throws Exception {
        assertEquals("Minimum effective valid version for field2 foo, '0' cannot be different than the " +
                "minimum effective valid version for field1 foo, '1'",
            assertThrows(UnificationException.class,
                () -> new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
                    field("foo", "1-2", "int64"),
                    field("foo", "0-2", "int64"))).getMessage());
    }

    @Test
    public void testMinimumValidVersionForField2IsHigherThanField1() throws Exception {
        assertEquals("Minimum effective valid version for field2 foo, '1' cannot be different than the " +
                "minimum effective valid version for field1 foo, '0'",
            assertThrows(UnificationException.class,
                () -> new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
                    field("foo", "0-2", "int64"),
                    field("foo", "1-2", "int64"))).getMessage());
    }

    @Test
    public void testNullableVersionsCheckPasses1() throws Exception {
        new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2_DROPPING_V0).unify(
            fieldWithNulls("foo", "0-2", "string", "0+"),
            fieldWithNulls("foo", "1-2", "string", "1-2"));
    }

    @Test
    public void testNullableVersionsCheckPasses2() throws Exception {
        new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
                fieldWithNulls("foo", "0+", "string", "0+"), // effectively 0-2 because of max valid version
                fieldWithNulls("foo", "0+", "string", "0-2"));
    }

    @Test
    public void testNullableVersionsCheckFails1() throws Exception {
        assertEquals("Minimum effective nullable version for field2 foo, '1' cannot be different than " +
                "the minimum effective nullable version for field1 foo, '0'",
            assertThrows(UnificationException.class,
                () -> new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
                    fieldWithNulls("foo", "0-2", "string", "0-2"),
                    fieldWithNulls("foo", "0-2", "string", "1-2"))).getMessage());
    }

    @Test
    public void testNullableVersionsCheckFails2() throws Exception {
        assertEquals("Minimum effective nullable version for field2 foo, '1' cannot be different than " +
                "the minimum effective nullable version for field1 foo, '0'",
            assertThrows(UnificationException.class,
                () -> new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
                    fieldWithNulls("foo", "0+", "string", "0+"),
                    fieldWithNulls("foo", "0+", "string", "1-2"))).getMessage());
    }

    @Test
    public void testFlexibleVersionsChangedCausesFailure1() throws Exception {
        assertEquals("Flexible versions for field2 foo is 2+, but flexible versions for field1 is 1+",
            assertThrows(UnificationException.class,
                () -> new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
                    fieldWithDefaults("foo", "0+", null, "1+"),
                    fieldWithDefaults("foo", "0+", null, "2+"))).getMessage());
    }

    @Test
    public void testFlexibleVersionsChangedCausesFailure2() throws Exception {
        assertEquals("Flexible versions for field2 foo is 2+, but flexible versions for field1 is none",
            assertThrows(UnificationException.class,
                () -> new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
                    fieldWithDefaults("foo", "0+", null, ""),
                    fieldWithDefaults("foo", "0+", null, "2+"))).getMessage());
    }

    @Test
    public void testFlexibleVersionsCheckPasses() throws Exception {
        new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
            fieldWithDefaults("foo", "0+", null, "1+"),
            fieldWithDefaults("foo", "0+", null, "1+"));
    }

    @Test
    public void testDefaultsChangedCausesFailure() throws Exception {
        assertEquals("Default for field2 foo is 'newDefault', but default for field1 foo is 'oldDefault'",
            assertThrows(UnificationException.class,
                () -> new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
                    fieldWithDefaults("foo", "0+", "oldDefault", null),
                    fieldWithDefaults("foo", "0+", "newDefault", null))).getMessage());
    }

    @Test
    public void testDefaultsCheckPasses() throws Exception {
        new Unifier(TOP_LEVEL_MESSAGE_1, TOP_LEVEL_MESSAGE_2).unify(
            fieldWithDefaults("foo", "0+", "oldDefault", null),
            fieldWithDefaults("foo", "0+", "oldDefault", null));
    }
}
