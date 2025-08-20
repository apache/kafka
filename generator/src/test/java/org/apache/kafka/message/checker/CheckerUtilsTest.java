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

import org.apache.kafka.message.Versions;

import org.junit.jupiter.api.Test;

import static org.apache.kafka.message.checker.CheckerTestUtils.field;
import static org.apache.kafka.message.checker.CheckerTestUtils.fieldWithTag;
import static org.apache.kafka.message.checker.CheckerTestUtils.messageSpecStringToTempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CheckerUtilsTest {
    @Test
    public void testMin0and1() {
        assertEquals((short) 0, CheckerUtils.min((short) 0, (short) 1));
    }

    @Test
    public void testMin1and0() {
        assertEquals((short) 0, CheckerUtils.min((short) 1, (short) 0));
    }

    @Test
    public void testMin5and5() {
        assertEquals((short) 5, CheckerUtils.min((short) 5, (short) 5));
    }

    @Test
    public void testMax0and1() {
        assertEquals((short) 1, CheckerUtils.max((short) 0, (short) 1));
    }

    @Test
    public void testMax1and0() {
        assertEquals((short) 1, CheckerUtils.max((short) 1, (short) 0));
    }

    @Test
    public void testMax5and5() {
        assertEquals((short) 5, CheckerUtils.max((short) 5, (short) 5));
    }

    @Test
    public void testValidateTaggedVersionsOnNontaggedField() {
        CheckerUtils.validateTaggedVersions("field1",
            field("foo", "0+", "int64"),
            Versions.parse("5+", Versions.NONE));
    }

    @Test
    public void testValidateTaggedVersionsOnTaggedField() {
        CheckerUtils.validateTaggedVersions("field1",
            fieldWithTag("foo", 123, "1+", "1+"),
            Versions.parse("1+", Versions.NONE));
    }

    @Test
    public void testValidateTaggedVersionsOnTaggedFieldWithError() {
        assertThrows(RuntimeException.class,
            () -> CheckerUtils.validateTaggedVersions("field1",
                fieldWithTag("foo", 123, "1+", "1+"),
                Versions.parse("2+", Versions.NONE)));
    }

    @Test
    public void testReadMessageSpecFromFile() throws Exception {
        CheckerUtils.readMessageSpecFromFile(messageSpecStringToTempFile(
            "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
            "'validVersions': '0-2', 'flexibleVersions': '0+', " +
            "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}"));
    }
}
