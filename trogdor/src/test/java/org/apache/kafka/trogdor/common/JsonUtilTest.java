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

package org.apache.kafka.trogdor.common;

import org.apache.kafka.test.TestUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 120)
public class JsonUtilTest {

    @Test
    public void testOpenBraceComesFirst() {
        assertTrue(JsonUtil.openBraceComesFirst("{}"));
        assertTrue(JsonUtil.openBraceComesFirst(" \t{\"foo\":\"bar\"}"));
        assertTrue(JsonUtil.openBraceComesFirst(" { \"foo\": \"bar\" }"));
        assertFalse(JsonUtil.openBraceComesFirst("/my/file/path"));
        assertFalse(JsonUtil.openBraceComesFirst("mypath"));
        assertFalse(JsonUtil.openBraceComesFirst(" blah{}"));
    }

    record Foo(@JsonProperty int bar) {
        @JsonCreator
        Foo(@JsonProperty("bar") int bar) {
            this.bar = bar;
        }
    }

    @Test
    public void testObjectFromCommandLineArgument() throws Exception {
        assertEquals(123, JsonUtil.objectFromCommandLineArgument("{\"bar\":123}", Foo.class).bar);
        assertEquals(1, JsonUtil.objectFromCommandLineArgument("   {\"bar\": 1}   ", Foo.class).bar);
        File tempFile = TestUtils.tempFile();
        try {
            Files.writeString(tempFile.toPath(), "{\"bar\": 456}");
            assertEquals(456, JsonUtil.objectFromCommandLineArgument(tempFile.getAbsolutePath(), Foo.class).bar);
        } finally {
            Files.delete(tempFile.toPath());
        }
    }
}
