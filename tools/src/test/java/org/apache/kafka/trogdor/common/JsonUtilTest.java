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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JsonUtilTest {
    private static final Logger log = LoggerFactory.getLogger(JsonUtilTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testOpenBraceComesFirst() {
        assertTrue(JsonUtil.openBraceComesFirst("{}"));
        assertTrue(JsonUtil.openBraceComesFirst(" \t{\"foo\":\"bar\"}"));
        assertTrue(JsonUtil.openBraceComesFirst(" { \"foo\": \"bar\" }"));
        assertFalse(JsonUtil.openBraceComesFirst("/my/file/path"));
        assertFalse(JsonUtil.openBraceComesFirst("mypath"));
        assertFalse(JsonUtil.openBraceComesFirst(" blah{}"));
    }

    static final class Foo {
        @JsonProperty
        final int bar;

        @JsonCreator
        Foo(@JsonProperty("bar") int bar) {
            this.bar = bar;
        }
    }

    @Test
    public void testObjectFromCommandLineArgument() throws Exception {
        assertEquals(123, JsonUtil.<Foo>
            objectFromCommandLineArgument("{\"bar\":123}", Foo.class).bar);
        assertEquals(1, JsonUtil.<Foo>
            objectFromCommandLineArgument("   {\"bar\": 1}   ", Foo.class).bar);
        File tempFile = TestUtils.tempFile();
        try {
            Files.write(tempFile.toPath(),
                "{\"bar\": 456}".getBytes(StandardCharsets.UTF_8));
            assertEquals(456, JsonUtil.<Foo>
                objectFromCommandLineArgument(tempFile.getAbsolutePath(), Foo.class).bar);
        } finally {
            Files.delete(tempFile.toPath());
        }
    }
};
