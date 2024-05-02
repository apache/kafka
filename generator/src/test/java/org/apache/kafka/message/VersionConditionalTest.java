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

import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(120)
public class VersionConditionalTest {

    static void claimEquals(CodeBuffer buffer, String... lines) throws Exception {
        StringWriter stringWriter = new StringWriter();
        buffer.write(stringWriter);
        StringBuilder expectedStringBuilder = new StringBuilder();
        for (String line : lines) {
            expectedStringBuilder.append(String.format(line));
        }
        assertEquals(stringWriter.toString(), expectedStringBuilder.toString());
    }

    @Test
    public void testAlwaysFalseConditional() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        VersionConditional.
            forVersions(Versions.parse("1-2", null), Versions.parse("3+", null)).
            ifMember(__ -> {
                buffer.printf("System.out.println(\"hello world\");%n");
            }).
            ifNotMember(__ -> {
                buffer.printf("System.out.println(\"foobar\");%n");
            }).
            generate(buffer);
        claimEquals(buffer,
            "System.out.println(\"foobar\");%n");
    }

    @Test
    public void testAnotherAlwaysFalseConditional() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        VersionConditional.
            forVersions(Versions.parse("3+", null), Versions.parse("1-2", null)).
            ifMember(__ -> {
                buffer.printf("System.out.println(\"hello world\");%n");
            }).
            ifNotMember(__ -> {
                buffer.printf("System.out.println(\"foobar\");%n");
            }).
            generate(buffer);
        claimEquals(buffer,
            "System.out.println(\"foobar\");%n");
    }

    @Test
    public void testAllowMembershipCheckAlwaysFalseFails() {
        try {
            CodeBuffer buffer = new CodeBuffer();
            VersionConditional.
                forVersions(Versions.parse("1-2", null), Versions.parse("3+", null)).
                ifMember(__ -> {
                    buffer.printf("System.out.println(\"hello world\");%n");
                }).
                ifNotMember(__ -> {
                    buffer.printf("System.out.println(\"foobar\");%n");
                }).
                allowMembershipCheckAlwaysFalse(false).
                generate(buffer);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("no versions in common"));
        }
    }

    @Test
    public void testAlwaysTrueConditional() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        VersionConditional.
            forVersions(Versions.parse("1-5", null), Versions.parse("2-4", null)).
            ifMember(__ -> {
                buffer.printf("System.out.println(\"hello world\");%n");
            }).
            ifNotMember(__ -> {
                buffer.printf("System.out.println(\"foobar\");%n");
            }).
            allowMembershipCheckAlwaysFalse(false).
            generate(buffer);
        claimEquals(buffer,
            "System.out.println(\"hello world\");%n");
    }

    @Test
    public void testAlwaysTrueConditionalWithAlwaysEmitBlockScope() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        VersionConditional.
            forVersions(Versions.parse("1-5", null), Versions.parse("2-4", null)).
            ifMember(__ -> {
                buffer.printf("System.out.println(\"hello world\");%n");
            }).
            ifNotMember(__ -> {
                buffer.printf("System.out.println(\"foobar\");%n");
            }).
            alwaysEmitBlockScope(true).
            generate(buffer);
        claimEquals(buffer,
            "{%n",
            "    System.out.println(\"hello world\");%n",
            "}%n");
    }

    @Test
    public void testLowerRangeCheckWithElse() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        VersionConditional.
            forVersions(Versions.parse("1+", null), Versions.parse("0-100", null)).
            ifMember(__ -> {
                buffer.printf("System.out.println(\"hello world\");%n");
            }).
            ifNotMember(__ -> {
                buffer.printf("System.out.println(\"foobar\");%n");
            }).
            generate(buffer);
        claimEquals(buffer,
            "if (_version >= 1) {%n",
            "    System.out.println(\"hello world\");%n",
            "} else {%n",
            "    System.out.println(\"foobar\");%n",
            "}%n");
    }

    @Test
    public void testLowerRangeCheckWithIfMember() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        VersionConditional.
            forVersions(Versions.parse("1+", null), Versions.parse("0-100", null)).
            ifMember(__ -> {
                buffer.printf("System.out.println(\"hello world\");%n");
            }).
            generate(buffer);
        claimEquals(buffer,
            "if (_version >= 1) {%n",
            "    System.out.println(\"hello world\");%n",
            "}%n");
    }

    @Test
    public void testLowerRangeCheckWithIfNotMember() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        VersionConditional.
            forVersions(Versions.parse("1+", null), Versions.parse("0-100", null)).
            ifNotMember(__ -> {
                buffer.printf("System.out.println(\"hello world\");%n");
            }).
            generate(buffer);
        claimEquals(buffer,
            "if (_version < 1) {%n",
            "    System.out.println(\"hello world\");%n",
            "}%n");
    }

    @Test
    public void testUpperRangeCheckWithElse() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        VersionConditional.
            forVersions(Versions.parse("0-10", null), Versions.parse("4+", null)).
            ifMember(__ -> {
                buffer.printf("System.out.println(\"hello world\");%n");
            }).
            ifNotMember(__ -> {
                buffer.printf("System.out.println(\"foobar\");%n");
            }).
            generate(buffer);
        claimEquals(buffer,
            "if (_version <= 10) {%n",
            "    System.out.println(\"hello world\");%n",
            "} else {%n",
            "    System.out.println(\"foobar\");%n",
            "}%n");
    }

    @Test
    public void testUpperRangeCheckWithIfMember() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        VersionConditional.
            forVersions(Versions.parse("0-10", null), Versions.parse("4+", null)).
            ifMember(__ -> {
                buffer.printf("System.out.println(\"hello world\");%n");
            }).
            generate(buffer);
        claimEquals(buffer,
            "if (_version <= 10) {%n",
            "    System.out.println(\"hello world\");%n",
            "}%n");
    }

    @Test
    public void testUpperRangeCheckWithIfNotMember() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        VersionConditional.
            forVersions(Versions.parse("1+", null), Versions.parse("0-100", null)).
            ifNotMember(__ -> {
                buffer.printf("System.out.println(\"hello world\");%n");
            }).
            generate(buffer);
        claimEquals(buffer,
            "if (_version < 1) {%n",
            "    System.out.println(\"hello world\");%n",
            "}%n");
    }

    @Test
    public void testFullRangeCheck() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        VersionConditional.
            forVersions(Versions.parse("5-10", null), Versions.parse("1+", null)).
            ifMember(__ -> {
                buffer.printf("System.out.println(\"hello world\");%n");
            }).
            allowMembershipCheckAlwaysFalse(false).
            generate(buffer);
        claimEquals(buffer,
            "if ((_version >= 5) && (_version <= 10)) {%n",
            "    System.out.println(\"hello world\");%n",
            "}%n");
    }
}
