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

@Timeout(120)
public class IsNullConditionalTest {

    @Test
    public void testNullCheck() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        IsNullConditional.
            forName("foobar").
            nullableVersions(Versions.parse("2+", null)).
            possibleVersions(Versions.parse("0+", null)).
            ifNull(() -> {
                buffer.printf("System.out.println(\"null\");%n");
            }).
            generate(buffer);
        VersionConditionalTest.claimEquals(buffer,
            "if (foobar == null) {%n",
            "    System.out.println(\"null\");%n",
            "}%n");
    }

    @Test
    public void testAnotherNullCheck() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        IsNullConditional.
            forName("foobar").
            nullableVersions(Versions.parse("0+", null)).
            possibleVersions(Versions.parse("2+", null)).
            ifNull(() -> {
                buffer.printf("System.out.println(\"null\");%n");
            }).
            ifShouldNotBeNull(() -> {
                buffer.printf("System.out.println(\"not null\");%n");
            }).
            generate(buffer);
        VersionConditionalTest.claimEquals(buffer,
            "if (foobar == null) {%n",
            "    System.out.println(\"null\");%n",
            "} else {%n",
            "    System.out.println(\"not null\");%n",
            "}%n");
    }

    @Test
    public void testNotNullCheck() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        IsNullConditional.
            forName("foobar").
            nullableVersions(Versions.parse("0+", null)).
            possibleVersions(Versions.parse("2+", null)).
            ifShouldNotBeNull(() -> {
                buffer.printf("System.out.println(\"not null\");%n");
            }).
            generate(buffer);
        VersionConditionalTest.claimEquals(buffer,
            "if (foobar != null) {%n",
            "    System.out.println(\"not null\");%n",
            "}%n");
    }

    @Test
    public void testNeverNull() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        IsNullConditional.
            forName("baz").
            nullableVersions(Versions.parse("0-2", null)).
            possibleVersions(Versions.parse("3+", null)).
            ifNull(() -> {
                buffer.printf("System.out.println(\"null\");%n");
            }).
            ifShouldNotBeNull(() -> {
                buffer.printf("System.out.println(\"not null\");%n");
            }).
            generate(buffer);
        VersionConditionalTest.claimEquals(buffer,
            "System.out.println(\"not null\");%n");
    }

    @Test
    public void testNeverNullWithBlockScope() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        IsNullConditional.
            forName("baz").
            nullableVersions(Versions.parse("0-2", null)).
            possibleVersions(Versions.parse("3+", null)).
            ifNull(() -> {
                buffer.printf("System.out.println(\"null\");%n");
            }).
            ifShouldNotBeNull(() -> {
                buffer.printf("System.out.println(\"not null\");%n");
            }).
            alwaysEmitBlockScope(true).
            generate(buffer);
        VersionConditionalTest.claimEquals(buffer,
            "{%n",
            "    System.out.println(\"not null\");%n",
            "}%n");
    }
}
