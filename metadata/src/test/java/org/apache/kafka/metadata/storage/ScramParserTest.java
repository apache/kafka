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

package org.apache.kafka.metadata.storage;

import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.metadata.storage.ScramParser.PerMechanismData;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.AbstractMap;
import java.util.Optional;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(value = 40)
public class ScramParserTest {
    static final byte[] TEST_SALT = new byte[] {
        49, 108, 118, 52, 112, 100, 110, 119, 52, 102, 119, 113,
        55, 110, 111, 116, 99, 120, 109, 48, 121, 121, 49, 107, 55, 113
    };

    static final byte[] TEST_SALTED_PASSWORD = new byte[] {
        -103, 61, 50, -55, 69, 49, -98, 82, 90, 11, -33, 71, 94,
        4, 83, 73, -119, 91, -70, -90, -72, 21, 33, -83, 36,
        34, 95, 76, -53, -29, 96, 33
    };

    @Test
    public void testSplitTrimmedConfigStringComponentOnNameEqualsFoo() {
        assertEquals(new AbstractMap.SimpleImmutableEntry<>("name", "foo"),
            ScramParser.splitTrimmedConfigStringComponent("name=foo"));
    }

    @Test
    public void testSplitTrimmedConfigStringComponentOnNameEqualsQuotedFoo() {
        assertEquals(new AbstractMap.SimpleImmutableEntry<>("name", "foo"),
            ScramParser.splitTrimmedConfigStringComponent("name=\"foo\""));
    }

    @Test
    public void testSplitTrimmedConfigStringComponentOnNameEqualsEmpty() {
        assertEquals(new AbstractMap.SimpleImmutableEntry<>("name", ""),
            ScramParser.splitTrimmedConfigStringComponent("name="));
    }

    @Test
    public void testSplitTrimmedConfigStringComponentOnNameEqualsQuotedEmpty() {
        assertEquals(new AbstractMap.SimpleImmutableEntry<>("name", ""),
            ScramParser.splitTrimmedConfigStringComponent("name=\"\""));
    }

    @Test
    public void testSplitTrimmedConfigStringComponentWithNoEquals() {
        assertEquals("No equals sign found in SCRAM component: name",
            assertThrows(FormatterException.class,
                () -> ScramParser.splitTrimmedConfigStringComponent("name")).getMessage());
    }

    @Test
    public void testRandomSalt() throws Exception {
        PerMechanismData data = new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
            "bob",
            Optional.empty(),
            OptionalInt.empty(),
            Optional.of("my pass"),
            Optional.empty());
        TestUtils.retryOnExceptionWithTimeout(10_000, () ->
            assertNotEquals(data.salt().toString(), data.salt().toString())
        );
    }

    @Test
    public void testConfiguredSalt() throws Exception {
        assertArrayEquals(TEST_SALT, new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
            "bob",
            Optional.of(TEST_SALT),
            OptionalInt.empty(),
            Optional.of("my pass"),
            Optional.empty()).salt());
    }

    @Test
    public void testDefaultIterations() {
        assertEquals(4096, new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
            "bob",
            Optional.empty(),
            OptionalInt.empty(),
            Optional.of("my pass"),
            Optional.empty()).iterations());
    }

    @Test
    public void testConfiguredIterations() {
        assertEquals(8192, new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
            "bob",
            Optional.empty(),
            OptionalInt.of(8192),
            Optional.of("my pass"),
            Optional.empty()).iterations());
    }
    @Test
    public void testParsePerMechanismArgument() {
        assertEquals(new AbstractMap.SimpleImmutableEntry<>(
            ScramMechanism.SCRAM_SHA_512, "name=scram-admin,password=scram-user-secret"),
                ScramParser.parsePerMechanismArgument(
                    "SCRAM-SHA-512=[name=scram-admin,password=scram-user-secret]"));
    }

    @Test
    public void testParsePerMechanismArgumentWithoutEqualsSign() {
        assertEquals("Failed to find equals sign in SCRAM argument 'SCRAM-SHA-512'",
            assertThrows(FormatterException.class,
                () -> ScramParser.parsePerMechanismArgument(
                    "SCRAM-SHA-512")).getMessage());
    }

    @Test
    public void testParsePerMechanismArgumentWithUnsupportedScramMethod() {
        assertEquals("The add-scram mechanism SCRAM-SHA-UNSUPPORTED is not supported.",
            assertThrows(FormatterException.class,
                () -> ScramParser.parsePerMechanismArgument(
                    "SCRAM-SHA-UNSUPPORTED=[name=scram-admin,password=scram-user-secret]")).
                        getMessage());
    }

    @Test
    public void testParsePerMechanismArgumentWithConfigStringWithoutBraces() {
        assertEquals("Expected configuration string to start with [",
            assertThrows(FormatterException.class,
                () -> ScramParser.parsePerMechanismArgument(
                    "SCRAM-SHA-256=name=scram-admin,password=scram-user-secret")).getMessage());
    }

    @Test
    public void testParsePerMechanismArgumentWithConfigStringWithoutEndBrace() {
        assertEquals("Expected configuration string to end with ]",
            assertThrows(FormatterException.class,
                () -> ScramParser.parsePerMechanismArgument(
                    "SCRAM-SHA-256=[name=scram-admin,password=scram-user-secret")).getMessage());
    }

    @Test
    public void testParsePerMechanismData() {
        assertEquals(new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
            "bob",
            Optional.empty(),
            OptionalInt.empty(),
            Optional.of("mypass"),
            Optional.empty()),
                new PerMechanismData(ScramMechanism.SCRAM_SHA_256, "name=bob,password=mypass"));
    }

    @Test
    public void testParsePerMechanismDataFailsWithoutName() {
        assertEquals("You must supply 'name' to add-scram",
            assertThrows(FormatterException.class,
                () -> new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
                    "password=mypass")).
                        getMessage());
    }

    @Test
    public void testParsePerMechanismDataFailsWithoutPassword() {
        assertEquals("You must supply one of 'password' or 'saltedpassword' to add-scram",
            assertThrows(FormatterException.class,
                () -> new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
                    "name=bar")).
                        getMessage());
    }

    @Test
    public void testParsePerMechanismDataFailsWithExtraArguments() {
        assertEquals("Unknown SCRAM configurations: unknown, unknown2",
            assertThrows(FormatterException.class,
                () -> new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
                    "name=bob,password=mypass,unknown=something,unknown2=somethingelse")).
                        getMessage());
    }

    @Test
    public void testParsePerMechanismDataWithIterations() {
        assertEquals(new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
            "bob",
            Optional.empty(),
            OptionalInt.of(8192),
            Optional.of("my pass"),
            Optional.empty()),
                new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
                    "name=bob,password=my pass,iterations=8192"));
    }

    @Test
    public void testParsePerMechanismDataWithConfiguredSalt() {
        assertEquals(new PerMechanismData(ScramMechanism.SCRAM_SHA_512,
            "bob",
            Optional.of(TEST_SALT),
            OptionalInt.empty(),
            Optional.of("my pass"),
            Optional.empty()),
                new PerMechanismData(ScramMechanism.SCRAM_SHA_512,
                    "name=bob,password=my pass,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\""));
    }

    @Test
    public void testParsePerMechanismDataWithIterationsAndConfiguredSalt() {
        assertEquals(new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
            "bob",
            Optional.of(TEST_SALT),
            OptionalInt.of(8192),
            Optional.of("my pass"),
            Optional.empty()),
                new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
                    "name=bob,password=my pass,iterations=8192,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\""));
    }

    @Test
    public void testParsePerMechanismDataWithConfiguredSaltedPasswordFailsWithoutSalt() {
        assertEquals("You must supply 'salt' with 'saltedpassword' to add-scram",
            assertThrows(FormatterException.class,
                () -> new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
                    "name=alice,saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\"")).
                        getMessage());
    }

    @Test
    public void testParsePerMechanismDataWithConfiguredSaltedPassword() {
        assertEquals(new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
            "alice",
            Optional.of(TEST_SALT),
            OptionalInt.empty(),
            Optional.empty(),
            Optional.of(TEST_SALTED_PASSWORD)),
                new PerMechanismData(ScramMechanism.SCRAM_SHA_256,
                    "name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\"," +
                        "saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\""));
    }

    @Test
    public void testPerMechanismDataToRecord() throws Exception {
        ScramFormatter formatter = new ScramFormatter(ScramMechanism.SCRAM_SHA_512);
        assertEquals(new UserScramCredentialRecord().
            setName("alice").
            setMechanism(ScramMechanism.SCRAM_SHA_512.type()).
            setSalt(TEST_SALT).
            setStoredKey(formatter.storedKey(formatter.clientKey(TEST_SALTED_PASSWORD))).
            setServerKey(formatter.serverKey(TEST_SALTED_PASSWORD)).
            setIterations(4096),
                new PerMechanismData(ScramMechanism.SCRAM_SHA_512,
                    "alice",
                    Optional.of(TEST_SALT),
                    OptionalInt.empty(),
                    Optional.empty(),
                    Optional.of(TEST_SALTED_PASSWORD)).toRecord());
    }
}
