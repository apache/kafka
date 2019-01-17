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

package org.apache.kafka.common.message;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicSet;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class MessageTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    /**
     * Test serializing and deserializing some messages.
     */
    @Test
    public void testRoundTrips() throws Exception {
        testMessageRoundTrips(new MetadataRequestData().setTopics(
            Arrays.asList(new MetadataRequestData.MetadataRequestTopic().setName("foo"),
                new MetadataRequestData.MetadataRequestTopic().setName("bar")
            )), (short) 6);
        testMessageRoundTrips(new AddOffsetsToTxnRequestData().
            setTransactionalId("foobar").
            setProducerId(0xbadcafebadcafeL).
            setProducerEpoch((short) 123).
            setGroupId("baaz"), (short) 1);
        testMessageRoundTrips(new AddOffsetsToTxnResponseData().
            setThrottleTimeMs(42).
            setErrorCode((short) 0), (short) 0);
        testMessageRoundTrips(new AddPartitionsToTxnRequestData().
            setTransactionalId("blah").
            setProducerId(0xbadcafebadcafeL).
            setProducerEpoch((short) 30000).
            setTopics(new AddPartitionsToTxnTopicSet(Collections.singletonList(
                new AddPartitionsToTxnTopic().
                    setName("Topic").
                    setPartitions(Collections.singletonList(1))).iterator())));
        testMessageRoundTrips(new CreateTopicsRequestData().
            setTimeoutMs(1000).setTopics(Collections.emptyList()));
        testMessageRoundTrips(new DescribeAclsRequestData().
            setResourceType((byte) 42).
            setResourceNameFilter(null).
            setResourcePatternType((byte) 3).
            setPrincipalFilter("abc").
            setHostFilter(null).
            setOperation((byte) 0).
            setPermissionType((byte) 0), (short) 0);
    }

    private void testMessageRoundTrips(Message message) throws Exception {
        testMessageRoundTrips(message, message.highestSupportedVersion());
    }

    private void testMessageRoundTrips(Message message, short version) throws Exception {
        testStructRoundTrip(message, version);
        testByteBufferRoundTrip(message, version);
    }

    private void testByteBufferRoundTrip(Message message, short version) throws Exception {
        int size = message.size(version);
        ByteBuffer buf = ByteBuffer.allocate(size);
        ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(buf);
        message.write(byteBufferAccessor, version);
        assertEquals(size, buf.position());
        Message message2 = message.getClass().newInstance();
        buf.flip();
        message2.read(byteBufferAccessor, version);
        assertEquals(size, buf.position());
        assertEquals(message, message2);
        assertEquals(message.hashCode(), message2.hashCode());
        assertEquals(message.toString(), message2.toString());
    }

    private void testStructRoundTrip(Message message, short version) throws Exception {
        Struct struct = message.toStruct(version);
        Message message2 = message.getClass().newInstance();
        message2.fromStruct(struct, version);
        assertEquals(message, message2);
        assertEquals(message.hashCode(), message2.hashCode());
        assertEquals(message.toString(), message2.toString());
    }

    /**
     * Verify that the JSON files support the same message versions as the
     * schemas accessible through the ApiKey class.
     */
    @Test
    public void testMessageVersions() throws Exception {
        for (ApiKeys apiKey : ApiKeys.values()) {
            Message message = null;
            try {
                message = ApiMessageFactory.newRequest(apiKey.id);
            } catch (UnsupportedVersionException e) {
                fail("No request message spec found for API " + apiKey);
            }
            assertTrue("Request message spec for " + apiKey + " only " +
                    "supports versions up to " + message.highestSupportedVersion(),
                apiKey.latestVersion() <= message.highestSupportedVersion());
            try {
                message = ApiMessageFactory.newResponse(apiKey.id);
            } catch (UnsupportedVersionException e) {
                fail("No response message spec found for API " + apiKey);
            }
            assertTrue("Response message spec for " + apiKey + " only " +
                    "supports versions up to " + message.highestSupportedVersion(),
                apiKey.latestVersion() <= message.highestSupportedVersion());
        }
    }

    /**
     * Test that the JSON request files match the schemas accessible through the ApiKey class.
     */
    @Test
    public void testRequestSchemas() throws Exception {
        for (ApiKeys apiKey : ApiKeys.values()) {
            Schema[] manualSchemas = apiKey.requestSchemas;
            Schema[] generatedSchemas = ApiMessageFactory.requestSchemas(apiKey.id);
            Assert.assertEquals("Mismatching request SCHEMAS lengths " +
                "for api key " + apiKey, manualSchemas.length, generatedSchemas.length);
            for (int v = 0; v < manualSchemas.length; v++) {
                try {
                    if (generatedSchemas[v] != null) {
                        compareTypes(manualSchemas[v], generatedSchemas[v]);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to compare request schemas " +
                        "for version " + v + " of " + apiKey, e);
                }
            }
        }
    }

    /**
     * Test that the JSON response files match the schemas accessible through the ApiKey class.
     */
    @Test
    public void testResponseSchemas() throws Exception {
        for (ApiKeys apiKey : ApiKeys.values()) {
            Schema[] manualSchemas = apiKey.responseSchemas;
            Schema[] generatedSchemas = ApiMessageFactory.responseSchemas(apiKey.id);
            Assert.assertEquals("Mismatching response SCHEMAS lengths " +
                "for api key " + apiKey, manualSchemas.length, generatedSchemas.length);
            for (int v = 0; v < manualSchemas.length; v++) {
                try {
                    if (generatedSchemas[v] != null) {
                        compareTypes(manualSchemas[v], generatedSchemas[v]);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to compare response schemas " +
                        "for version " + v + " of " + apiKey, e);
                }
            }
        }
    }

    private static class NamedType {
        final String name;
        final Type type;

        NamedType(String name, Type type) {
            this.name = name;
            this.type = type;
        }

        boolean hasSimilarType(NamedType other) {
            if (type.getClass().equals(other.type.getClass())) {
                return true;
            }
            if (type.getClass().equals(Type.RECORDS.getClass())) {
                if (other.type.getClass().equals(Type.NULLABLE_BYTES.getClass())) {
                    return true;
                }
            } else if (type.getClass().equals(Type.NULLABLE_BYTES.getClass())) {
                if (other.type.getClass().equals(Type.RECORDS.getClass())) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            return name + "[" + type + "]";
        }
    }

    private static void compareTypes(Schema schemaA, Schema schemaB) {
        compareTypes(new NamedType("schemaA", schemaA),
                     new NamedType("schemaB", schemaB));
    }

    private static void compareTypes(NamedType typeA, NamedType typeB) {
        List<NamedType> listA = flatten(typeA);
        List<NamedType> listB = flatten(typeB);
        if (listA.size() != listB.size()) {
            throw new RuntimeException("Can't match up structures: typeA has " +
                Utils.join(listA, ", ") + ", but typeB has " +
                Utils.join(listB, ", "));
        }
        for (int i = 0; i < listA.size(); i++) {
            NamedType entryA = listA.get(i);
            NamedType entryB = listB.get(i);
            if (!entryA.hasSimilarType(entryB)) {
                throw new RuntimeException("Type " + entryA + " in schema A " +
                    "does not match type " + entryB + " in schema B.");
            }
            if (entryA.type.isNullable() != entryB.type.isNullable()) {
                throw new RuntimeException(String.format(
                    "Type %s in Schema A is %s, but type %s in " +
                        "Schema B is %s",
                    entryA, entryA.type.isNullable() ? "nullable" : "non-nullable",
                    entryB, entryB.type.isNullable() ? "nullable" : "non-nullable"));
            }
            if (entryA.type instanceof ArrayOf) {
                compareTypes(new NamedType(entryA.name, ((ArrayOf) entryA.type).type()),
                             new NamedType(entryB.name, ((ArrayOf) entryB.type).type()));
            }
        }
    }

    /**
     * We want to remove Schema nodes from the hierarchy before doing
     * our comparison.  The reason is because Schema nodes don't change what
     * is written to the wire.  Schema(STRING, Schema(INT, SHORT)) is equivalent to
     * Schema(STRING, INT, SHORT).  This function translates schema nodes into their
     * component types.
     */
    private static List<NamedType> flatten(NamedType type) {
        if (!(type.type instanceof Schema)) {
            return Collections.singletonList(type);
        }
        Schema schema = (Schema) type.type;
        ArrayList<NamedType> results = new ArrayList<>();
        for (BoundField field : schema.fields()) {
            results.addAll(flatten(new NamedType(field.def.name, field.def.type)));
        }
        return results;
    }

    @Test
    public void testDefaultValues() throws Exception {
        verifySizeRaisesUve((short) 0, "validateOnly",
            new CreateTopicsRequestData().setValidateOnly(true));
        verifySizeSucceeds((short) 0,
            new CreateTopicsRequestData().setValidateOnly(false));
        verifySizeSucceeds((short) 0,
            new OffsetCommitRequestData().setRetentionTimeMs(123));
        verifySizeRaisesUve((short) 5, "forgotten",
            new FetchRequestData().setForgotten(Collections.singletonList(
                new FetchRequestData.ForgottenTopic().setName("foo"))));
    }

    private void verifySizeRaisesUve(short version, String problemFieldName,
                                     Message message) throws Exception {
        try {
            message.size(version);
            fail("Expected to see an UnsupportedVersionException when writing " +
                message + " at version " + version);
        } catch (UnsupportedVersionException e) {
            assertTrue("Expected to get an error message about " + problemFieldName,
                e.getMessage().contains(problemFieldName));
        }
    }

    private void verifySizeSucceeds(short version, Message message) throws Exception {
        message.size(version);
    }
}
