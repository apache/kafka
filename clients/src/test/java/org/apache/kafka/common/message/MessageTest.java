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
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection;
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.Utils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class MessageTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testAddOffsetsToTxnVersions() throws Exception {
        testAllMessageRoundTrips(new AddOffsetsToTxnRequestData().
                setTransactionalId("foobar").
                setProducerId(0xbadcafebadcafeL).
                setProducerEpoch((short) 123).
                setGroupId("baaz"));
        testAllMessageRoundTrips(new AddOffsetsToTxnResponseData().
                setThrottleTimeMs(42).
                setErrorCode((short) 0));
    }

    @Test
    public void testAddPartitionsToTxnVersions() throws Exception {
        testAllMessageRoundTrips(new AddPartitionsToTxnRequestData().
                setTransactionalId("blah").
                setProducerId(0xbadcafebadcafeL).
                setProducerEpoch((short) 30000).
                setTopics(new AddPartitionsToTxnTopicCollection(singletonList(
                        new AddPartitionsToTxnTopic().
                                setName("Topic").
                                setPartitions(singletonList(1))).iterator())));
    }

    @Test
    public void testCreateTopicsVersions() throws Exception {
        testAllMessageRoundTrips(new CreateTopicsRequestData().
                setTimeoutMs(1000).setTopics(new CreateTopicsRequestData.CreatableTopicCollection()));
    }

    @Test
    public void testDescribeAclsRequest() throws Exception {
        testAllMessageRoundTrips(new DescribeAclsRequestData().
                setResourceType((byte) 42).
                setResourceNameFilter(null).
                setResourcePatternType((byte) 3).
                setPrincipalFilter("abc").
                setHostFilter(null).
                setOperation((byte) 0).
                setPermissionType((byte) 0));
    }

    @Test
    public void testMetadataVersions() throws Exception {
        testAllMessageRoundTrips(new MetadataRequestData().setTopics(
                Arrays.asList(new MetadataRequestData.MetadataRequestTopic().setName("foo"),
                        new MetadataRequestData.MetadataRequestTopic().setName("bar")
                )));
        testAllMessageRoundTripsFromVersion((short) 1, new MetadataRequestData().
                setTopics(null).
                setAllowAutoTopicCreation(true).
                setIncludeClusterAuthorizedOperations(false).
                setIncludeTopicAuthorizedOperations(false));
        testAllMessageRoundTripsFromVersion((short) 4, new MetadataRequestData().
                setTopics(null).
                setAllowAutoTopicCreation(false).
                setIncludeClusterAuthorizedOperations(false).
                setIncludeTopicAuthorizedOperations(false));
    }

    @Test
    public void testHeartbeatVersions() throws Exception {
        Supplier<HeartbeatRequestData> newRequest = () -> new HeartbeatRequestData()
                .setGroupId("groupId")
                .setMemberId("memberId")
                .setGenerationId(15);
        testAllMessageRoundTrips(newRequest.get());
        testAllMessageRoundTrips(newRequest.get().setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 3, newRequest.get().setGroupInstanceId("instanceId"));
    }

    @Test
    public void testJoinGroupRequestVersions() throws Exception {
        Supplier<JoinGroupRequestData> newRequest = () -> new JoinGroupRequestData()
                .setGroupId("groupId")
                .setMemberId("memberId")
                .setProtocolType("consumer")
                .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection())
                .setSessionTimeoutMs(10000);
        testAllMessageRoundTrips(newRequest.get());
        testAllMessageRoundTripsFromVersion((short) 1, newRequest.get().setRebalanceTimeoutMs(20000));
        testAllMessageRoundTrips(newRequest.get().setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 5, newRequest.get().setGroupInstanceId("instanceId"));
    }

    @Test
    public void testJoinGroupResponseVersions() throws Exception {
        String memberId = "memberId";
        Supplier<JoinGroupResponseData> newResponse = () -> new JoinGroupResponseData()
                .setMemberId(memberId)
                .setLeader(memberId)
                .setGenerationId(1)
                .setMembers(Collections.singletonList(
                        new JoinGroupResponseMember()
                                .setMemberId(memberId)
                ));
        testAllMessageRoundTrips(newResponse.get());
        testAllMessageRoundTripsFromVersion((short) 2, newResponse.get().setThrottleTimeMs(1000));
        testAllMessageRoundTrips(newResponse.get().members().get(0).setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 5, newResponse.get().members().get(0).setGroupInstanceId("instanceId"));
    }

    @Test
    public void testSyncGroupDefaultGroupInstanceId() throws Exception {
        Supplier<SyncGroupRequestData> request = () -> new SyncGroupRequestData()
                .setGroupId("groupId")
                .setMemberId("memberId")
                .setGenerationId(15)
                .setAssignments(new ArrayList<>());
        testAllMessageRoundTrips(request.get());
        testAllMessageRoundTrips(request.get().setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 3, request.get().setGroupInstanceId("instanceId"));
    }

    @Test
    public void testOffsetCommitDefaultGroupInstanceId() throws Exception {
        testAllMessageRoundTrips(new OffsetCommitRequestData()
                .setTopics(new ArrayList<>())
                .setGroupId("groupId"));

        Supplier<OffsetCommitRequestData> request = () -> new OffsetCommitRequestData()
                .setGroupId("groupId")
                .setMemberId("memberId")
                .setTopics(new ArrayList<>())
                .setGenerationId(15);
        testAllMessageRoundTripsFromVersion((short) 1, request.get());
        testAllMessageRoundTripsFromVersion((short) 1, request.get().setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 7, request.get().setGroupInstanceId("instanceId"));
    }

    @Test
    public void testOffsetForLeaderEpochVersions() throws Exception {
        // Version 2 adds optional current leader epoch
        OffsetForLeaderEpochRequestData.OffsetForLeaderPartition partitionDataNoCurrentEpoch =
                new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
                        .setPartitionIndex(0)
                        .setLeaderEpoch(3);
        OffsetForLeaderEpochRequestData.OffsetForLeaderPartition partitionDataWithCurrentEpoch =
                new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
                        .setPartitionIndex(0)
                        .setLeaderEpoch(3)
                        .setCurrentLeaderEpoch(5);

        testAllMessageRoundTrips(new OffsetForLeaderEpochRequestData().setTopics(singletonList(
                new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic()
                        .setName("foo")
                        .setPartitions(singletonList(partitionDataNoCurrentEpoch)))
        ));
        testAllMessageRoundTripsBeforeVersion((short) 2, partitionDataWithCurrentEpoch, partitionDataNoCurrentEpoch);
        testAllMessageRoundTripsFromVersion((short) 2, partitionDataWithCurrentEpoch);

        // Version 3 adds the optional replica Id field
        testAllMessageRoundTripsFromVersion((short) 3, new OffsetForLeaderEpochRequestData().setReplicaId(5));
        testAllMessageRoundTripsBeforeVersion((short) 3,
                new OffsetForLeaderEpochRequestData().setReplicaId(5),
                new OffsetForLeaderEpochRequestData());
        testAllMessageRoundTripsBeforeVersion((short) 3,
                new OffsetForLeaderEpochRequestData().setReplicaId(5),
                new OffsetForLeaderEpochRequestData().setReplicaId(-2));

    }

    private void testAllMessageRoundTrips(Message message) throws Exception {
        testAllMessageRoundTripsFromVersion(message.lowestSupportedVersion(), message);
    }

    private void testAllMessageRoundTripsBeforeVersion(short beforeVersion, Message message, Message expected) throws Exception {
        for (short version = 0; version < beforeVersion; version++) {
            testMessageRoundTrip(version, message, expected);
        }
    }

    private void testAllMessageRoundTripsFromVersion(short fromVersion, Message message) throws Exception {
        for (short version = fromVersion; version < message.highestSupportedVersion(); version++) {
            testEquivalentMessageRoundTrip(version, message);
        }
    }

    private void testMessageRoundTrip(short version, Message message, Message expected) throws Exception {
        testByteBufferRoundTrip(version, message, expected);
        testStructRoundTrip(version, message, expected);
    }

    private void testEquivalentMessageRoundTrip(short version, Message message) throws Exception {
        testStructRoundTrip(version, message, message);
        testByteBufferRoundTrip(version, message, message);
    }

    private void testByteBufferRoundTrip(short version, Message message, Message expected) throws Exception {
        int size = message.size(version);
        ByteBuffer buf = ByteBuffer.allocate(size);
        ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(buf);
        message.write(byteBufferAccessor, version);
        assertEquals(size, buf.position());
        Message message2 = message.getClass().newInstance();
        buf.flip();
        message2.read(byteBufferAccessor, version);
        assertEquals(size, buf.position());
        assertEquals(expected, message2);
        assertEquals(expected.hashCode(), message2.hashCode());
        assertEquals(expected.toString(), message2.toString());
    }

    private void testStructRoundTrip(short version, Message message, Message expected) throws Exception {
        Struct struct = message.toStruct(version);
        Message message2 = message.getClass().newInstance();
        message2.fromStruct(struct, version);
        assertEquals(expected, message2);
        assertEquals(expected.hashCode(), message2.hashCode());
        assertEquals(expected.toString(), message2.toString());
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
                message = ApiMessageType.fromApiKey(apiKey.id).newRequest();
            } catch (UnsupportedVersionException e) {
                fail("No request message spec found for API " + apiKey);
            }
            assertTrue("Request message spec for " + apiKey + " only " +
                    "supports versions up to " + message.highestSupportedVersion(),
                apiKey.latestVersion() <= message.highestSupportedVersion());
            try {
                message = ApiMessageType.fromApiKey(apiKey.id).newResponse();
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
            Schema[] generatedSchemas = ApiMessageType.fromApiKey(apiKey.id).requestSchemas();
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
            Schema[] generatedSchemas = ApiMessageType.fromApiKey(apiKey.id).responseSchemas();
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
            return singletonList(type);
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
            new FetchRequestData().setForgotten(singletonList(
                new FetchRequestData.ForgottenTopic().setName("foo"))));
    }

    @Test
    public void testNonIgnorableFieldWithDefaultNull() throws Exception {
        // Test non-ignorable string field `groupInstanceId` with default null
        verifySizeRaisesUve((short) 0, "groupInstanceId", new HeartbeatRequestData()
                .setGroupId("groupId")
                .setGenerationId(15)
                .setMemberId("memberId")
                .setGroupInstanceId("instanceId"));
        verifySizeSucceeds((short) 0, new HeartbeatRequestData()
                .setGroupId("groupId")
                .setGenerationId(15)
                .setMemberId("memberId")
                .setGroupInstanceId(null));
        verifySizeSucceeds((short) 0, new HeartbeatRequestData()
                .setGroupId("groupId")
                .setGenerationId(15)
                .setMemberId("memberId"));
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
