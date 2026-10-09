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
package org.apache.kafka.connect.runtime.rest.entities;

import org.apache.kafka.connect.runtime.TargetState;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CreateConnectorRequestTest {

    // A bare ObjectMapper, matching what RestClient and ConnectStandalone use, so that these tests exercise the same
    // (de)serialization behaviour as the follower -> leader forwarding path and the standalone CLI.
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testToTargetState() {
        assertEquals(TargetState.STARTED, CreateConnectorRequest.InitialState.RUNNING.toTargetState());
        assertEquals(TargetState.PAUSED, CreateConnectorRequest.InitialState.PAUSED.toTargetState());
        assertEquals(TargetState.STOPPED, CreateConnectorRequest.InitialState.STOPPED.toTargetState());

        CreateConnectorRequest createConnectorRequest = new CreateConnectorRequest("test-name", Map.of(), null);
        assertNull(createConnectorRequest.initialTargetState());
    }

    @Test
    public void testForValue() {
        assertEquals(CreateConnectorRequest.InitialState.RUNNING, CreateConnectorRequest.InitialState.forValue("running"));
        assertEquals(CreateConnectorRequest.InitialState.RUNNING, CreateConnectorRequest.InitialState.forValue("Running"));
        assertEquals(CreateConnectorRequest.InitialState.RUNNING, CreateConnectorRequest.InitialState.forValue("RUNNING"));

        assertEquals(CreateConnectorRequest.InitialState.PAUSED, CreateConnectorRequest.InitialState.forValue("paused"));
        assertEquals(CreateConnectorRequest.InitialState.PAUSED, CreateConnectorRequest.InitialState.forValue("Paused"));
        assertEquals(CreateConnectorRequest.InitialState.PAUSED, CreateConnectorRequest.InitialState.forValue("PAUSED"));

        assertEquals(CreateConnectorRequest.InitialState.STOPPED, CreateConnectorRequest.InitialState.forValue("stopped"));
        assertEquals(CreateConnectorRequest.InitialState.STOPPED, CreateConnectorRequest.InitialState.forValue("Stopped"));
        assertEquals(CreateConnectorRequest.InitialState.STOPPED, CreateConnectorRequest.InitialState.forValue("STOPPED"));
    }

    @Test
    public void testThreeArgConstructorLeavesInitialOffsetsNull() {
        CreateConnectorRequest request = new CreateConnectorRequest("test-name", Map.of(), null);
        assertNull(request.initialOffsets());
        assertNull(request.initialOffsetsMap());
    }

    @Test
    public void testDeserializeSourceInitialOffsets() throws Exception {
        String json = "{"
            + "\"name\": \"source-1\","
            + "\"config\": {\"connector.class\": \"FileStreamSource\"},"
            + "\"initial_offsets\": [{\"partition\": {\"filename\": \"test.txt\"}, \"offset\": {\"position\": 4096}}]"
            + "}";

        CreateConnectorRequest request = OBJECT_MAPPER.readValue(json, CreateConnectorRequest.class);

        assertEquals("source-1", request.name());
        assertNull(request.initialState());
        assertEquals(1, request.initialOffsets().size());
        assertEquals(Map.of("filename", "test.txt"), request.initialOffsets().get(0).partition());
        assertEquals(Map.of("position", 4096), request.initialOffsets().get(0).offset());
        assertEquals(Map.of(Map.of("filename", "test.txt"), Map.of("position", 4096)), request.initialOffsetsMap());
    }

    @Test
    public void testDeserializeSinkInitialOffsets() throws Exception {
        String json = "{"
            + "\"name\": \"sink-1\","
            + "\"config\": {\"connector.class\": \"FileStreamSink\"},"
            + "\"initial_offsets\": [{\"partition\": {\"kafka_topic\": \"t\", \"kafka_partition\": 0},"
            + " \"offset\": {\"kafka_offset\": 100}}]"
            + "}";

        CreateConnectorRequest request = OBJECT_MAPPER.readValue(json, CreateConnectorRequest.class);

        assertEquals(Map.of(Map.of("kafka_topic", "t", "kafka_partition", 0), Map.of("kafka_offset", 100)),
            request.initialOffsetsMap());
    }

    @Test
    public void testInitialOffsetsRoundTrip() throws Exception {
        // Serialize then deserialize. This pins Jackson's choice of creator: CreateConnectorRequest declares both a
        // canonical (4-arg) and a convenience (3-arg) constructor, and only the canonical one is annotated.
        CreateConnectorRequest original = new CreateConnectorRequest(
            "source-1",
            Map.of("connector.class", "FileStreamSource"),
            CreateConnectorRequest.InitialState.STOPPED,
            List.of(new ConnectorOffset(Map.of("filename", "test.txt"), Map.of("position", 4096)))
        );

        String serialized = OBJECT_MAPPER.writeValueAsString(original);
        assertTrue(serialized.contains("\"initial_offsets\""),
            "Serialized form should use the initial_offsets JSON property name: " + serialized);

        CreateConnectorRequest roundTripped = OBJECT_MAPPER.readValue(serialized, CreateConnectorRequest.class);
        assertEquals(original, roundTripped);
        assertEquals(original.initialOffsetsMap(), roundTripped.initialOffsetsMap());
    }

    @Test
    public void testInitialOffsetsAbsentIsNull() throws Exception {
        // Absence must be distinguishable from an empty list: absent means "leave offsets alone and omit
        // offsets_status from the response", whereas an empty list is an invalid request.
        String json = "{\"name\": \"source-1\", \"config\": {\"connector.class\": \"FileStreamSource\"}}";

        CreateConnectorRequest request = OBJECT_MAPPER.readValue(json, CreateConnectorRequest.class);

        assertNull(request.initialOffsets());
        assertNull(request.initialOffsetsMap());
    }

    @Test
    public void testInitialOffsetsEmptyListIsEmptyMap() throws Exception {
        String json = "{\"name\": \"source-1\", \"config\": {}, \"initial_offsets\": []}";

        CreateConnectorRequest request = OBJECT_MAPPER.readValue(json, CreateConnectorRequest.class);

        assertNotNull(request.initialOffsets());
        assertTrue(request.initialOffsets().isEmpty());
        assertNotNull(request.initialOffsetsMap());
        assertTrue(request.initialOffsetsMap().isEmpty());
    }

    @Test
    public void testDeserializeNullOffsetValue() throws Exception {
        // A null offset value is accepted by the wire format (it means "reset this partition" on
        // PATCH /connectors/{connector}/offsets). Whether it is legal in initial_offsets is a separate
        // validation concern; this test only pins that parsing does not fail.
        String json = "{\"name\": \"sink-1\", \"config\": {},"
            + " \"initial_offsets\": [{\"partition\": {\"kafka_topic\": \"t\", \"kafka_partition\": 0}, \"offset\": null}]}";

        CreateConnectorRequest request = OBJECT_MAPPER.readValue(json, CreateConnectorRequest.class);

        assertEquals(1, request.initialOffsets().size());
        assertNull(request.initialOffsets().get(0).offset());
        assertNull(request.initialOffsetsMap().get(Map.of("kafka_topic", "t", "kafka_partition", 0)));
    }

    @Test
    public void testUnknownFieldRejected() {
        // Pins the strictness that the follower -> leader forwarding path relies on: an old worker receiving a
        // forwarded request with initial_offsets must fail loudly rather than silently drop the field.
        String json = "{\"name\": \"source-1\", \"config\": {}, \"not_a_real_field\": 1}";

        assertThrows(UnrecognizedPropertyException.class,
            () -> OBJECT_MAPPER.readValue(json, CreateConnectorRequest.class));
    }
}
