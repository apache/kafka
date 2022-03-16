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
package org.apache.kafka.connect.runtime.rest.resources;

import com.fasterxml.jackson.core.type.TypeReference;

import javax.crypto.Mac;
import javax.ws.rs.core.HttpHeaders;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.RestartRequest;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.NotAssignedException;
import org.apache.kafka.connect.runtime.distributed.NotLeaderException;
import org.apache.kafka.connect.runtime.distributed.RebalanceNeededException;
import org.apache.kafka.connect.runtime.rest.InternalRequestSignature;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.entities.ActiveTopicsInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_TRACKING_ALLOW_RESET_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_TRACKING_ENABLE_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest(RestClient.class)
@PowerMockIgnore({"javax.management.*", "javax.crypto.*"})
@SuppressWarnings("unchecked")
public class ConnectorsResourceTest {
    // Note trailing / and that we do *not* use LEADER_URL to construct our reference values. This checks that we handle
    // URL construction properly, avoiding //, which will mess up routing in the REST server
    private static final String LEADER_URL = "http://leader:8083/";
    private static final String CONNECTOR_NAME = "test";
    private static final String CONNECTOR_NAME_SPECIAL_CHARS = "ta/b&c=d//\\rx=1þ.1>< `'\" x%y+z!ሴ#$&'(æ)*+,:;=?ñ@[]ÿ";
    private static final String CONNECTOR_NAME_CONTROL_SEQUENCES1 = "ta/b&c=drx=1\n.1>< `'\" x%y+z!#$&'()*+,:;=?@[]";
    private static final String CONNECTOR2_NAME = "test2";
    private static final String CONNECTOR_NAME_ALL_WHITESPACES = "   \t\n  \b";
    private static final String CONNECTOR_NAME_PADDING_WHITESPACES = "   " + CONNECTOR_NAME + "  \n  ";
    private static final Boolean FORWARD = true;
    private static final Map<String, String> CONNECTOR_CONFIG_SPECIAL_CHARS = new HashMap<>();
    private static final HttpHeaders NULL_HEADERS = null;
    static {
        CONNECTOR_CONFIG_SPECIAL_CHARS.put("name", CONNECTOR_NAME_SPECIAL_CHARS);
        CONNECTOR_CONFIG_SPECIAL_CHARS.put("sample_config", "test_config");
    }

    private static final Map<String, String> CONNECTOR_CONFIG = new HashMap<>();
    static {
        CONNECTOR_CONFIG.put("name", CONNECTOR_NAME);
        CONNECTOR_CONFIG.put("sample_config", "test_config");
    }

    private static final Map<String, String> CONNECTOR_CONFIG_CONTROL_SEQUENCES = new HashMap<>();
    static {
        CONNECTOR_CONFIG_CONTROL_SEQUENCES.put("name", CONNECTOR_NAME_CONTROL_SEQUENCES1);
        CONNECTOR_CONFIG_CONTROL_SEQUENCES.put("sample_config", "test_config");
    }

    private static final Map<String, String> CONNECTOR_CONFIG_WITHOUT_NAME = new HashMap<>();
    static {
        CONNECTOR_CONFIG_WITHOUT_NAME.put("sample_config", "test_config");
    }

    private static final Map<String, String> CONNECTOR_CONFIG_WITH_EMPTY_NAME = new HashMap<>();

    static {
        CONNECTOR_CONFIG_WITH_EMPTY_NAME.put(ConnectorConfig.NAME_CONFIG, "");
        CONNECTOR_CONFIG_WITH_EMPTY_NAME.put("sample_config", "test_config");
    }
    private static final List<ConnectorTaskId> CONNECTOR_TASK_NAMES = Arrays.asList(
            new ConnectorTaskId(CONNECTOR_NAME, 0),
            new ConnectorTaskId(CONNECTOR_NAME, 1)
    );
    private static final List<Map<String, String>> TASK_CONFIGS = new ArrayList<>();
    static {
        TASK_CONFIGS.add(Collections.singletonMap("config", "value"));
        TASK_CONFIGS.add(Collections.singletonMap("config", "other_value"));
    }
    private static final List<TaskInfo> TASK_INFOS = new ArrayList<>();
    static {
        TASK_INFOS.add(new TaskInfo(new ConnectorTaskId(CONNECTOR_NAME, 0), TASK_CONFIGS.get(0)));
        TASK_INFOS.add(new TaskInfo(new ConnectorTaskId(CONNECTOR_NAME, 1), TASK_CONFIGS.get(1)));
    }

    private static final Set<String> CONNECTOR_ACTIVE_TOPICS = new HashSet<>(
            Arrays.asList("foo_topic", "bar_topic"));
    private static final Set<String> CONNECTOR2_ACTIVE_TOPICS = new HashSet<>(
            Arrays.asList("foo_topic", "baz_topic"));

    @Mock
    private Herder herder;
    private ConnectorsResource connectorsResource;
    private UriInfo forward;
    @Mock
    private WorkerConfig workerConfig;

    @Before
    public void setUp() throws NoSuchMethodException {
        PowerMock.mockStatic(RestClient.class,
                RestClient.class.getMethod("httpRequest", String.class, String.class, HttpHeaders.class, Object.class, TypeReference.class, WorkerConfig.class));
        EasyMock.expect(workerConfig.getBoolean(TOPIC_TRACKING_ENABLE_CONFIG)).andReturn(true);
        EasyMock.expect(workerConfig.getBoolean(TOPIC_TRACKING_ALLOW_RESET_CONFIG)).andReturn(true);
        PowerMock.replay(workerConfig);
        connectorsResource = new ConnectorsResource(herder, workerConfig);
        forward = EasyMock.mock(UriInfo.class);
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
        queryParams.putSingle("forward", "true");
        EasyMock.expect(forward.getQueryParameters()).andReturn(queryParams).anyTimes();
        EasyMock.replay(forward);
    }

    private static Map<String, String> getConnectorConfig(Map<String, String> mapToClone) {
        Map<String, String> result = new HashMap<>(mapToClone);
        return result;
    }

    @Test
    public void testListConnectors() {
        final Capture<Callback<Collection<String>>> cb = Capture.newInstance();
        EasyMock.expect(herder.connectors()).andReturn(Arrays.asList(CONNECTOR2_NAME, CONNECTOR_NAME));

        PowerMock.replayAll();

        Collection<String> connectors = (Collection<String>) connectorsResource.listConnectors(forward, NULL_HEADERS).getEntity();
        // Ordering isn't guaranteed, compare sets
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_NAME, CONNECTOR2_NAME)), new HashSet<>(connectors));

        PowerMock.verifyAll();
    }

    @Test
    public void testExpandConnectorsStatus() {
        EasyMock.expect(herder.connectors()).andReturn(Arrays.asList(CONNECTOR2_NAME, CONNECTOR_NAME));
        ConnectorStateInfo connector = EasyMock.mock(ConnectorStateInfo.class);
        ConnectorStateInfo connector2 = EasyMock.mock(ConnectorStateInfo.class);
        EasyMock.expect(herder.connectorStatus(CONNECTOR2_NAME)).andReturn(connector2);
        EasyMock.expect(herder.connectorStatus(CONNECTOR_NAME)).andReturn(connector);

        forward = EasyMock.mock(UriInfo.class);
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
        queryParams.putSingle("expand", "status");
        EasyMock.expect(forward.getQueryParameters()).andReturn(queryParams).anyTimes();
        EasyMock.replay(forward);

        PowerMock.replayAll();

        Map<String, Map<String, Object>> expanded = (Map<String, Map<String, Object>>) connectorsResource.listConnectors(forward, NULL_HEADERS).getEntity();
        // Ordering isn't guaranteed, compare sets
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_NAME, CONNECTOR2_NAME)), expanded.keySet());
        assertEquals(connector2, expanded.get(CONNECTOR2_NAME).get("status"));
        assertEquals(connector, expanded.get(CONNECTOR_NAME).get("status"));
        PowerMock.verifyAll();
    }

    @Test
    public void testExpandConnectorsInfo() {
        EasyMock.expect(herder.connectors()).andReturn(Arrays.asList(CONNECTOR2_NAME, CONNECTOR_NAME));
        ConnectorInfo connector = EasyMock.mock(ConnectorInfo.class);
        ConnectorInfo connector2 = EasyMock.mock(ConnectorInfo.class);
        EasyMock.expect(herder.connectorInfo(CONNECTOR2_NAME)).andReturn(connector2);
        EasyMock.expect(herder.connectorInfo(CONNECTOR_NAME)).andReturn(connector);

        forward = EasyMock.mock(UriInfo.class);
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
        queryParams.putSingle("expand", "info");
        EasyMock.expect(forward.getQueryParameters()).andReturn(queryParams).anyTimes();
        EasyMock.replay(forward);

        PowerMock.replayAll();

        Map<String, Map<String, Object>> expanded = (Map<String, Map<String, Object>>) connectorsResource.listConnectors(forward, NULL_HEADERS).getEntity();
        // Ordering isn't guaranteed, compare sets
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_NAME, CONNECTOR2_NAME)), expanded.keySet());
        assertEquals(connector2, expanded.get(CONNECTOR2_NAME).get("info"));
        assertEquals(connector, expanded.get(CONNECTOR_NAME).get("info"));
        PowerMock.verifyAll();
    }

    @Test
    public void testFullExpandConnectors() {
        EasyMock.expect(herder.connectors()).andReturn(Arrays.asList(CONNECTOR2_NAME, CONNECTOR_NAME));
        ConnectorInfo connectorInfo = EasyMock.mock(ConnectorInfo.class);
        ConnectorInfo connectorInfo2 = EasyMock.mock(ConnectorInfo.class);
        EasyMock.expect(herder.connectorInfo(CONNECTOR2_NAME)).andReturn(connectorInfo2);
        EasyMock.expect(herder.connectorInfo(CONNECTOR_NAME)).andReturn(connectorInfo);
        ConnectorStateInfo connector = EasyMock.mock(ConnectorStateInfo.class);
        ConnectorStateInfo connector2 = EasyMock.mock(ConnectorStateInfo.class);
        EasyMock.expect(herder.connectorStatus(CONNECTOR2_NAME)).andReturn(connector2);
        EasyMock.expect(herder.connectorStatus(CONNECTOR_NAME)).andReturn(connector);

        forward = EasyMock.mock(UriInfo.class);
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
        queryParams.put("expand", Arrays.asList("info", "status"));
        EasyMock.expect(forward.getQueryParameters()).andReturn(queryParams).anyTimes();
        EasyMock.replay(forward);

        PowerMock.replayAll();

        Map<String, Map<String, Object>> expanded = (Map<String, Map<String, Object>>) connectorsResource.listConnectors(forward, NULL_HEADERS).getEntity();
        // Ordering isn't guaranteed, compare sets
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_NAME, CONNECTOR2_NAME)), expanded.keySet());
        assertEquals(connectorInfo2, expanded.get(CONNECTOR2_NAME).get("info"));
        assertEquals(connectorInfo, expanded.get(CONNECTOR_NAME).get("info"));
        assertEquals(connector2, expanded.get(CONNECTOR2_NAME).get("status"));
        assertEquals(connector, expanded.get(CONNECTOR_NAME).get("status"));
        PowerMock.verifyAll();
    }

    @Test
    public void testExpandConnectorsWithConnectorNotFound() {
        EasyMock.expect(herder.connectors()).andReturn(Arrays.asList(CONNECTOR2_NAME, CONNECTOR_NAME));
        ConnectorStateInfo connector = EasyMock.mock(ConnectorStateInfo.class);
        ConnectorStateInfo connector2 = EasyMock.mock(ConnectorStateInfo.class);
        EasyMock.expect(herder.connectorStatus(CONNECTOR2_NAME)).andReturn(connector2);
        EasyMock.expect(herder.connectorStatus(CONNECTOR_NAME)).andThrow(EasyMock.mock(NotFoundException.class));

        forward = EasyMock.mock(UriInfo.class);
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
        queryParams.putSingle("expand", "status");
        EasyMock.expect(forward.getQueryParameters()).andReturn(queryParams).anyTimes();
        EasyMock.replay(forward);

        PowerMock.replayAll();

        Map<String, Map<String, Object>> expanded = (Map<String, Map<String, Object>>) connectorsResource.listConnectors(forward, NULL_HEADERS).getEntity();
        // Ordering isn't guaranteed, compare sets
        assertEquals(Collections.singleton(CONNECTOR2_NAME), expanded.keySet());
        assertEquals(connector2, expanded.get(CONNECTOR2_NAME).get("status"));
        PowerMock.verifyAll();
    }


    @Test
    public void testCreateConnector() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME, Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME));

        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(body.config()), EasyMock.eq(false), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG,
            CONNECTOR_TASK_NAMES, ConnectorType.SOURCE)));

        PowerMock.replayAll();

        connectorsResource.createConnector(FORWARD, NULL_HEADERS, body);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorNotLeader() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME, Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME));

        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(body.config()), EasyMock.eq(false), EasyMock.capture(cb));
        expectAndCallbackNotLeaderException(cb);
        // Should forward request
        EasyMock.expect(RestClient.httpRequest(EasyMock.eq("http://leader:8083/connectors?forward=false"), EasyMock.eq("POST"), EasyMock.isNull(), EasyMock.eq(body), EasyMock.anyObject(), EasyMock.anyObject(WorkerConfig.class)))
                .andReturn(new RestClient.HttpResponse<>(201, new HashMap<>(), new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG, CONNECTOR_TASK_NAMES,
                    ConnectorType.SOURCE)));

        PowerMock.replayAll();

        connectorsResource.createConnector(FORWARD, NULL_HEADERS, body);

        PowerMock.verifyAll();


    }

    @Test
    public void testCreateConnectorWithHeaderAuthorization() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME, Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME));
        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        HttpHeaders httpHeaders = EasyMock.mock(HttpHeaders.class);
        EasyMock.expect(httpHeaders.getHeaderString("Authorization")).andReturn("Basic YWxhZGRpbjpvcGVuc2VzYW1l").times(1);
        EasyMock.replay(httpHeaders);
        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(body.config()), EasyMock.eq(false), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG,
            CONNECTOR_TASK_NAMES, ConnectorType.SOURCE)));

        PowerMock.replayAll();

        connectorsResource.createConnector(FORWARD, httpHeaders, body);

        PowerMock.verifyAll();
    }



    @Test
    public void testCreateConnectorWithoutHeaderAuthorization() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME, Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME));
        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        HttpHeaders httpHeaders = EasyMock.mock(HttpHeaders.class);
        EasyMock.expect(httpHeaders.getHeaderString("Authorization")).andReturn(null).times(1);
        EasyMock.replay(httpHeaders);
        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(body.config()), EasyMock.eq(false), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG,
            CONNECTOR_TASK_NAMES, ConnectorType.SOURCE)));

        PowerMock.replayAll();

        connectorsResource.createConnector(FORWARD, httpHeaders, body);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorExists() {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME, Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME));

        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(body.config()), EasyMock.eq(false), EasyMock.capture(cb));
        expectAndCallbackException(cb, new AlreadyExistsException("already exists"));

        PowerMock.replayAll();

        assertThrows(AlreadyExistsException.class, () -> connectorsResource.createConnector(FORWARD, NULL_HEADERS, body));

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorNameTrimWhitespaces() throws Throwable {
        // Clone CONNECTOR_CONFIG_WITHOUT_NAME Map, as createConnector changes it (puts the name in it) and this
        // will affect later tests
        Map<String, String> inputConfig = getConnectorConfig(CONNECTOR_CONFIG_WITHOUT_NAME);
        final CreateConnectorRequest bodyIn = new CreateConnectorRequest(CONNECTOR_NAME_PADDING_WHITESPACES, inputConfig);
        final CreateConnectorRequest bodyOut = new CreateConnectorRequest(CONNECTOR_NAME, CONNECTOR_CONFIG);

        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.putConnectorConfig(EasyMock.eq(bodyOut.name()), EasyMock.eq(bodyOut.config()), EasyMock.eq(false), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(bodyOut.name(), bodyOut.config(), CONNECTOR_TASK_NAMES, ConnectorType.SOURCE)));

        PowerMock.replayAll();

        connectorsResource.createConnector(FORWARD, NULL_HEADERS, bodyIn);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorNameAllWhitespaces() throws Throwable {
        // Clone CONNECTOR_CONFIG_WITHOUT_NAME Map, as createConnector changes it (puts the name in it) and this
        // will affect later tests
        Map<String, String> inputConfig = getConnectorConfig(CONNECTOR_CONFIG_WITHOUT_NAME);
        final CreateConnectorRequest bodyIn = new CreateConnectorRequest(CONNECTOR_NAME_ALL_WHITESPACES, inputConfig);
        final CreateConnectorRequest bodyOut = new CreateConnectorRequest("", CONNECTOR_CONFIG_WITH_EMPTY_NAME);

        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.putConnectorConfig(EasyMock.eq(bodyOut.name()), EasyMock.eq(bodyOut.config()), EasyMock.eq(false), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(bodyOut.name(), bodyOut.config(), CONNECTOR_TASK_NAMES, ConnectorType.SOURCE)));

        PowerMock.replayAll();

        connectorsResource.createConnector(FORWARD, NULL_HEADERS, bodyIn);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorNoName() throws Throwable {
        // Clone CONNECTOR_CONFIG_WITHOUT_NAME Map, as createConnector changes it (puts the name in it) and this
        // will affect later tests
        Map<String, String> inputConfig = getConnectorConfig(CONNECTOR_CONFIG_WITHOUT_NAME);
        final CreateConnectorRequest bodyIn = new CreateConnectorRequest(null, inputConfig);
        final CreateConnectorRequest bodyOut = new CreateConnectorRequest("", CONNECTOR_CONFIG_WITH_EMPTY_NAME);

        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.putConnectorConfig(EasyMock.eq(bodyOut.name()), EasyMock.eq(bodyOut.config()), EasyMock.eq(false), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(bodyOut.name(), bodyOut.config(), CONNECTOR_TASK_NAMES, ConnectorType.SOURCE)));

        PowerMock.replayAll();

        connectorsResource.createConnector(FORWARD, NULL_HEADERS, bodyIn);

        PowerMock.verifyAll();
    }

    @Test
    public void testDeleteConnector() throws Throwable {
        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.deleteConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackResult(cb, null);

        PowerMock.replayAll();

        connectorsResource.destroyConnector(CONNECTOR_NAME, NULL_HEADERS, FORWARD);

        PowerMock.verifyAll();
    }

    @Test
    public void testDeleteConnectorNotLeader() throws Throwable {
        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.deleteConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackNotLeaderException(cb);
        // Should forward request
        EasyMock.expect(RestClient.httpRequest("http://leader:8083/connectors/" + CONNECTOR_NAME + "?forward=false", "DELETE", NULL_HEADERS, null, null, workerConfig))
                .andReturn(new RestClient.HttpResponse<>(204, new HashMap<>(), null));

        PowerMock.replayAll();

        connectorsResource.destroyConnector(CONNECTOR_NAME, NULL_HEADERS, FORWARD);

        PowerMock.verifyAll();
    }

    // Not found exceptions should pass through to caller so they can be processed for 404s
    @Test
    public void testDeleteConnectorNotFound() {
        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.deleteConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        assertThrows(NotFoundException.class, () -> connectorsResource.destroyConnector(CONNECTOR_NAME, NULL_HEADERS, FORWARD));

        PowerMock.verifyAll();
    }

    @Test
    public void testGetConnector() throws Throwable {
        final Capture<Callback<ConnectorInfo>> cb = Capture.newInstance();
        herder.connectorInfo(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG, CONNECTOR_TASK_NAMES,
            ConnectorType.SOURCE));

        PowerMock.replayAll();

        ConnectorInfo connInfo = connectorsResource.getConnector(CONNECTOR_NAME, NULL_HEADERS, FORWARD);
        assertEquals(new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG, CONNECTOR_TASK_NAMES, ConnectorType.SOURCE),
            connInfo);

        PowerMock.verifyAll();
    }

    @Test
    public void testGetConnectorConfig() throws Throwable {
        final Capture<Callback<Map<String, String>>> cb = Capture.newInstance();
        herder.connectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackResult(cb, CONNECTOR_CONFIG);

        PowerMock.replayAll();

        Map<String, String> connConfig = connectorsResource.getConnectorConfig(CONNECTOR_NAME, NULL_HEADERS, FORWARD);
        assertEquals(CONNECTOR_CONFIG, connConfig);

        PowerMock.verifyAll();
    }

    @Test
    public void testGetConnectorConfigConnectorNotFound() {
        final Capture<Callback<Map<String, String>>> cb = Capture.newInstance();
        herder.connectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        assertThrows(NotFoundException.class, () -> connectorsResource.getConnectorConfig(CONNECTOR_NAME, NULL_HEADERS, FORWARD));

        PowerMock.verifyAll();
    }

    @Test
    public void testGetTasksConfig() throws Throwable {
        final ConnectorTaskId connectorTask0 = new ConnectorTaskId(CONNECTOR_NAME, 0);
        final Map<String, String> connectorTask0Configs = new HashMap<>();
        connectorTask0Configs.put("connector-task0-config0", "123");
        connectorTask0Configs.put("connector-task0-config1", "456");
        final ConnectorTaskId connectorTask1 = new ConnectorTaskId(CONNECTOR_NAME, 1);
        final Map<String, String> connectorTask1Configs = new HashMap<>();
        connectorTask0Configs.put("connector-task1-config0", "321");
        connectorTask0Configs.put("connector-task1-config1", "654");
        final ConnectorTaskId connector2Task0 = new ConnectorTaskId(CONNECTOR2_NAME, 0);
        final Map<String, String> connector2Task0Configs = Collections.singletonMap("connector2-task0-config0", "789");

        final Map<ConnectorTaskId, Map<String, String>> expectedTasksConnector = new HashMap<>();
        expectedTasksConnector.put(connectorTask0, connectorTask0Configs);
        expectedTasksConnector.put(connectorTask1, connectorTask1Configs);
        final Map<ConnectorTaskId, Map<String, String>> expectedTasksConnector2 = new HashMap<>();
        expectedTasksConnector2.put(connector2Task0, connector2Task0Configs);

        final Capture<Callback<Map<ConnectorTaskId, Map<String, String>>>> cb1 = Capture.newInstance();
        herder.tasksConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb1));
        expectAndCallbackResult(cb1, expectedTasksConnector);
        final Capture<Callback<Map<ConnectorTaskId, Map<String, String>>>> cb2 = Capture.newInstance();
        herder.tasksConfig(EasyMock.eq(CONNECTOR2_NAME), EasyMock.capture(cb2));
        expectAndCallbackResult(cb2, expectedTasksConnector2);

        PowerMock.replayAll();

        Map<ConnectorTaskId, Map<String, String>> tasksConfig = connectorsResource.getTasksConfig(CONNECTOR_NAME, NULL_HEADERS, FORWARD);
        assertEquals(expectedTasksConnector, tasksConfig);
        Map<ConnectorTaskId, Map<String, String>> tasksConfig2 = connectorsResource.getTasksConfig(CONNECTOR2_NAME, NULL_HEADERS, FORWARD);
        assertEquals(expectedTasksConnector2, tasksConfig2);

        PowerMock.verifyAll();
    }

    @Test(expected = NotFoundException.class)
    public void testGetTasksConfigConnectorNotFound() throws Throwable {
        final Capture<Callback<Map<ConnectorTaskId, Map<String, String>>>> cb = Capture.newInstance();
        herder.tasksConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        connectorsResource.getTasksConfig(CONNECTOR_NAME, NULL_HEADERS, FORWARD);

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorConfig() throws Throwable {
        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(CONNECTOR_CONFIG), EasyMock.eq(true), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new Herder.Created<>(false, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG, CONNECTOR_TASK_NAMES,
            ConnectorType.SINK)));

        PowerMock.replayAll();

        connectorsResource.putConnectorConfig(CONNECTOR_NAME, NULL_HEADERS, FORWARD, CONNECTOR_CONFIG);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorWithSpecialCharsInName() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME_SPECIAL_CHARS, Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME_SPECIAL_CHARS));

        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME_SPECIAL_CHARS), EasyMock.eq(body.config()), EasyMock.eq(false), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME_SPECIAL_CHARS, CONNECTOR_CONFIG,
                CONNECTOR_TASK_NAMES, ConnectorType.SOURCE)));

        PowerMock.replayAll();

        String rspLocation = connectorsResource.createConnector(FORWARD, NULL_HEADERS, body).getLocation().toString();
        String decoded = new URI(rspLocation).getPath();
        Assert.assertEquals("/connectors/" + CONNECTOR_NAME_SPECIAL_CHARS, decoded);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorWithControlSequenceInName() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME_CONTROL_SEQUENCES1, Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME_CONTROL_SEQUENCES1));

        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME_CONTROL_SEQUENCES1), EasyMock.eq(body.config()), EasyMock.eq(false), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME_CONTROL_SEQUENCES1, CONNECTOR_CONFIG,
                CONNECTOR_TASK_NAMES, ConnectorType.SOURCE)));

        PowerMock.replayAll();

        String rspLocation = connectorsResource.createConnector(FORWARD, NULL_HEADERS, body).getLocation().toString();
        String decoded = new URI(rspLocation).getPath();
        Assert.assertEquals("/connectors/" + CONNECTOR_NAME_CONTROL_SEQUENCES1, decoded);

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorConfigWithSpecialCharsInName() throws Throwable {
        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();

        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME_SPECIAL_CHARS), EasyMock.eq(CONNECTOR_CONFIG_SPECIAL_CHARS), EasyMock.eq(true), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME_SPECIAL_CHARS, CONNECTOR_CONFIG_SPECIAL_CHARS, CONNECTOR_TASK_NAMES,
                ConnectorType.SINK)));

        PowerMock.replayAll();

        String rspLocation = connectorsResource.putConnectorConfig(CONNECTOR_NAME_SPECIAL_CHARS, NULL_HEADERS, FORWARD, CONNECTOR_CONFIG_SPECIAL_CHARS).getLocation().toString();
        String decoded = new URI(rspLocation).getPath();
        Assert.assertEquals("/connectors/" + CONNECTOR_NAME_SPECIAL_CHARS, decoded);

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorConfigWithControlSequenceInName() throws Throwable {
        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();

        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME_CONTROL_SEQUENCES1), EasyMock.eq(CONNECTOR_CONFIG_CONTROL_SEQUENCES), EasyMock.eq(true), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME_CONTROL_SEQUENCES1, CONNECTOR_CONFIG_CONTROL_SEQUENCES, CONNECTOR_TASK_NAMES,
                ConnectorType.SINK)));

        PowerMock.replayAll();

        String rspLocation = connectorsResource.putConnectorConfig(CONNECTOR_NAME_CONTROL_SEQUENCES1, NULL_HEADERS, FORWARD, CONNECTOR_CONFIG_CONTROL_SEQUENCES).getLocation().toString();
        String decoded = new URI(rspLocation).getPath();
        Assert.assertEquals("/connectors/" + CONNECTOR_NAME_CONTROL_SEQUENCES1, decoded);

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorConfigNameMismatch() {
        Map<String, String> connConfig = new HashMap<>(CONNECTOR_CONFIG);
        connConfig.put(ConnectorConfig.NAME_CONFIG, "mismatched-name");
        assertThrows(BadRequestException.class, () -> connectorsResource.putConnectorConfig(CONNECTOR_NAME,
            NULL_HEADERS, FORWARD, connConfig));
    }

    @Test
    public void testCreateConnectorConfigNameMismatch() {
        Map<String, String> connConfig = new HashMap<>();
        connConfig.put(ConnectorConfig.NAME_CONFIG, "mismatched-name");
        CreateConnectorRequest request = new CreateConnectorRequest(CONNECTOR_NAME, connConfig);
        assertThrows(BadRequestException.class, () -> connectorsResource.createConnector(FORWARD, NULL_HEADERS, request));
    }

    @Test
    public void testGetConnectorTaskConfigs() throws Throwable {
        final Capture<Callback<List<TaskInfo>>> cb = Capture.newInstance();
        herder.taskConfigs(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackResult(cb, TASK_INFOS);

        PowerMock.replayAll();

        List<TaskInfo> taskInfos = connectorsResource.getTaskConfigs(CONNECTOR_NAME, NULL_HEADERS, FORWARD);
        assertEquals(TASK_INFOS, taskInfos);

        PowerMock.verifyAll();
    }

    @Test
    public void testGetConnectorTaskConfigsConnectorNotFound() {
        final Capture<Callback<List<TaskInfo>>> cb = Capture.newInstance();
        herder.taskConfigs(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("connector not found"));

        PowerMock.replayAll();

        assertThrows(NotFoundException.class, () -> connectorsResource.getTaskConfigs(CONNECTOR_NAME, NULL_HEADERS, FORWARD));

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorTaskConfigsNoInternalRequestSignature() throws Throwable {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.putTaskConfigs(
            EasyMock.eq(CONNECTOR_NAME),
            EasyMock.eq(TASK_CONFIGS),
            EasyMock.capture(cb),
            EasyMock.anyObject(InternalRequestSignature.class)
        );
        expectAndCallbackResult(cb, null);

        PowerMock.replayAll();

        connectorsResource.putTaskConfigs(CONNECTOR_NAME, NULL_HEADERS, FORWARD, serializeAsBytes(TASK_CONFIGS));

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorTaskConfigsWithInternalRequestSignature() throws Throwable {
        final String signatureAlgorithm = "HmacSHA256";
        final String encodedSignature = "Kv1/OSsxzdVIwvZ4e30avyRIVrngDfhzVUm/kAZEKc4=";

        final Capture<Callback<Void>> cb = Capture.newInstance();
        final Capture<InternalRequestSignature> signatureCapture = Capture.newInstance();
        herder.putTaskConfigs(
            EasyMock.eq(CONNECTOR_NAME),
            EasyMock.eq(TASK_CONFIGS),
            EasyMock.capture(cb),
            EasyMock.capture(signatureCapture)
        );
        expectAndCallbackResult(cb, null);

        HttpHeaders headers = EasyMock.mock(HttpHeaders.class);
        EasyMock.expect(headers.getHeaderString(InternalRequestSignature.SIGNATURE_ALGORITHM_HEADER))
            .andReturn(signatureAlgorithm)
            .once();
        EasyMock.expect(headers.getHeaderString(InternalRequestSignature.SIGNATURE_HEADER))
            .andReturn(encodedSignature)
            .once();

        PowerMock.replayAll(headers);

        connectorsResource.putTaskConfigs(CONNECTOR_NAME, headers, FORWARD, serializeAsBytes(TASK_CONFIGS));

        PowerMock.verifyAll();
        InternalRequestSignature expectedSignature = new InternalRequestSignature(
            serializeAsBytes(TASK_CONFIGS),
            Mac.getInstance(signatureAlgorithm),
            Base64.getDecoder().decode(encodedSignature)
        );
        assertEquals(
            expectedSignature,
            signatureCapture.getValue()
        );
    }

    @Test
    public void testPutConnectorTaskConfigsConnectorNotFound() {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.putTaskConfigs(
            EasyMock.eq(CONNECTOR_NAME),
            EasyMock.eq(TASK_CONFIGS),
            EasyMock.capture(cb),
            EasyMock.anyObject(InternalRequestSignature.class)
        );
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        assertThrows(NotFoundException.class, () -> connectorsResource.putTaskConfigs(CONNECTOR_NAME, NULL_HEADERS,
            FORWARD, serializeAsBytes(TASK_CONFIGS)));

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorAndTasksConnectorNotFound() {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, true, false);
        final Capture<Callback<ConnectorStateInfo>> cb = Capture.newInstance();
        herder.restartConnectorAndTasks(EasyMock.eq(restartRequest), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        assertThrows(NotFoundException.class, () ->
                connectorsResource.restartConnector(CONNECTOR_NAME, NULL_HEADERS, restartRequest.includeTasks(), restartRequest.onlyFailed(), FORWARD)
        );

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorAndTasksLeaderRedirect() throws Throwable {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, true, false);
        final Capture<Callback<ConnectorStateInfo>> cb = Capture.newInstance();
        herder.restartConnectorAndTasks(EasyMock.eq(restartRequest), EasyMock.capture(cb));
        expectAndCallbackNotLeaderException(cb);

        EasyMock.expect(RestClient.httpRequest(EasyMock.eq("http://leader:8083/connectors/" + CONNECTOR_NAME + "/restart?forward=true&includeTasks=" + restartRequest.includeTasks() + "&onlyFailed=" + restartRequest.onlyFailed()),
                EasyMock.eq("POST"), EasyMock.isNull(), EasyMock.isNull(), EasyMock.anyObject(), EasyMock.anyObject(WorkerConfig.class)))
                .andReturn(new RestClient.HttpResponse<>(202, new HashMap<>(), null));

        PowerMock.replayAll();

        Response response = connectorsResource.restartConnector(CONNECTOR_NAME, NULL_HEADERS, restartRequest.includeTasks(), restartRequest.onlyFailed(), null);
        assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorAndTasksRebalanceNeeded() {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, true, false);
        final Capture<Callback<ConnectorStateInfo>> cb = Capture.newInstance();
        herder.restartConnectorAndTasks(EasyMock.eq(restartRequest), EasyMock.capture(cb));
        expectAndCallbackException(cb, new RebalanceNeededException("Request cannot be completed because a rebalance is expected"));

        PowerMock.replayAll();

        ConnectRestException ex = assertThrows(ConnectRestException.class, () ->
                connectorsResource.restartConnector(CONNECTOR_NAME, NULL_HEADERS, restartRequest.includeTasks(), restartRequest.onlyFailed(), FORWARD)
        );
        assertEquals(Response.Status.CONFLICT.getStatusCode(), ex.statusCode());
        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorAndTasksRequestAccepted() throws Throwable {
        ConnectorStateInfo.ConnectorState state = new ConnectorStateInfo.ConnectorState(
                AbstractStatus.State.RESTARTING.name(),
                "foo",
                null
        );
        ConnectorStateInfo connectorStateInfo = new ConnectorStateInfo(CONNECTOR_NAME, state, Collections.emptyList(), ConnectorType.SOURCE);

        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, true, false);
        final Capture<Callback<ConnectorStateInfo>> cb = Capture.newInstance();
        herder.restartConnectorAndTasks(EasyMock.eq(restartRequest), EasyMock.capture(cb));
        expectAndCallbackResult(cb, connectorStateInfo);

        PowerMock.replayAll();

        Response response = connectorsResource.restartConnector(CONNECTOR_NAME, NULL_HEADERS, restartRequest.includeTasks(), restartRequest.onlyFailed(), FORWARD);
        assertEquals(CONNECTOR_NAME, ((ConnectorStateInfo) response.getEntity()).name());
        assertEquals(state.state(), ((ConnectorStateInfo) response.getEntity()).connector().state());
        assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorNotFound() {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.restartConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        assertThrows(NotFoundException.class, () ->
                connectorsResource.restartConnector(CONNECTOR_NAME, NULL_HEADERS, false, false, FORWARD)
        );

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorLeaderRedirect() throws Throwable {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.restartConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackNotLeaderException(cb);

        EasyMock.expect(RestClient.httpRequest(EasyMock.eq("http://leader:8083/connectors/" + CONNECTOR_NAME + "/restart?forward=true"),
                EasyMock.eq("POST"), EasyMock.isNull(), EasyMock.isNull(), EasyMock.anyObject(), EasyMock.anyObject(WorkerConfig.class)))
                .andReturn(new RestClient.HttpResponse<>(202, new HashMap<>(), null));

        PowerMock.replayAll();

        Response response = connectorsResource.restartConnector(CONNECTOR_NAME, NULL_HEADERS, false, false, null);
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
        PowerMock.verifyAll();

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorOwnerRedirect() throws Throwable {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.restartConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        String ownerUrl = "http://owner:8083";
        expectAndCallbackException(cb, new NotAssignedException("not owner test", ownerUrl));

        EasyMock.expect(RestClient.httpRequest(EasyMock.eq("http://owner:8083/connectors/" + CONNECTOR_NAME + "/restart?forward=false"),
                EasyMock.eq("POST"), EasyMock.isNull(), EasyMock.isNull(), EasyMock.anyObject(), EasyMock.anyObject(WorkerConfig.class)))
                .andReturn(new RestClient.HttpResponse<>(202, new HashMap<>(), null));

        PowerMock.replayAll();

        Response response = connectorsResource.restartConnector(CONNECTOR_NAME, NULL_HEADERS, false, false, true);
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
        PowerMock.verifyAll();

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartTaskNotFound() {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.restartTask(EasyMock.eq(taskId), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        assertThrows(NotFoundException.class, () -> connectorsResource.restartTask(CONNECTOR_NAME, 0, NULL_HEADERS, FORWARD));

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartTaskLeaderRedirect() throws Throwable {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);

        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.restartTask(EasyMock.eq(taskId), EasyMock.capture(cb));
        expectAndCallbackNotLeaderException(cb);

        EasyMock.expect(RestClient.httpRequest(EasyMock.eq("http://leader:8083/connectors/" + CONNECTOR_NAME + "/tasks/0/restart?forward=true"),
                EasyMock.eq("POST"), EasyMock.isNull(), EasyMock.isNull(), EasyMock.anyObject(), EasyMock.anyObject(WorkerConfig.class)))
                .andReturn(new RestClient.HttpResponse<>(202, new HashMap<>(), null));

        PowerMock.replayAll();

        connectorsResource.restartTask(CONNECTOR_NAME, 0, NULL_HEADERS, null);

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartTaskOwnerRedirect() throws Throwable {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);

        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.restartTask(EasyMock.eq(taskId), EasyMock.capture(cb));
        String ownerUrl = "http://owner:8083";
        expectAndCallbackException(cb, new NotAssignedException("not owner test", ownerUrl));

        EasyMock.expect(RestClient.httpRequest(EasyMock.eq("http://owner:8083/connectors/" + CONNECTOR_NAME + "/tasks/0/restart?forward=false"),
                EasyMock.eq("POST"), EasyMock.isNull(), EasyMock.isNull(), EasyMock.anyObject(), EasyMock.anyObject(WorkerConfig.class)))
                .andReturn(new RestClient.HttpResponse<>(202, new HashMap<>(), null));

        PowerMock.replayAll();

        connectorsResource.restartTask(CONNECTOR_NAME, 0, NULL_HEADERS, true);

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorActiveTopicsWithTopicTrackingDisabled() {
        PowerMock.reset(workerConfig);
        EasyMock.expect(workerConfig.getBoolean(TOPIC_TRACKING_ENABLE_CONFIG)).andReturn(false);
        EasyMock.expect(workerConfig.getBoolean(TOPIC_TRACKING_ALLOW_RESET_CONFIG)).andReturn(false);
        PowerMock.replay(workerConfig);
        connectorsResource = new ConnectorsResource(herder, workerConfig);
        PowerMock.replayAll();

        Exception e = assertThrows(ConnectRestException.class,
            () -> connectorsResource.getConnectorActiveTopics(CONNECTOR_NAME));
        assertEquals("Topic tracking is disabled.", e.getMessage());
        PowerMock.verifyAll();
    }

    @Test
    public void testResetConnectorActiveTopicsWithTopicTrackingDisabled() {
        PowerMock.reset(workerConfig);
        EasyMock.expect(workerConfig.getBoolean(TOPIC_TRACKING_ENABLE_CONFIG)).andReturn(false);
        EasyMock.expect(workerConfig.getBoolean(TOPIC_TRACKING_ALLOW_RESET_CONFIG)).andReturn(true);
        HttpHeaders headers = EasyMock.mock(HttpHeaders.class);
        PowerMock.replay(workerConfig);
        connectorsResource = new ConnectorsResource(herder, workerConfig);
        PowerMock.replayAll();

        Exception e = assertThrows(ConnectRestException.class,
            () -> connectorsResource.resetConnectorActiveTopics(CONNECTOR_NAME, headers));
        assertEquals("Topic tracking is disabled.", e.getMessage());
        PowerMock.verifyAll();
    }

    @Test
    public void testResetConnectorActiveTopicsWithTopicTrackingEnabled() {
        PowerMock.reset(workerConfig);
        EasyMock.expect(workerConfig.getBoolean(TOPIC_TRACKING_ENABLE_CONFIG)).andReturn(true);
        EasyMock.expect(workerConfig.getBoolean(TOPIC_TRACKING_ALLOW_RESET_CONFIG)).andReturn(false);
        HttpHeaders headers = EasyMock.mock(HttpHeaders.class);
        PowerMock.replay(workerConfig);
        connectorsResource = new ConnectorsResource(herder, workerConfig);
        PowerMock.replayAll();

        Exception e = assertThrows(ConnectRestException.class,
            () -> connectorsResource.resetConnectorActiveTopics(CONNECTOR_NAME, headers));
        assertEquals("Topic tracking reset is disabled.", e.getMessage());
        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorActiveTopics() {
        PowerMock.reset(workerConfig);
        EasyMock.expect(workerConfig.getBoolean(TOPIC_TRACKING_ENABLE_CONFIG)).andReturn(true);
        EasyMock.expect(workerConfig.getBoolean(TOPIC_TRACKING_ALLOW_RESET_CONFIG)).andReturn(true);
        EasyMock.expect(herder.connectorActiveTopics(CONNECTOR_NAME))
                .andReturn(new ActiveTopicsInfo(CONNECTOR_NAME, CONNECTOR_ACTIVE_TOPICS));
        PowerMock.replay(workerConfig);
        connectorsResource = new ConnectorsResource(herder, workerConfig);
        PowerMock.replayAll();

        Response response = connectorsResource.getConnectorActiveTopics(CONNECTOR_NAME);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        Map<String, Map<String, Object>> body = (Map<String, Map<String, Object>>) response.getEntity();
        assertEquals(CONNECTOR_NAME, ((ActiveTopicsInfo) body.get(CONNECTOR_NAME)).connector());
        assertEquals(new HashSet<>(CONNECTOR_ACTIVE_TOPICS),
                ((ActiveTopicsInfo) body.get(CONNECTOR_NAME)).topics());
        PowerMock.verifyAll();
    }

    @Test
    public void testResetConnectorActiveTopics() {
        PowerMock.reset(workerConfig);
        EasyMock.expect(workerConfig.getBoolean(TOPIC_TRACKING_ENABLE_CONFIG)).andReturn(true);
        EasyMock.expect(workerConfig.getBoolean(TOPIC_TRACKING_ALLOW_RESET_CONFIG)).andReturn(true);
        HttpHeaders headers = EasyMock.mock(HttpHeaders.class);
        herder.resetConnectorActiveTopics(CONNECTOR_NAME);
        EasyMock.expectLastCall();
        PowerMock.replay(workerConfig);
        connectorsResource = new ConnectorsResource(herder, workerConfig);
        PowerMock.replayAll();

        Response response = connectorsResource.resetConnectorActiveTopics(CONNECTOR_NAME, headers);
        assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
        PowerMock.verifyAll();
    }

    @Test
    public void testCompleteOrForwardWithErrorAndNoForwardUrl() {
        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.deleteConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        String leaderUrl = null;
        expectAndCallbackException(cb, new NotLeaderException("not leader", leaderUrl));

        PowerMock.replayAll();

        ConnectRestException e = assertThrows(ConnectRestException.class, () ->
            connectorsResource.destroyConnector(CONNECTOR_NAME, NULL_HEADERS, FORWARD));
        assertTrue(e.getMessage().contains("no known leader URL"));
        PowerMock.verifyAll();
    }

    private <T> byte[] serializeAsBytes(final T value) throws IOException {
        return new ObjectMapper().writeValueAsBytes(value);
    }

    private  <T> void expectAndCallbackResult(final Capture<Callback<T>> cb, final T value) {
        PowerMock.expectLastCall().andAnswer(() -> {
            cb.getValue().onCompletion(null, value);
            return null;
        });
    }

    private  <T> void expectAndCallbackException(final Capture<Callback<T>> cb, final Throwable t) {
        PowerMock.expectLastCall().andAnswer((IAnswer<Void>) () -> {
            cb.getValue().onCompletion(t, null);
            return null;
        });
    }

    private  <T> void expectAndCallbackNotLeaderException(final Capture<Callback<T>> cb) {
        expectAndCallbackException(cb, new NotLeaderException("not leader test", LEADER_URL));
    }
}
