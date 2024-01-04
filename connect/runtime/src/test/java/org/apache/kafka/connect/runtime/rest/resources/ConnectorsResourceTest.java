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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.RestartRequest;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.distributed.NotAssignedException;
import org.apache.kafka.connect.runtime.distributed.NotLeaderException;
import org.apache.kafka.connect.runtime.distributed.RebalanceNeededException;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServerConfig;
import org.apache.kafka.connect.runtime.rest.entities.ActiveTopicsInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffset;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.rest.entities.Message;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Stubber;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
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
    private RestClient restClient;
    @Mock
    private RestServerConfig serverConfig;

    @Before
    public void setUp() throws NoSuchMethodException {
        when(serverConfig.topicTrackingEnabled()).thenReturn(true);
        when(serverConfig.topicTrackingResetEnabled()).thenReturn(true);
        connectorsResource = new ConnectorsResource(herder, serverConfig, restClient);
        forward = mock(UriInfo.class);
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
        queryParams.putSingle("forward", "true");
        when(forward.getQueryParameters()).thenReturn(queryParams);
    }

    @After
    public void teardown() {
        verifyNoMoreInteractions(herder);
    }

    private static Map<String, String> getConnectorConfig(Map<String, String> mapToClone) {
        Map<String, String> result = new HashMap<>(mapToClone);
        return result;
    }

    @Test
    public void testListConnectors() {
        when(herder.connectors()).thenReturn(Arrays.asList(CONNECTOR2_NAME, CONNECTOR_NAME));

        Collection<String> connectors = (Collection<String>) connectorsResource.listConnectors(forward, NULL_HEADERS).getEntity();
        // Ordering isn't guaranteed, compare sets
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_NAME, CONNECTOR2_NAME)), new HashSet<>(connectors));
    }

    @Test
    public void testExpandConnectorsStatus() {
        when(herder.connectors()).thenReturn(Arrays.asList(CONNECTOR2_NAME, CONNECTOR_NAME));
        ConnectorStateInfo connector = mock(ConnectorStateInfo.class);
        ConnectorStateInfo connector2 = mock(ConnectorStateInfo.class);
        when(herder.connectorStatus(CONNECTOR2_NAME)).thenReturn(connector2);
        when(herder.connectorStatus(CONNECTOR_NAME)).thenReturn(connector);

        forward = mock(UriInfo.class);
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
        queryParams.putSingle("expand", "status");
        when(forward.getQueryParameters()).thenReturn(queryParams);

        Map<String, Map<String, Object>> expanded = (Map<String, Map<String, Object>>) connectorsResource.listConnectors(forward, NULL_HEADERS).getEntity();
        // Ordering isn't guaranteed, compare sets
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_NAME, CONNECTOR2_NAME)), expanded.keySet());
        assertEquals(connector2, expanded.get(CONNECTOR2_NAME).get("status"));
        assertEquals(connector, expanded.get(CONNECTOR_NAME).get("status"));
    }

    @Test
    public void testExpandConnectorsInfo() {
        when(herder.connectors()).thenReturn(Arrays.asList(CONNECTOR2_NAME, CONNECTOR_NAME));
        ConnectorInfo connector = mock(ConnectorInfo.class);
        ConnectorInfo connector2 = mock(ConnectorInfo.class);
        when(herder.connectorInfo(CONNECTOR2_NAME)).thenReturn(connector2);
        when(herder.connectorInfo(CONNECTOR_NAME)).thenReturn(connector);

        forward = mock(UriInfo.class);
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
        queryParams.putSingle("expand", "info");
        when(forward.getQueryParameters()).thenReturn(queryParams);

        Map<String, Map<String, Object>> expanded = (Map<String, Map<String, Object>>) connectorsResource.listConnectors(forward, NULL_HEADERS).getEntity();
        // Ordering isn't guaranteed, compare sets
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_NAME, CONNECTOR2_NAME)), expanded.keySet());
        assertEquals(connector2, expanded.get(CONNECTOR2_NAME).get("info"));
        assertEquals(connector, expanded.get(CONNECTOR_NAME).get("info"));
    }

    @Test
    public void testFullExpandConnectors() {
        when(herder.connectors()).thenReturn(Arrays.asList(CONNECTOR2_NAME, CONNECTOR_NAME));
        ConnectorInfo connectorInfo = mock(ConnectorInfo.class);
        ConnectorInfo connectorInfo2 = mock(ConnectorInfo.class);
        when(herder.connectorInfo(CONNECTOR2_NAME)).thenReturn(connectorInfo2);
        when(herder.connectorInfo(CONNECTOR_NAME)).thenReturn(connectorInfo);
        ConnectorStateInfo connector = mock(ConnectorStateInfo.class);
        ConnectorStateInfo connector2 = mock(ConnectorStateInfo.class);
        when(herder.connectorStatus(CONNECTOR2_NAME)).thenReturn(connector2);
        when(herder.connectorStatus(CONNECTOR_NAME)).thenReturn(connector);

        forward = mock(UriInfo.class);
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
        queryParams.put("expand", Arrays.asList("info", "status"));
        when(forward.getQueryParameters()).thenReturn(queryParams);

        Map<String, Map<String, Object>> expanded = (Map<String, Map<String, Object>>) connectorsResource.listConnectors(forward, NULL_HEADERS).getEntity();
        // Ordering isn't guaranteed, compare sets
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_NAME, CONNECTOR2_NAME)), expanded.keySet());
        assertEquals(connectorInfo2, expanded.get(CONNECTOR2_NAME).get("info"));
        assertEquals(connectorInfo, expanded.get(CONNECTOR_NAME).get("info"));
        assertEquals(connector2, expanded.get(CONNECTOR2_NAME).get("status"));
        assertEquals(connector, expanded.get(CONNECTOR_NAME).get("status"));
    }

    @Test
    public void testExpandConnectorsWithConnectorNotFound() {
        when(herder.connectors()).thenReturn(Arrays.asList(CONNECTOR2_NAME, CONNECTOR_NAME));
        ConnectorStateInfo connector = mock(ConnectorStateInfo.class);
        ConnectorStateInfo connector2 = mock(ConnectorStateInfo.class);
        when(herder.connectorStatus(CONNECTOR2_NAME)).thenReturn(connector2);
        doThrow(mock(NotFoundException.class)).when(herder).connectorStatus(CONNECTOR_NAME);

        forward = mock(UriInfo.class);
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
        queryParams.putSingle("expand", "status");
        when(forward.getQueryParameters()).thenReturn(queryParams);

        Map<String, Map<String, Object>> expanded = (Map<String, Map<String, Object>>) connectorsResource.listConnectors(forward, NULL_HEADERS).getEntity();
        // Ordering isn't guaranteed, compare sets
        assertEquals(Collections.singleton(CONNECTOR2_NAME), expanded.keySet());
        assertEquals(connector2, expanded.get(CONNECTOR2_NAME).get("status"));
    }


    @Test
    public void testCreateConnector() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME,
            Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME), null);

        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG,
            CONNECTOR_TASK_NAMES, ConnectorType.SOURCE))
        ).when(herder).putConnectorConfig(eq(CONNECTOR_NAME), eq(body.config()), isNull(), eq(false), cb.capture());

        connectorsResource.createConnector(FORWARD, NULL_HEADERS, body);
    }

    @Test
    public void testCreateConnectorWithPausedInitialState() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME,
            Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME), CreateConnectorRequest.InitialState.PAUSED);

        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG,
            CONNECTOR_TASK_NAMES, ConnectorType.SOURCE))
        ).when(herder).putConnectorConfig(eq(CONNECTOR_NAME), eq(body.config()), eq(TargetState.PAUSED), eq(false), cb.capture());

        connectorsResource.createConnector(FORWARD, NULL_HEADERS, body);
    }

    @Test
    public void testCreateConnectorWithStoppedInitialState() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME,
            Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME), CreateConnectorRequest.InitialState.STOPPED);

        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG,
            CONNECTOR_TASK_NAMES, ConnectorType.SOURCE))
        ).when(herder).putConnectorConfig(eq(CONNECTOR_NAME), eq(body.config()), eq(TargetState.STOPPED), eq(false), cb.capture());

        connectorsResource.createConnector(FORWARD, NULL_HEADERS, body);
    }

    @Test
    public void testCreateConnectorWithRunningInitialState() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME,
            Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME), CreateConnectorRequest.InitialState.RUNNING);

        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG,
            CONNECTOR_TASK_NAMES, ConnectorType.SOURCE))
        ).when(herder).putConnectorConfig(eq(CONNECTOR_NAME), eq(body.config()), eq(TargetState.STARTED), eq(false), cb.capture());

        connectorsResource.createConnector(FORWARD, NULL_HEADERS, body);
    }

    @Test
    public void testCreateConnectorNotLeader() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME,
            Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME), null);

        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackNotLeaderException(cb).when(herder)
            .putConnectorConfig(eq(CONNECTOR_NAME), eq(body.config()), isNull(), eq(false), cb.capture());

        when(restClient.httpRequest(eq(LEADER_URL + "connectors?forward=false"), eq("POST"), isNull(), eq(body), any()))
                .thenReturn(new RestClient.HttpResponse<>(201, new HashMap<>(), new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG, CONNECTOR_TASK_NAMES, ConnectorType.SOURCE)));
        connectorsResource.createConnector(FORWARD, NULL_HEADERS, body);
    }

    @Test
    public void testCreateConnectorWithHeaders() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME,
            Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME), null);
        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        HttpHeaders httpHeaders = mock(HttpHeaders.class);
        expectAndCallbackNotLeaderException(cb)
            .when(herder).putConnectorConfig(eq(CONNECTOR_NAME), eq(body.config()), isNull(), eq(false), cb.capture());

        when(restClient.httpRequest(eq(LEADER_URL + "connectors?forward=false"), eq("POST"), eq(httpHeaders), any(), any()))
                .thenReturn(new RestClient.HttpResponse<>(202, new HashMap<>(), null));
        connectorsResource.createConnector(FORWARD, httpHeaders, body);
    }

    @Test
    public void testCreateConnectorExists() {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME,
            Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME), null);

        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackException(cb, new AlreadyExistsException("already exists"))
            .when(herder).putConnectorConfig(eq(CONNECTOR_NAME), eq(body.config()), isNull(), eq(false), cb.capture());
        assertThrows(AlreadyExistsException.class, () -> connectorsResource.createConnector(FORWARD, NULL_HEADERS, body));
    }

    @Test
    public void testCreateConnectorNameTrimWhitespaces() throws Throwable {
        // Clone CONNECTOR_CONFIG_WITHOUT_NAME Map, as createConnector changes it (puts the name in it) and this
        // will affect later tests
        Map<String, String> inputConfig = getConnectorConfig(CONNECTOR_CONFIG_WITHOUT_NAME);
        final CreateConnectorRequest bodyIn = new CreateConnectorRequest(CONNECTOR_NAME_PADDING_WHITESPACES, inputConfig, null);
        final CreateConnectorRequest bodyOut = new CreateConnectorRequest(CONNECTOR_NAME, CONNECTOR_CONFIG, null);

        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(bodyOut.name(), bodyOut.config(),
            CONNECTOR_TASK_NAMES, ConnectorType.SOURCE))
        ).when(herder).putConnectorConfig(eq(bodyOut.name()), eq(bodyOut.config()), isNull(), eq(false), cb.capture());

        connectorsResource.createConnector(FORWARD, NULL_HEADERS, bodyIn);
    }

    @Test
    public void testCreateConnectorNameAllWhitespaces() throws Throwable {
        // Clone CONNECTOR_CONFIG_WITHOUT_NAME Map, as createConnector changes it (puts the name in it) and this
        // will affect later tests
        Map<String, String> inputConfig = getConnectorConfig(CONNECTOR_CONFIG_WITHOUT_NAME);
        final CreateConnectorRequest bodyIn = new CreateConnectorRequest(CONNECTOR_NAME_ALL_WHITESPACES, inputConfig, null);
        final CreateConnectorRequest bodyOut = new CreateConnectorRequest("", CONNECTOR_CONFIG_WITH_EMPTY_NAME, null);

        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(bodyOut.name(), bodyOut.config(),
            CONNECTOR_TASK_NAMES, ConnectorType.SOURCE))
        ).when(herder).putConnectorConfig(eq(bodyOut.name()), eq(bodyOut.config()), isNull(), eq(false), cb.capture());

        connectorsResource.createConnector(FORWARD, NULL_HEADERS, bodyIn);
    }

    @Test
    public void testCreateConnectorNoName() throws Throwable {
        // Clone CONNECTOR_CONFIG_WITHOUT_NAME Map, as createConnector changes it (puts the name in it) and this
        // will affect later tests
        Map<String, String> inputConfig = getConnectorConfig(CONNECTOR_CONFIG_WITHOUT_NAME);
        final CreateConnectorRequest bodyIn = new CreateConnectorRequest(null, inputConfig, null);
        final CreateConnectorRequest bodyOut = new CreateConnectorRequest("", CONNECTOR_CONFIG_WITH_EMPTY_NAME, null);

        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(bodyOut.name(), bodyOut.config(),
            CONNECTOR_TASK_NAMES, ConnectorType.SOURCE))
        ).when(herder).putConnectorConfig(eq(bodyOut.name()), eq(bodyOut.config()), isNull(), eq(false), cb.capture());

        connectorsResource.createConnector(FORWARD, NULL_HEADERS, bodyIn);
    }

    @Test
    public void testDeleteConnector() throws Throwable {
        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, null).when(herder).deleteConnectorConfig(eq(CONNECTOR_NAME), cb.capture());

        connectorsResource.destroyConnector(CONNECTOR_NAME, NULL_HEADERS, FORWARD);
    }

    @Test
    public void testDeleteConnectorNotLeader() throws Throwable {
        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackNotLeaderException(cb).when(herder)
            .deleteConnectorConfig(eq(CONNECTOR_NAME), cb.capture());
        // Should forward request
        when(restClient.httpRequest(LEADER_URL + "connectors/" + CONNECTOR_NAME + "?forward=false", "DELETE", NULL_HEADERS, null, null))
                .thenReturn(new RestClient.HttpResponse<>(204, new HashMap<>(), null));
        connectorsResource.destroyConnector(CONNECTOR_NAME, NULL_HEADERS, FORWARD);
    }

    // Not found exceptions should pass through to caller so they can be processed for 404s
    @Test
    public void testDeleteConnectorNotFound() {
        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackException(cb, new NotFoundException("not found"))
            .when(herder).deleteConnectorConfig(eq(CONNECTOR_NAME), cb.capture());

        assertThrows(NotFoundException.class, () -> connectorsResource.destroyConnector(CONNECTOR_NAME, NULL_HEADERS, FORWARD));
    }

    @Test
    public void testGetConnector() throws Throwable {
        final ArgumentCaptor<Callback<ConnectorInfo>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG, CONNECTOR_TASK_NAMES, ConnectorType.SOURCE))
            .when(herder).connectorInfo(eq(CONNECTOR_NAME), cb.capture());

        ConnectorInfo connInfo = connectorsResource.getConnector(CONNECTOR_NAME);
        assertEquals(new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG, CONNECTOR_TASK_NAMES, ConnectorType.SOURCE),
            connInfo);
    }

    @Test
    public void testGetConnectorConfig() throws Throwable {
        final ArgumentCaptor<Callback<Map<String, String>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, CONNECTOR_CONFIG).when(herder).connectorConfig(eq(CONNECTOR_NAME), cb.capture());

        Map<String, String> connConfig = connectorsResource.getConnectorConfig(CONNECTOR_NAME);
        assertEquals(CONNECTOR_CONFIG, connConfig);
    }

    @Test
    public void testGetConnectorConfigConnectorNotFound() {
        final ArgumentCaptor<Callback<Map<String, String>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackException(cb, new NotFoundException("not found"))
            .when(herder).connectorConfig(eq(CONNECTOR_NAME), cb.capture());

        assertThrows(NotFoundException.class, () -> connectorsResource.getConnectorConfig(CONNECTOR_NAME));
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

        final ArgumentCaptor<Callback<Map<ConnectorTaskId, Map<String, String>>>> cb1 = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb1, expectedTasksConnector).when(herder).tasksConfig(eq(CONNECTOR_NAME), cb1.capture());
        final ArgumentCaptor<Callback<Map<ConnectorTaskId, Map<String, String>>>> cb2 = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb2, expectedTasksConnector2).when(herder).tasksConfig(eq(CONNECTOR2_NAME), cb2.capture());

        Map<ConnectorTaskId, Map<String, String>> tasksConfig = connectorsResource.getTasksConfig(CONNECTOR_NAME);
        assertEquals(expectedTasksConnector, tasksConfig);
        Map<ConnectorTaskId, Map<String, String>> tasksConfig2 = connectorsResource.getTasksConfig(CONNECTOR2_NAME);
        assertEquals(expectedTasksConnector2, tasksConfig2);
    }

    @Test
    public void testGetTasksConfigConnectorNotFound() throws Throwable {
        final ArgumentCaptor<Callback<Map<ConnectorTaskId, Map<String, String>>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackException(cb, new NotFoundException("not found"))
            .when(herder).tasksConfig(eq(CONNECTOR_NAME), cb.capture());

        assertThrows(NotFoundException.class, () ->
            connectorsResource.getTasksConfig(CONNECTOR_NAME));
    }

    @Test
    public void testPutConnectorConfig() throws Throwable {
        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, new Herder.Created<>(false, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG, CONNECTOR_TASK_NAMES,
            ConnectorType.SINK))
        ).when(herder).putConnectorConfig(eq(CONNECTOR_NAME), eq(CONNECTOR_CONFIG), eq(true), cb.capture());

        connectorsResource.putConnectorConfig(CONNECTOR_NAME, NULL_HEADERS, FORWARD, CONNECTOR_CONFIG);
    }

    @Test
    public void testCreateConnectorWithSpecialCharsInName() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME_SPECIAL_CHARS,
            Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME_SPECIAL_CHARS), null);

        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME_SPECIAL_CHARS, CONNECTOR_CONFIG,
            CONNECTOR_TASK_NAMES, ConnectorType.SOURCE))
        ).when(herder).putConnectorConfig(eq(CONNECTOR_NAME_SPECIAL_CHARS), eq(body.config()), isNull(), eq(false), cb.capture());

        String rspLocation = connectorsResource.createConnector(FORWARD, NULL_HEADERS, body).getLocation().toString();
        String decoded = new URI(rspLocation).getPath();
        Assert.assertEquals("/connectors/" + CONNECTOR_NAME_SPECIAL_CHARS, decoded);
    }

    @Test
    public void testCreateConnectorWithControlSequenceInName() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME_CONTROL_SEQUENCES1,
            Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME_CONTROL_SEQUENCES1), null);

        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME_CONTROL_SEQUENCES1, CONNECTOR_CONFIG,
            CONNECTOR_TASK_NAMES, ConnectorType.SOURCE))
        ).when(herder).putConnectorConfig(eq(CONNECTOR_NAME_CONTROL_SEQUENCES1), eq(body.config()), isNull(), eq(false), cb.capture());

        String rspLocation = connectorsResource.createConnector(FORWARD, NULL_HEADERS, body).getLocation().toString();
        String decoded = new URI(rspLocation).getPath();
        Assert.assertEquals("/connectors/" + CONNECTOR_NAME_CONTROL_SEQUENCES1, decoded);
    }

    @Test
    public void testPutConnectorConfigWithSpecialCharsInName() throws Throwable {
        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);

        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME_SPECIAL_CHARS, CONNECTOR_CONFIG_SPECIAL_CHARS, CONNECTOR_TASK_NAMES,
            ConnectorType.SINK))
        ).when(herder).putConnectorConfig(eq(CONNECTOR_NAME_SPECIAL_CHARS), eq(CONNECTOR_CONFIG_SPECIAL_CHARS), eq(true), cb.capture());

        String rspLocation = connectorsResource.putConnectorConfig(CONNECTOR_NAME_SPECIAL_CHARS, NULL_HEADERS, FORWARD, CONNECTOR_CONFIG_SPECIAL_CHARS).getLocation().toString();
        String decoded = new URI(rspLocation).getPath();
        Assert.assertEquals("/connectors/" + CONNECTOR_NAME_SPECIAL_CHARS, decoded);
    }

    @Test
    public void testPutConnectorConfigWithControlSequenceInName() throws Throwable {
        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);

        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME_CONTROL_SEQUENCES1, CONNECTOR_CONFIG_CONTROL_SEQUENCES, CONNECTOR_TASK_NAMES,
            ConnectorType.SINK))
        ).when(herder).putConnectorConfig(eq(CONNECTOR_NAME_CONTROL_SEQUENCES1), eq(CONNECTOR_CONFIG_CONTROL_SEQUENCES), eq(true), cb.capture());

        String rspLocation = connectorsResource.putConnectorConfig(CONNECTOR_NAME_CONTROL_SEQUENCES1, NULL_HEADERS, FORWARD, CONNECTOR_CONFIG_CONTROL_SEQUENCES).getLocation().toString();
        String decoded = new URI(rspLocation).getPath();
        Assert.assertEquals("/connectors/" + CONNECTOR_NAME_CONTROL_SEQUENCES1, decoded);
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
        CreateConnectorRequest request = new CreateConnectorRequest(CONNECTOR_NAME, connConfig, null);
        assertThrows(BadRequestException.class, () -> connectorsResource.createConnector(FORWARD, NULL_HEADERS, request));
    }

    @Test
    public void testGetConnectorTaskConfigs() throws Throwable {
        final ArgumentCaptor<Callback<List<TaskInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, TASK_INFOS).when(herder).taskConfigs(eq(CONNECTOR_NAME), cb.capture());

        List<TaskInfo> taskInfos = connectorsResource.getTaskConfigs(CONNECTOR_NAME);
        assertEquals(TASK_INFOS, taskInfos);
    }

    @Test
    public void testGetConnectorTaskConfigsConnectorNotFound() {
        final ArgumentCaptor<Callback<List<TaskInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackException(cb, new NotFoundException("connector not found"))
            .when(herder).taskConfigs(eq(CONNECTOR_NAME), cb.capture());

        assertThrows(NotFoundException.class, () -> connectorsResource.getTaskConfigs(CONNECTOR_NAME));
    }

    @Test
    public void testRestartConnectorAndTasksConnectorNotFound() {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, true, false);
        final ArgumentCaptor<Callback<ConnectorStateInfo>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackException(cb, new NotFoundException("not found"))
            .when(herder).restartConnectorAndTasks(eq(restartRequest), cb.capture());

        assertThrows(NotFoundException.class, () ->
                connectorsResource.restartConnector(CONNECTOR_NAME, NULL_HEADERS, restartRequest.includeTasks(), restartRequest.onlyFailed(), FORWARD)
        );
    }

    @Test
    public void testRestartConnectorAndTasksLeaderRedirect() throws Throwable {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, true, false);
        final ArgumentCaptor<Callback<ConnectorStateInfo>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackNotLeaderException(cb).when(herder)
            .restartConnectorAndTasks(eq(restartRequest), cb.capture());

        when(restClient.httpRequest(eq(LEADER_URL + "connectors/" + CONNECTOR_NAME + "/restart?forward=true&includeTasks=" + restartRequest.includeTasks() + "&onlyFailed=" + restartRequest.onlyFailed()), eq("POST"), isNull(), isNull(), any()))
                .thenReturn(new RestClient.HttpResponse<>(202, new HashMap<>(), null));
        Response response = connectorsResource.restartConnector(CONNECTOR_NAME, NULL_HEADERS, restartRequest.includeTasks(), restartRequest.onlyFailed(), null);
        assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    }

    @Test
    public void testRestartConnectorAndTasksRebalanceNeeded() {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, true, false);
        final ArgumentCaptor<Callback<ConnectorStateInfo>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackException(cb, new RebalanceNeededException("Request cannot be completed because a rebalance is expected"))
            .when(herder).restartConnectorAndTasks(eq(restartRequest), cb.capture());

        ConnectRestException ex = assertThrows(ConnectRestException.class, () ->
                connectorsResource.restartConnector(CONNECTOR_NAME, NULL_HEADERS, restartRequest.includeTasks(), restartRequest.onlyFailed(), FORWARD)
        );
        assertEquals(Response.Status.CONFLICT.getStatusCode(), ex.statusCode());
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
        final ArgumentCaptor<Callback<ConnectorStateInfo>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackResult(cb, connectorStateInfo)
            .when(herder).restartConnectorAndTasks(eq(restartRequest), cb.capture());

        Response response = connectorsResource.restartConnector(CONNECTOR_NAME, NULL_HEADERS, restartRequest.includeTasks(), restartRequest.onlyFailed(), FORWARD);
        assertEquals(CONNECTOR_NAME, ((ConnectorStateInfo) response.getEntity()).name());
        assertEquals(state.state(), ((ConnectorStateInfo) response.getEntity()).connector().state());
        assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    }

    @Test
    public void testRestartConnectorNotFound() {
        final ArgumentCaptor<Callback<Void>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackException(cb, new NotFoundException("not found"))
            .when(herder).restartConnector(eq(CONNECTOR_NAME), cb.capture());

        assertThrows(NotFoundException.class, () ->
                connectorsResource.restartConnector(CONNECTOR_NAME, NULL_HEADERS, false, false, FORWARD)
        );
    }

    @Test
    public void testRestartConnectorLeaderRedirect() throws Throwable {
        final ArgumentCaptor<Callback<Void>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackNotLeaderException(cb).when(herder)
            .restartConnector(eq(CONNECTOR_NAME), cb.capture());

        when(restClient.httpRequest(eq(LEADER_URL + "connectors/" + CONNECTOR_NAME + "/restart?forward=true"), eq("POST"), isNull(), isNull(), any()))
                .thenReturn(new RestClient.HttpResponse<>(202, new HashMap<>(), null));
        Response response = connectorsResource.restartConnector(CONNECTOR_NAME, NULL_HEADERS, false, false, null);
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    }

    @Test
    public void testRestartConnectorOwnerRedirect() throws Throwable {
        final ArgumentCaptor<Callback<Void>> cb = ArgumentCaptor.forClass(Callback.class);
        String ownerUrl = "http://owner:8083";
        expectAndCallbackException(cb, new NotAssignedException("not owner test", ownerUrl))
            .when(herder).restartConnector(eq(CONNECTOR_NAME), cb.capture());
        when(restClient.httpRequest(eq("http://owner:8083/connectors/" + CONNECTOR_NAME + "/restart?forward=false"), eq("POST"), isNull(), isNull(), any()))
                .thenReturn(new RestClient.HttpResponse<>(202, new HashMap<>(), null));
        Response response = connectorsResource.restartConnector(CONNECTOR_NAME, NULL_HEADERS, false, false, true);
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    }

    @Test
    public void testRestartTaskNotFound() {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        final ArgumentCaptor<Callback<Void>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackException(cb, new NotFoundException("not found"))
            .when(herder).restartTask(eq(taskId), cb.capture());

        assertThrows(NotFoundException.class, () -> connectorsResource.restartTask(CONNECTOR_NAME, 0, NULL_HEADERS, FORWARD));
    }

    @Test
    public void testRestartTaskLeaderRedirect() throws Throwable {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);

        final ArgumentCaptor<Callback<Void>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackNotLeaderException(cb).when(herder)
            .restartTask(eq(taskId), cb.capture());

        when(restClient.httpRequest(eq(LEADER_URL + "connectors/" + CONNECTOR_NAME + "/tasks/0/restart?forward=true"), eq("POST"), isNull(), isNull(), any()))
                .thenReturn(new RestClient.HttpResponse<>(202, new HashMap<>(), null));
        connectorsResource.restartTask(CONNECTOR_NAME, 0, NULL_HEADERS, null);
    }

    @Test
    public void testRestartTaskOwnerRedirect() throws Throwable {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);

        final ArgumentCaptor<Callback<Void>> cb = ArgumentCaptor.forClass(Callback.class);
        String ownerUrl = "http://owner:8083";
        expectAndCallbackException(cb, new NotAssignedException("not owner test", ownerUrl))
            .when(herder).restartTask(eq(taskId), cb.capture());

        when(restClient.httpRequest(eq("http://owner:8083/connectors/" + CONNECTOR_NAME + "/tasks/0/restart?forward=false"), eq("POST"), isNull(), isNull(), any()))
                .thenReturn(new RestClient.HttpResponse<>(202, new HashMap<>(), null));
        connectorsResource.restartTask(CONNECTOR_NAME, 0, NULL_HEADERS, true);
    }

    @Test
    public void testConnectorActiveTopicsWithTopicTrackingDisabled() {
        when(serverConfig.topicTrackingEnabled()).thenReturn(false);
        when(serverConfig.topicTrackingResetEnabled()).thenReturn(false);
        connectorsResource = new ConnectorsResource(herder, serverConfig, restClient);

        Exception e = assertThrows(ConnectRestException.class,
            () -> connectorsResource.getConnectorActiveTopics(CONNECTOR_NAME));
        assertEquals("Topic tracking is disabled.", e.getMessage());
    }

    @Test
    public void testResetConnectorActiveTopicsWithTopicTrackingDisabled() {
        when(serverConfig.topicTrackingEnabled()).thenReturn(false);
        when(serverConfig.topicTrackingResetEnabled()).thenReturn(true);
        HttpHeaders headers = mock(HttpHeaders.class);
        connectorsResource = new ConnectorsResource(herder, serverConfig, restClient);

        Exception e = assertThrows(ConnectRestException.class,
            () -> connectorsResource.resetConnectorActiveTopics(CONNECTOR_NAME, headers));
        assertEquals("Topic tracking is disabled.", e.getMessage());
    }

    @Test
    public void testResetConnectorActiveTopicsWithTopicTrackingEnabled() {
        when(serverConfig.topicTrackingEnabled()).thenReturn(true);
        when(serverConfig.topicTrackingResetEnabled()).thenReturn(false);
        HttpHeaders headers = mock(HttpHeaders.class);
        connectorsResource = new ConnectorsResource(herder, serverConfig, restClient);

        Exception e = assertThrows(ConnectRestException.class,
            () -> connectorsResource.resetConnectorActiveTopics(CONNECTOR_NAME, headers));
        assertEquals("Topic tracking reset is disabled.", e.getMessage());
    }

    @Test
    public void testConnectorActiveTopics() {
        when(serverConfig.topicTrackingEnabled()).thenReturn(true);
        when(serverConfig.topicTrackingResetEnabled()).thenReturn(true);
        when(herder.connectorActiveTopics(CONNECTOR_NAME))
            .thenReturn(new ActiveTopicsInfo(CONNECTOR_NAME, CONNECTOR_ACTIVE_TOPICS));
        connectorsResource = new ConnectorsResource(herder, serverConfig, restClient);

        Response response = connectorsResource.getConnectorActiveTopics(CONNECTOR_NAME);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        Map<String, Map<String, Object>> body = (Map<String, Map<String, Object>>) response.getEntity();
        assertEquals(CONNECTOR_NAME, ((ActiveTopicsInfo) body.get(CONNECTOR_NAME)).connector());
        assertEquals(new HashSet<>(CONNECTOR_ACTIVE_TOPICS),
                ((ActiveTopicsInfo) body.get(CONNECTOR_NAME)).topics());
    }

    @Test
    public void testResetConnectorActiveTopics() {
        HttpHeaders headers = mock(HttpHeaders.class);
        connectorsResource = new ConnectorsResource(herder, serverConfig, restClient);

        Response response = connectorsResource.resetConnectorActiveTopics(CONNECTOR_NAME, headers);
        verify(herder).resetConnectorActiveTopics(CONNECTOR_NAME);
        assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    }

    @Test
    public void testCompleteOrForwardWithErrorAndNoForwardUrl() {
        final ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> cb = ArgumentCaptor.forClass(Callback.class);
        String leaderUrl = null;
        expectAndCallbackException(cb, new NotLeaderException("not leader", leaderUrl))
            .when(herder).deleteConnectorConfig(eq(CONNECTOR_NAME), cb.capture());

        ConnectRestException e = assertThrows(ConnectRestException.class, () ->
            connectorsResource.destroyConnector(CONNECTOR_NAME, NULL_HEADERS, FORWARD));
        assertTrue(e.getMessage().contains("no known leader URL"));
    }

    @Test
    public void testGetOffsetsConnectorNotFound() {
        final ArgumentCaptor<Callback<ConnectorOffsets>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackException(cb, new NotFoundException("Connector not found"))
                .when(herder).connectorOffsets(anyString(), cb.capture());

        assertThrows(NotFoundException.class, () -> connectorsResource.getOffsets("unknown-connector"));
    }

    @Test
    public void testGetOffsets() throws Throwable {
        final ArgumentCaptor<Callback<ConnectorOffsets>> cb = ArgumentCaptor.forClass(Callback.class);
        ConnectorOffsets offsets = new ConnectorOffsets(Arrays.asList(
                new ConnectorOffset(Collections.singletonMap("partitionKey", "partitionValue"), Collections.singletonMap("offsetKey", "offsetValue")),
                new ConnectorOffset(Collections.singletonMap("partitionKey", "partitionValue2"), Collections.singletonMap("offsetKey", "offsetValue"))
        ));
        expectAndCallbackResult(cb, offsets).when(herder).connectorOffsets(eq(CONNECTOR_NAME), cb.capture());

        assertEquals(offsets, connectorsResource.getOffsets(CONNECTOR_NAME));
    }

    @Test
    public void testAlterOffsetsEmptyOffsets() {
        assertThrows(BadRequestException.class, () -> connectorsResource.alterConnectorOffsets(
                false, NULL_HEADERS, CONNECTOR_NAME, new ConnectorOffsets(Collections.emptyList())));
    }

    @Test
    public void testAlterOffsetsNotLeader() throws Throwable {
        Map<String, ?> partition = new HashMap<>();
        Map<String, ?> offset = new HashMap<>();
        ConnectorOffset connectorOffset = new ConnectorOffset(partition, offset);
        ConnectorOffsets body = new ConnectorOffsets(Collections.singletonList(connectorOffset));

        final ArgumentCaptor<Callback<Message>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackNotLeaderException(cb).when(herder).alterConnectorOffsets(eq(CONNECTOR_NAME), eq(body.toMap()), cb.capture());

        when(restClient.httpRequest(eq(LEADER_URL + "connectors/" + CONNECTOR_NAME + "/offsets?forward=true"), eq("PATCH"), isNull(), eq(body), any()))
                .thenReturn(new RestClient.HttpResponse<>(200, new HashMap<>(), new Message("")));
        connectorsResource.alterConnectorOffsets(null, NULL_HEADERS, CONNECTOR_NAME, body);
    }

    @Test
    public void testAlterOffsetsConnectorNotFound() {
        Map<String, ?> partition = new HashMap<>();
        Map<String, ?> offset = new HashMap<>();
        ConnectorOffset connectorOffset = new ConnectorOffset(partition, offset);
        ConnectorOffsets body = new ConnectorOffsets(Collections.singletonList(connectorOffset));
        final ArgumentCaptor<Callback<Message>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackException(cb, new NotFoundException("Connector not found"))
                .when(herder).alterConnectorOffsets(eq(CONNECTOR_NAME), eq(body.toMap()), cb.capture());

        assertThrows(NotFoundException.class, () -> connectorsResource.alterConnectorOffsets(null, NULL_HEADERS, CONNECTOR_NAME, body));
    }

    @Test
    public void testAlterOffsets() throws Throwable {
        Map<String, ?> partition = Collections.singletonMap("partitionKey", "partitionValue");
        Map<String, ?> offset = Collections.singletonMap("offsetKey", "offsetValue");
        ConnectorOffset connectorOffset = new ConnectorOffset(partition, offset);
        ConnectorOffsets body = new ConnectorOffsets(Collections.singletonList(connectorOffset));

        final ArgumentCaptor<Callback<Message>> cb = ArgumentCaptor.forClass(Callback.class);
        Message msg = new Message("The offsets for this connector have been altered successfully");
        doAnswer(invocation -> {
            cb.getValue().onCompletion(null, msg);
            return null;
        }).when(herder).alterConnectorOffsets(eq(CONNECTOR_NAME), eq(body.toMap()), cb.capture());
        Response response = connectorsResource.alterConnectorOffsets(null, NULL_HEADERS, CONNECTOR_NAME, body);
        assertEquals(200, response.getStatus());
        assertEquals(msg, response.getEntity());
    }

    @Test
    public void testResetOffsetsNotLeader() throws Throwable {
        final ArgumentCaptor<Callback<Message>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackNotLeaderException(cb).when(herder).resetConnectorOffsets(eq(CONNECTOR_NAME), cb.capture());

        when(restClient.httpRequest(eq(LEADER_URL + "connectors/" + CONNECTOR_NAME + "/offsets?forward=true"), eq("DELETE"), isNull(), isNull(), any()))
                .thenReturn(new RestClient.HttpResponse<>(200, new HashMap<>(), new Message("")));
        connectorsResource.resetConnectorOffsets(null, NULL_HEADERS, CONNECTOR_NAME);
    }

    @Test
    public void testResetOffsetsConnectorNotFound() {
        final ArgumentCaptor<Callback<Message>> cb = ArgumentCaptor.forClass(Callback.class);
        expectAndCallbackException(cb, new NotFoundException("Connector not found"))
                .when(herder).resetConnectorOffsets(eq(CONNECTOR_NAME), cb.capture());

        assertThrows(NotFoundException.class, () -> connectorsResource.resetConnectorOffsets(null, NULL_HEADERS, CONNECTOR_NAME));
    }

    @Test
    public void testResetOffsets() throws Throwable {
        final ArgumentCaptor<Callback<Message>> cb = ArgumentCaptor.forClass(Callback.class);
        Message msg = new Message("The offsets for this connector have been reset successfully");
        doAnswer(invocation -> {
            cb.getValue().onCompletion(null, msg);
            return null;
        }).when(herder).resetConnectorOffsets(eq(CONNECTOR_NAME), cb.capture());
        Response response = connectorsResource.resetConnectorOffsets(null, NULL_HEADERS, CONNECTOR_NAME);
        assertEquals(200, response.getStatus());
        assertEquals(msg, response.getEntity());
    }

    private <T> byte[] serializeAsBytes(final T value) throws IOException {
        return new ObjectMapper().writeValueAsBytes(value);
    }

    private <T> Stubber expectAndCallbackResult(final ArgumentCaptor<Callback<T>> cb, final T value) {
        return doAnswer(invocation -> {
            cb.getValue().onCompletion(null, value);
            return null;
        });
    }

    private <T> Stubber expectAndCallbackException(final ArgumentCaptor<Callback<T>> cb, final Throwable t) {
        return doAnswer(invocation -> {
            cb.getValue().onCompletion(t, null);
            return null;
        });
    }

    private <T> Stubber expectAndCallbackNotLeaderException(final ArgumentCaptor<Callback<T>> cb) {
        return expectAndCallbackException(cb, new NotLeaderException("not leader test", LEADER_URL));
    }

    @FunctionalInterface
    public interface RunnableWithThrowable<T> {
        T run() throws Throwable;
    }

}
