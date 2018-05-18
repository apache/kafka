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

import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.NotAssignedException;
import org.apache.kafka.connect.runtime.distributed.NotLeaderException;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
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
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest(RestClient.class)
@PowerMockIgnore("javax.management.*")
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

    @Mock
    private Herder herder;
    private ConnectorsResource connectorsResource;

    @Before
    public void setUp() throws NoSuchMethodException {
        PowerMock.mockStatic(RestClient.class,
                RestClient.class.getMethod("httpRequest", String.class, String.class, Object.class, TypeReference.class, WorkerConfig.class));
        connectorsResource = new ConnectorsResource(herder, null);
    }

    private static final Map<String, String> getConnectorConfig(Map<String, String> mapToClone) {
        Map<String, String> result = new HashMap<>(mapToClone);
        return result;
    }

    @Test
    public void testListConnectors() throws Throwable {
        final Capture<Callback<Collection<String>>> cb = Capture.newInstance();
        herder.connectors(EasyMock.capture(cb));
        expectAndCallbackResult(cb, Arrays.asList(CONNECTOR2_NAME, CONNECTOR_NAME));

        PowerMock.replayAll();

        Collection<String> connectors = connectorsResource.listConnectors(FORWARD);
        // Ordering isn't guaranteed, compare sets
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_NAME, CONNECTOR2_NAME)), new HashSet<>(connectors));

        PowerMock.verifyAll();
    }

    @Test
    public void testListConnectorsNotLeader() throws Throwable {
        final Capture<Callback<Collection<String>>> cb = Capture.newInstance();
        herder.connectors(EasyMock.capture(cb));
        expectAndCallbackNotLeaderException(cb);
        // Should forward request
        EasyMock.expect(RestClient.httpRequest(EasyMock.eq("http://leader:8083/connectors?forward=false"), EasyMock.eq("GET"),
                EasyMock.isNull(), EasyMock.anyObject(TypeReference.class), EasyMock.anyObject(WorkerConfig.class)))
                .andReturn(new RestClient.HttpResponse<>(200, new HashMap<String, String>(), Arrays.asList(CONNECTOR2_NAME, CONNECTOR_NAME)));

        PowerMock.replayAll();

        Collection<String> connectors = connectorsResource.listConnectors(FORWARD);
        // Ordering isn't guaranteed, compare sets
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_NAME, CONNECTOR2_NAME)), new HashSet<>(connectors));

        PowerMock.verifyAll();
    }

    @Test(expected = ConnectException.class)
    public void testListConnectorsNotSynced() throws Throwable {
        final Capture<Callback<Collection<String>>> cb = Capture.newInstance();
        herder.connectors(EasyMock.capture(cb));
        expectAndCallbackException(cb, new ConnectException("not synced"));

        PowerMock.replayAll();

        // throws
        connectorsResource.listConnectors(FORWARD);
    }

    @Test
    public void testCreateConnector() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME, Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME));

        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(body.config()), EasyMock.eq(false), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new Herder.Created<>(true, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG,
            CONNECTOR_TASK_NAMES, ConnectorType.SOURCE)));

        PowerMock.replayAll();

        connectorsResource.createConnector(FORWARD, body);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorNotLeader() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME, Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME));

        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(body.config()), EasyMock.eq(false), EasyMock.capture(cb));
        expectAndCallbackNotLeaderException(cb);
        // Should forward request
        EasyMock.expect(RestClient.httpRequest(EasyMock.eq("http://leader:8083/connectors?forward=false"), EasyMock.eq("POST"), EasyMock.eq(body), EasyMock.<TypeReference>anyObject(), EasyMock.anyObject(WorkerConfig.class)))
                .andReturn(new RestClient.HttpResponse<>(201, new HashMap<String, String>(), new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG, CONNECTOR_TASK_NAMES,
                    ConnectorType.SOURCE)));

        PowerMock.replayAll();

        connectorsResource.createConnector(FORWARD, body);

        PowerMock.verifyAll();


    }

    @Test(expected = AlreadyExistsException.class)
    public void testCreateConnectorExists() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME, Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME));

        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(body.config()), EasyMock.eq(false), EasyMock.capture(cb));
        expectAndCallbackException(cb, new AlreadyExistsException("already exists"));

        PowerMock.replayAll();

        connectorsResource.createConnector(FORWARD, body);

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

        connectorsResource.createConnector(FORWARD, bodyIn);

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

        connectorsResource.createConnector(FORWARD, bodyIn);

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

        connectorsResource.createConnector(FORWARD, bodyIn);

        PowerMock.verifyAll();
    }

    @Test
    public void testDeleteConnector() throws Throwable {
        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.deleteConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackResult(cb, null);

        PowerMock.replayAll();

        connectorsResource.destroyConnector(CONNECTOR_NAME, FORWARD);

        PowerMock.verifyAll();
    }

    @Test
    public void testDeleteConnectorNotLeader() throws Throwable {
        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.deleteConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackNotLeaderException(cb);
        // Should forward request
        EasyMock.expect(RestClient.httpRequest("http://leader:8083/connectors/" + CONNECTOR_NAME + "?forward=false", "DELETE", null, null, null))
                .andReturn(new RestClient.HttpResponse<>(204, new HashMap<String, String>(), null));

        PowerMock.replayAll();

        connectorsResource.destroyConnector(CONNECTOR_NAME, FORWARD);

        PowerMock.verifyAll();
    }

    // Not found exceptions should pass through to caller so they can be processed for 404s
    @Test(expected = NotFoundException.class)
    public void testDeleteConnectorNotFound() throws Throwable {
        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.deleteConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        connectorsResource.destroyConnector(CONNECTOR_NAME, FORWARD);

        PowerMock.verifyAll();
    }

    @Test
    public void testGetConnector() throws Throwable {
        final Capture<Callback<ConnectorInfo>> cb = Capture.newInstance();
        herder.connectorInfo(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG, CONNECTOR_TASK_NAMES,
            ConnectorType.SOURCE));

        PowerMock.replayAll();

        ConnectorInfo connInfo = connectorsResource.getConnector(CONNECTOR_NAME, FORWARD);
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

        Map<String, String> connConfig = connectorsResource.getConnectorConfig(CONNECTOR_NAME, FORWARD);
        assertEquals(CONNECTOR_CONFIG, connConfig);

        PowerMock.verifyAll();
    }

    @Test(expected = NotFoundException.class)
    public void testGetConnectorConfigConnectorNotFound() throws Throwable {
        final Capture<Callback<Map<String, String>>> cb = Capture.newInstance();
        herder.connectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        connectorsResource.getConnectorConfig(CONNECTOR_NAME, FORWARD);

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorConfig() throws Throwable {
        final Capture<Callback<Herder.Created<ConnectorInfo>>> cb = Capture.newInstance();
        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(CONNECTOR_CONFIG), EasyMock.eq(true), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new Herder.Created<>(false, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG, CONNECTOR_TASK_NAMES,
            ConnectorType.SINK)));

        PowerMock.replayAll();

        connectorsResource.putConnectorConfig(CONNECTOR_NAME, FORWARD, CONNECTOR_CONFIG);

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

        String rspLocation = connectorsResource.createConnector(FORWARD, body).getLocation().toString();
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

        String rspLocation = connectorsResource.createConnector(FORWARD, body).getLocation().toString();
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

        String rspLocation = connectorsResource.putConnectorConfig(CONNECTOR_NAME_SPECIAL_CHARS, FORWARD, CONNECTOR_CONFIG_SPECIAL_CHARS).getLocation().toString();
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

        String rspLocation = connectorsResource.putConnectorConfig(CONNECTOR_NAME_CONTROL_SEQUENCES1, FORWARD, CONNECTOR_CONFIG_CONTROL_SEQUENCES).getLocation().toString();
        String decoded = new URI(rspLocation).getPath();
        Assert.assertEquals("/connectors/" + CONNECTOR_NAME_CONTROL_SEQUENCES1, decoded);

        PowerMock.verifyAll();
    }

    @Test(expected = BadRequestException.class)
    public void testPutConnectorConfigNameMismatch() throws Throwable {
        Map<String, String> connConfig = new HashMap<>(CONNECTOR_CONFIG);
        connConfig.put(ConnectorConfig.NAME_CONFIG, "mismatched-name");
        connectorsResource.putConnectorConfig(CONNECTOR_NAME, FORWARD, connConfig);
    }

    @Test(expected = BadRequestException.class)
    public void testCreateConnectorConfigNameMismatch() throws Throwable {
        Map<String, String> connConfig = new HashMap<>();
        connConfig.put(ConnectorConfig.NAME_CONFIG, "mismatched-name");
        CreateConnectorRequest request = new CreateConnectorRequest(CONNECTOR_NAME, connConfig);
        connectorsResource.createConnector(FORWARD, request);
    }

    @Test
    public void testGetConnectorTaskConfigs() throws Throwable {
        final Capture<Callback<List<TaskInfo>>> cb = Capture.newInstance();
        herder.taskConfigs(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackResult(cb, TASK_INFOS);

        PowerMock.replayAll();

        List<TaskInfo> taskInfos = connectorsResource.getTaskConfigs(CONNECTOR_NAME, FORWARD);
        assertEquals(TASK_INFOS, taskInfos);

        PowerMock.verifyAll();
    }

    @Test(expected = NotFoundException.class)
    public void testGetConnectorTaskConfigsConnectorNotFound() throws Throwable {
        final Capture<Callback<List<TaskInfo>>> cb = Capture.newInstance();
        herder.taskConfigs(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("connector not found"));

        PowerMock.replayAll();

        connectorsResource.getTaskConfigs(CONNECTOR_NAME, FORWARD);

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorTaskConfigs() throws Throwable {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.putTaskConfigs(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(TASK_CONFIGS), EasyMock.capture(cb));
        expectAndCallbackResult(cb, null);

        PowerMock.replayAll();

        connectorsResource.putTaskConfigs(CONNECTOR_NAME, FORWARD, TASK_CONFIGS);

        PowerMock.verifyAll();
    }

    @Test(expected = NotFoundException.class)
    public void testPutConnectorTaskConfigsConnectorNotFound() throws Throwable {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.putTaskConfigs(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(TASK_CONFIGS), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        connectorsResource.putTaskConfigs(CONNECTOR_NAME, FORWARD, TASK_CONFIGS);

        PowerMock.verifyAll();
    }

    @Test(expected = NotFoundException.class)
    public void testRestartConnectorNotFound() throws Throwable {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.restartConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        connectorsResource.restartConnector(CONNECTOR_NAME, FORWARD);

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorLeaderRedirect() throws Throwable {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.restartConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackNotLeaderException(cb);

        EasyMock.expect(RestClient.httpRequest(EasyMock.eq("http://leader:8083/connectors/" + CONNECTOR_NAME + "/restart?forward=true"),
                EasyMock.eq("POST"), EasyMock.isNull(), EasyMock.<TypeReference>anyObject(), EasyMock.anyObject(WorkerConfig.class)))
                .andReturn(new RestClient.HttpResponse<>(202, new HashMap<String, String>(), null));

        PowerMock.replayAll();

        connectorsResource.restartConnector(CONNECTOR_NAME, null);

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartConnectorOwnerRedirect() throws Throwable {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.restartConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        String ownerUrl = "http://owner:8083";
        expectAndCallbackException(cb, new NotAssignedException("not owner test", ownerUrl));

        EasyMock.expect(RestClient.httpRequest(EasyMock.eq("http://owner:8083/connectors/" + CONNECTOR_NAME + "/restart?forward=false"),
                EasyMock.eq("POST"), EasyMock.isNull(), EasyMock.<TypeReference>anyObject(), EasyMock.anyObject(WorkerConfig.class)))
                .andReturn(new RestClient.HttpResponse<>(202, new HashMap<String, String>(), null));

        PowerMock.replayAll();

        connectorsResource.restartConnector(CONNECTOR_NAME, true);

        PowerMock.verifyAll();
    }

    @Test(expected = NotFoundException.class)
    public void testRestartTaskNotFound() throws Throwable {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.restartTask(EasyMock.eq(taskId), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        connectorsResource.restartTask(CONNECTOR_NAME, 0, FORWARD);

        PowerMock.verifyAll();
    }

    @Test
    public void testRestartTaskLeaderRedirect() throws Throwable {
        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, 0);

        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.restartTask(EasyMock.eq(taskId), EasyMock.capture(cb));
        expectAndCallbackNotLeaderException(cb);

        EasyMock.expect(RestClient.httpRequest(EasyMock.eq("http://leader:8083/connectors/" + CONNECTOR_NAME + "/tasks/0/restart?forward=true"),
                EasyMock.eq("POST"), EasyMock.isNull(), EasyMock.<TypeReference>anyObject(), EasyMock.anyObject(WorkerConfig.class)))
                .andReturn(new RestClient.HttpResponse<>(202, new HashMap<String, String>(), null));

        PowerMock.replayAll();

        connectorsResource.restartTask(CONNECTOR_NAME, 0, null);

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
                EasyMock.eq("POST"), EasyMock.isNull(), EasyMock.<TypeReference>anyObject(), EasyMock.anyObject(WorkerConfig.class)))
                .andReturn(new RestClient.HttpResponse<>(202, new HashMap<String, String>(), null));

        PowerMock.replayAll();

        connectorsResource.restartTask(CONNECTOR_NAME, 0, true);

        PowerMock.verifyAll();
    }

    private  <T> void expectAndCallbackResult(final Capture<Callback<T>> cb, final T value) {
        PowerMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                cb.getValue().onCompletion(null, value);
                return null;
            }
        });
    }

    private  <T> void expectAndCallbackException(final Capture<Callback<T>> cb, final Throwable t) {
        PowerMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                cb.getValue().onCompletion(t, null);
                return null;
            }
        });
    }

    private  <T> void expectAndCallbackNotLeaderException(final Capture<Callback<T>> cb) {
        expectAndCallbackException(cb, new NotLeaderException("not leader test", LEADER_URL));
    }
}
