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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.FutureCallback;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class AbstractHerderConfigErrorTest {

    @Mock
    private Worker worker;
    @Mock
    private Plugins plugins;
    @Mock
    private StatusBackingStore statusBackingStore;
    @Mock
    private ConfigBackingStore configBackingStore;

    @BeforeEach
    public void setUp() {
        when(worker.metrics()).thenReturn(new MockConnectMetrics());
        when(worker.getPlugins()).thenReturn(plugins);
    }

    @Test
    public void testMaybeAddConfigErrorsPreservesExtraValueErrorsBeforeGeneratedNullConfigValues() throws Exception {
        AbstractHerder herder = testHerder();
        String name = "com.acme.connector.MyConnector";
        Map<String, ConfigDef.ConfigKey> keys = new LinkedHashMap<>();
        addConfigKey(keys, "config.defined.without.value", "group");

        List<ConfigValue> values = new ArrayList<>();
        addValue(values, "config.extra.with.error", "value.extra", "actual validation error");

        ConfigInfos infos = AbstractHerder.generateResult(name, keys, values, List.of("group"));
        assertEquals(2, infos.configs().size());
        assertGeneratedConfigWithoutValue(infos, "config.defined.without.value");

        BadRequestException error = assertConfigErrors(herder, infos);
        assertConfigErrorMessage(error, "actual validation error");
    }

    @Test
    public void testMaybeAddConfigErrorsIgnoresGeneratedNullConfigValuesWhenErrorCountIsZero() {
        AbstractHerder herder = testHerder();
        String name = "com.acme.connector.MyConnector";
        Map<String, ConfigDef.ConfigKey> keys = new LinkedHashMap<>();
        addConfigKey(keys, "config.defined.without.value", "group");

        ConfigInfos infos = AbstractHerder.generateResult(name, keys, List.of(), List.of("group"));
        assertEquals(1, infos.configs().size());
        assertGeneratedConfigWithoutValue(infos, "config.defined.without.value");
        assertEquals(0, infos.errorCount());

        FutureCallback<Herder.Created<ConnectorInfo>> callback = new FutureCallback<>();
        assertFalse(herder.maybeAddConfigErrors(infos, callback));
        assertFalse(callback.isDone());
    }

    @Test
    public void testMaybeAddConfigErrorsPreservesGeneratedErrorsAroundNullConfigValues() throws Exception {
        AbstractHerder herder = testHerder();
        String name = "com.acme.connector.MyConnector";
        Map<String, ConfigDef.ConfigKey> keys = new LinkedHashMap<>();
        addConfigKey(keys, "config.defined.with.error.before", "group");
        addConfigKey(keys, "config.defined.without.value", "group");
        addConfigKey(keys, "config.defined.with.error.after", "group");

        List<ConfigValue> values = new ArrayList<>();
        addValue(values, "config.defined.with.error.before", "bad",
                "first error before null config value", "second error before null config value");
        addValue(values, "config.defined.with.error.after", "bad", "error after null config value");

        ConfigInfos infos = AbstractHerder.generateResult(name, keys, values, List.of("group"));
        assertEquals(3, infos.configs().size());
        assertEquals("config.defined.with.error.before", infos.configs().get(0).configValue().name());
        assertGeneratedConfigWithoutValue(infos, "config.defined.without.value");
        assertNull(infos.configs().get(1).configValue());
        assertEquals("config.defined.with.error.after", infos.configs().get(2).configValue().name());

        BadRequestException error = assertConfigErrors(herder, infos);
        assertConfigErrorMessage(
                error,
                "first error before null config value",
                "second error before null config value",
                "error after null config value"
        );
    }

    @Test
    public void testMaybeAddConfigErrorsPreservesErrorsWhenGeneratedConfigValuesArePresent() throws Exception {
        AbstractHerder herder = testHerder();
        String name = "com.acme.connector.MyConnector";
        Map<String, ConfigDef.ConfigKey> keys = new LinkedHashMap<>();
        addConfigKey(keys, "config.a", "group");
        addConfigKey(keys, "config.b", "group");
        addConfigKey(keys, "config.c", "group");

        List<ConfigValue> values = new ArrayList<>();
        addValue(values, "config.a", "value.a", "error a");
        addValue(values, "config.b", "value.b");
        addValue(values, "config.c", "value.c", "error c");

        ConfigInfos infos = AbstractHerder.generateResult(name, keys, values, List.of("group"));

        BadRequestException error = assertConfigErrors(herder, infos);
        assertConfigErrorMessage(error, "error a", "error c");
    }

    @Test
    public void testMaybeAddConfigErrorsInvokesCallbackOnceForGeneratedNullConfigValueErrors() {
        AbstractHerder herder = testHerder();
        String name = "com.acme.connector.MyConnector";
        Map<String, ConfigDef.ConfigKey> keys = new LinkedHashMap<>();
        addConfigKey(keys, "config.defined.without.value", "group");
        addConfigKey(keys, "config.defined.with.error", "group");

        List<ConfigValue> values = new ArrayList<>();
        addValue(values, "config.defined.with.error", "bad", "actual validation error");

        ConfigInfos infos = AbstractHerder.generateResult(name, keys, values, List.of("group"));
        assertGeneratedConfigWithoutValue(infos, "config.defined.without.value");

        @SuppressWarnings("unchecked")
        Callback<Herder.Created<ConnectorInfo>> callback = mock(Callback.class);

        assertTrue(herder.maybeAddConfigErrors(infos, callback));

        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(callback).onCompletion(error.capture(), isNull());
        verifyNoMoreInteractions(callback);
        BadRequestException badRequestException = assertInstanceOf(BadRequestException.class, error.getValue());
        assertConfigErrorMessage(badRequestException, "actual validation error");
    }

    @Test
    public void testMaybeAddConfigErrorsDoesNotCompleteCallbackWhenErrorCountIsZero() {
        AbstractHerder herder = testHerder();
        String name = "com.acme.connector.MyConnector";
        Map<String, ConfigDef.ConfigKey> keys = new LinkedHashMap<>();
        addConfigKey(keys, "config.defined.without.value", "group");
        addConfigKey(keys, "config.defined.with.value", "group");

        List<ConfigValue> values = new ArrayList<>();
        addValue(values, "config.defined.with.value", "value");

        ConfigInfos infos = AbstractHerder.generateResult(name, keys, values, List.of("group"));
        assertEquals(0, infos.errorCount());
        assertGeneratedConfigWithoutValue(infos, "config.defined.without.value");

        FutureCallback<Herder.Created<ConnectorInfo>> callback = new FutureCallback<>();

        assertFalse(herder.maybeAddConfigErrors(infos, callback));
        assertFalse(callback.isDone());
    }

    private AbstractHerder testHerder() {
        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy =
                new NoneConnectorClientConfigOverridePolicy();
        return mock(AbstractHerder.class, withSettings()
                .useConstructor(
                        worker,
                        "workerId",
                        "kafkaClusterId",
                        statusBackingStore,
                        configBackingStore,
                        connectorClientConfigOverridePolicy,
                        Time.SYSTEM
                )
                .defaultAnswer(CALLS_REAL_METHODS));
    }

    private void addConfigKey(Map<String, ConfigDef.ConfigKey> keys, String name, String group) {
        ConfigDef configDef = new ConfigDef().define(name, ConfigDef.Type.STRING, null, null,
                ConfigDef.Importance.HIGH, "doc", group, 10,
                ConfigDef.Width.MEDIUM, "display name", List.of(), null, null);
        keys.putAll(configDef.configKeys());
    }

    private void addValue(List<ConfigValue> values, String name, String value, String... errors) {
        values.add(new ConfigValue(name, value, new ArrayList<>(), List.of(errors)));
    }

    private void assertGeneratedConfigWithoutValue(ConfigInfos infos, String expectedName) {
        List<ConfigInfo> configsWithoutValue = infos.configs().stream()
                .filter(info -> info.configValue() == null)
                .toList();

        assertEquals(1, configsWithoutValue.size());
        ConfigInfo configInfo = configsWithoutValue.get(0);
        assertNotNull(configInfo.configKey());
        assertEquals(expectedName, configInfo.configKey().name());
    }

    private BadRequestException assertConfigErrors(AbstractHerder herder, ConfigInfos infos) throws Exception {
        FutureCallback<Herder.Created<ConnectorInfo>> callback = new FutureCallback<>();

        assertTrue(herder.maybeAddConfigErrors(infos, callback));
        assertTrue(callback.isDone());

        ExecutionException error = assertThrows(ExecutionException.class, callback::get);
        assertEquals(BadRequestException.class, error.getCause().getClass());
        return (BadRequestException) error.getCause();
    }

    private void assertConfigErrorMessage(BadRequestException error, String... expectedErrors) {
        StringBuilder expected = new StringBuilder();
        expected.append("Connector configuration is invalid and contains the following ")
                .append(expectedErrors.length)
                .append(" error(s):");
        for (String expectedError : expectedErrors) {
            expected.append('\n').append(expectedError);
        }
        expected.append("\nYou can also find the above list of errors at the endpoint ")
                .append("`/connector-plugins/{connectorType}/config/validate`");

        assertEquals(expected.toString(), error.getMessage());
    }
}
