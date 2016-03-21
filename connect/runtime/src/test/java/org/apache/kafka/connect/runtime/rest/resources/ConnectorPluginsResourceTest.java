/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime.rest.resources;

import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.AbstractHerder;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest(RestServer.class)
@PowerMockIgnore("javax.management.*")
public class ConnectorPluginsResourceTest {

    private static Map<String, String> props = new HashMap<>();
    static {
        props.put("test.string.config", "testString");
        props.put("test.int.config", "10");
    }

    private static final ConfigInfos CONFIG_INFOS;
    static {
        List<ConfigInfo> configs = new LinkedList<>();

        ConfigKeyInfo configKeyInfo = new ConfigKeyInfo("test.string.config", "STRING", true, "", "HIGH", "Test configuration for string type.", null, -1, "NONE", "test.string.config", new LinkedList<String>());
        ConfigValueInfo configValueInfo = new ConfigValueInfo("test.string.config", "testString", Collections.<Object>emptyList(), Collections.<String>emptyList(), true);
        ConfigInfo configInfo = new ConfigInfo(configKeyInfo, configValueInfo);
        configs.add(configInfo);

        configKeyInfo = new ConfigKeyInfo("test.int.config", "INT", true, "", "MEDIUM", "Test configuration for integer type.", null, -1, "NONE", "test.int.config", new LinkedList<String>());
        configValueInfo = new ConfigValueInfo("test.int.config", 10, Collections.<Object>emptyList(), Collections.<String>emptyList(), true);
        configInfo = new ConfigInfo(configKeyInfo, configValueInfo);
        configs.add(configInfo);

        configKeyInfo = new ConfigKeyInfo("test.string.config.default", "STRING", false, "", "LOW", "Test configuration with default value.", null, -1, "NONE", "test.string.config.default", new LinkedList<String>());
        configValueInfo = new ConfigValueInfo("test.string.config.default", "", Collections.<Object>emptyList(), Collections.<String>emptyList(), true);
        configInfo = new ConfigInfo(configKeyInfo, configValueInfo);
        configs.add(configInfo);

        CONFIG_INFOS = new ConfigInfos(ConnectorPluginsResourceTestConnector.class.getName(), 0, Collections.<String>emptyList(), configs);
    }

    @Mock
    private Herder herder;
    private ConnectorPluginsResource connectorPluginsResource;

    @Before
    public void setUp() throws NoSuchMethodException {
        PowerMock.mockStatic(RestServer.class,
                             RestServer.class.getMethod("httpRequest", String.class, String.class, Object.class, TypeReference.class));
        connectorPluginsResource = new ConnectorPluginsResource(herder);
    }

    @Test
    public void testValidateConfig() throws Throwable {
        herder.validateConfigs(EasyMock.eq(ConnectorPluginsResourceTestConnector.class.getName()), EasyMock.eq(props));

        PowerMock.expectLastCall().andAnswer(new IAnswer<ConfigInfos>() {
            @Override
            public ConfigInfos answer() {
                Config config = new ConnectorPluginsResourceTestConnector().validate(props);
                Connector connector = new ConnectorPluginsResourceTestConnector();
                ConfigDef configDef = connector.config();
                return AbstractHerder.generateResult(ConnectorPluginsResourceTestConnector.class.getName(), configDef.configKeys(), config.configValues(), configDef.groups());
            }
        });
        PowerMock.replayAll();

        ConfigInfos configInfos = connectorPluginsResource.validateConfigs(ConnectorPluginsResourceTestConnector.class.getName(), props);
        assertEquals(CONFIG_INFOS.name(), configInfos.name());
        assertEquals(CONFIG_INFOS.errorCount(), configInfos.errorCount());
        assertEquals(CONFIG_INFOS.groups(), configInfos.groups());
        assertEquals(new HashSet<>(CONFIG_INFOS.values()), new HashSet<>(configInfos.values()));

        PowerMock.verifyAll();
    }

    /* Name here needs to be unique as we are testing the aliasing mechanism */
    public static class ConnectorPluginsResourceTestConnector extends Connector {

        public static final String TEST_STRING_CONFIG = "test.string.config";
        public static final String TEST_INT_CONFIG = "test.int.config";
        public static final String TEST_STRING_CONFIG_DEFAULT = "test.string.config.default";

        private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TEST_STRING_CONFIG, Type.STRING, Importance.HIGH, "Test configuration for string type.")
            .define(TEST_INT_CONFIG, Type.INT, Importance.MEDIUM, "Test configuration for integer type.")
            .define(TEST_STRING_CONFIG_DEFAULT, Type.STRING, "", Importance.LOW, "Test configuration with default value.");

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public void start(Map<String, String> props) {

        }

        @Override
        public Class<? extends Task> taskClass() {
            return null;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            return null;
        }

        @Override
        public void stop() {

        }

        @Override
        public ConfigDef config() {
            return CONFIG_DEF;
        }
    }
}
