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

import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.tools.MockConnector;
import org.apache.kafka.connect.transforms.Cast;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Flatten;
import org.apache.kafka.connect.transforms.HoistField;
import org.apache.kafka.connect.transforms.InsertField;
import org.apache.kafka.connect.transforms.MaskField;
import org.apache.kafka.connect.transforms.RegexRouter;
import org.apache.kafka.connect.transforms.ReplaceField;
import org.apache.kafka.connect.transforms.SetSchemaMetadata;
import org.apache.kafka.connect.transforms.TimestampConverter;
import org.apache.kafka.connect.transforms.TimestampRouter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.ValueToKey;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that transformations' configs can be composed with ConnectorConfig during its construction, ensuring no
 * conflicting fields or other issues.
 *
 * This test appears here simply because it requires both connect-runtime and connect-transforms and connect-runtime
 * already depends on connect-transforms.
 */
@SuppressWarnings("rawtypes")
public class TransformationConfigTest {

    private Plugins setupMockPlugins(Transformation transformation) {
        Plugins plugins = mock(Plugins.class);
        try {
            when(plugins.newPlugin(transformation.getClass().getName(), null, (ClassLoader) null)).thenReturn(transformation);
        } catch (ClassNotFoundException e) {
            // Shouldn't happen since we're mocking the plugins
        }
        return plugins;
    }

    @Test
    public void testEmbeddedConfigCast() {
        // Validate that we can construct a Connector config containing the extended config for the transform
        HashMap<String, String> connProps = new HashMap<>();
        connProps.put("name", "foo");
        connProps.put("connector.class", MockConnector.class.getName());
        connProps.put("transforms", "example");
        connProps.put("transforms.example.type", Cast.Value.class.getName());
        connProps.put("transforms.example.spec", "int8");

        Plugins plugins = setupMockPlugins(new Cast.Value());
        new ConnectorConfig(plugins, connProps);
    }

    @Test
    public void testEmbeddedConfigExtractField() {
        // Validate that we can construct a Connector config containing the extended config for the transform
        HashMap<String, String> connProps = new HashMap<>();
        connProps.put("name", "foo");
        connProps.put("connector.class", MockConnector.class.getName());
        connProps.put("transforms", "example");
        connProps.put("transforms.example.type", ExtractField.Value.class.getName());
        connProps.put("transforms.example.field", "field");

        Plugins plugins = setupMockPlugins(new ExtractField.Value());
        new ConnectorConfig(plugins, connProps);
    }

    @Test
    public void testEmbeddedConfigFlatten() {
        // Validate that we can construct a Connector config containing the extended config for the transform
        HashMap<String, String> connProps = new HashMap<>();
        connProps.put("name", "foo");
        connProps.put("connector.class", MockConnector.class.getName());
        connProps.put("transforms", "example");
        connProps.put("transforms.example.type", Flatten.Value.class.getName());

        Plugins plugins = setupMockPlugins(new Flatten.Value());
        new ConnectorConfig(plugins, connProps);
    }

    @Test
    public void testEmbeddedConfigHoistField() {
        // Validate that we can construct a Connector config containing the extended config for the transform
        HashMap<String, String> connProps = new HashMap<>();
        connProps.put("name", "foo");
        connProps.put("connector.class", MockConnector.class.getName());
        connProps.put("transforms", "example");
        connProps.put("transforms.example.type", HoistField.Value.class.getName());
        connProps.put("transforms.example.field", "field");

        Plugins plugins = setupMockPlugins(new HoistField.Value());
        new ConnectorConfig(plugins, connProps);
    }

    @Test
    public void testEmbeddedConfigInsertField() {
        // Validate that we can construct a Connector config containing the extended config for the transform
        HashMap<String, String> connProps = new HashMap<>();
        connProps.put("name", "foo");
        connProps.put("connector.class", MockConnector.class.getName());
        connProps.put("transforms", "example");
        connProps.put("transforms.example.type", InsertField.Value.class.getName());

        Plugins plugins = setupMockPlugins(new InsertField.Value());
        new ConnectorConfig(plugins, connProps);
    }

    @Test
    public void testEmbeddedConfigMaskField() {
        // Validate that we can construct a Connector config containing the extended config for the transform
        HashMap<String, String> connProps = new HashMap<>();
        connProps.put("name", "foo");
        connProps.put("connector.class", MockConnector.class.getName());
        connProps.put("transforms", "example");
        connProps.put("transforms.example.type", MaskField.Value.class.getName());
        connProps.put("transforms.example.fields", "field");
        connProps.put("transforms.example.replacement", "nothing");

        Plugins plugins = setupMockPlugins(new MaskField.Value());
        new ConnectorConfig(plugins, connProps);
    }

    @Test
    public void testEmbeddedConfigRegexRouter() {
        // Validate that we can construct a Connector config containing the extended config for the transform
        HashMap<String, String> connProps = new HashMap<>();
        connProps.put("name", "foo");
        connProps.put("connector.class", MockConnector.class.getName());
        connProps.put("transforms", "example");
        connProps.put("transforms.example.type", RegexRouter.class.getName());
        connProps.put("transforms.example.regex", "(.*)");
        connProps.put("transforms.example.replacement", "prefix-$1");

        Plugins plugins = setupMockPlugins(new RegexRouter());
        new ConnectorConfig(plugins, connProps);
    }

    @Test
    public void testEmbeddedConfigReplaceField() {
        // Validate that we can construct a Connector config containing the extended config for the transform
        HashMap<String, String> connProps = new HashMap<>();
        connProps.put("name", "foo");
        connProps.put("connector.class", MockConnector.class.getName());
        connProps.put("transforms", "example");
        connProps.put("transforms.example.type", ReplaceField.Value.class.getName());

        Plugins plugins = setupMockPlugins(new ReplaceField.Value());
        new ConnectorConfig(plugins, connProps);
    }

    @Test
    public void testEmbeddedConfigSetSchemaMetadata() {
        // Validate that we can construct a Connector config containing the extended config for the transform
        HashMap<String, String> connProps = new HashMap<>();
        connProps.put("name", "foo");
        connProps.put("connector.class", MockConnector.class.getName());
        connProps.put("transforms", "example");
        connProps.put("transforms.example.type", SetSchemaMetadata.Value.class.getName());

        Plugins plugins = setupMockPlugins(new SetSchemaMetadata.Value());
        new ConnectorConfig(plugins, connProps);
    }

    @Test
    public void testEmbeddedConfigTimestampConverter() {
        // Validate that we can construct a Connector config containing the extended config for the transform
        HashMap<String, String> connProps = new HashMap<>();
        connProps.put("name", "foo");
        connProps.put("connector.class", MockConnector.class.getName());
        connProps.put("transforms", "example");
        connProps.put("transforms.example.type", TimestampConverter.Value.class.getName());
        connProps.put("transforms.example.target.type", "unix");

        Plugins plugins = setupMockPlugins(new TimestampConverter.Value());
        new ConnectorConfig(plugins, connProps);
    }

    @Test
    public void testEmbeddedConfigTimestampRouter() {
        // Validate that we can construct a Connector config containing the extended config for the transform
        HashMap<String, String> connProps = new HashMap<>();
        connProps.put("name", "foo");
        connProps.put("connector.class", MockConnector.class.getName());
        connProps.put("transforms", "example");
        connProps.put("transforms.example.type", TimestampRouter.class.getName());

        Plugins plugins = setupMockPlugins(new TimestampRouter());
        new ConnectorConfig(plugins, connProps);
    }

    @Test
    public void testEmbeddedConfigValueToKey() {
        // Validate that we can construct a Connector config containing the extended config for the transform
        HashMap<String, String> connProps = new HashMap<>();
        connProps.put("name", "foo");
        connProps.put("connector.class", MockConnector.class.getName());
        connProps.put("transforms", "example");
        connProps.put("transforms.example.type", ValueToKey.class.getName());
        connProps.put("transforms.example.fields", "field");

        Plugins plugins = setupMockPlugins(new ValueToKey());
        new ConnectorConfig(plugins, connProps);
    }

}
