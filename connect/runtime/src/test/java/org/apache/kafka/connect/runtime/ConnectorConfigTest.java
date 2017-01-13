/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConnectorConfigTest<R extends ConnectRecord<R>> {

    public static abstract class TestConnector extends Connector {
    }

    public static class SimpleTransformation<R extends ConnectRecord<R>> implements Transformation<R>  {

        int magicNumber = 0;

        @Override
        public void configure(Map<String, ?> props) {
            magicNumber = Integer.parseInt((String) props.get("magic.number"));
        }

        @Override
        public R apply(R record) {
            return null;
        }

        @Override
        public void close() {
            magicNumber = 0;
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef()
                    .define("magic.number", ConfigDef.Type.INT, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Range.atLeast(42), ConfigDef.Importance.HIGH, "");
        }
    }

    @Test
    public void noTransforms() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        new ConnectorConfig(props);
    }

    @Test(expected = ConfigException.class)
    public void danglingTransformAlias() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "dangler");
        new ConnectorConfig(props);
    }

    @Test(expected = ConfigException.class)
    public void wrongTransformationType() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", "uninstantiable");
        new ConnectorConfig(props);
    }

    @Test(expected = ConfigException.class)
    public void unconfiguredTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        new ConnectorConfig(props);
    }

    @Test
    public void misconfiguredTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "40");
        try {
            new ConnectorConfig(props);
            fail();
        } catch (ConfigException e) {
            assertTrue(e.getMessage().contains("Value must be at least 42"));
        }
    }

    @Test
    public void singleTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        final ConnectorConfig config = new ConnectorConfig(props);
        final List<Transformation<R>> transformations = config.transformations();
        assertEquals(1, transformations.size());
        final SimpleTransformation xform = (SimpleTransformation) transformations.get(0);
        assertEquals(42, xform.magicNumber);
    }

    @Test(expected = ConfigException.class)
    public void multipleTransformsOneDangling() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a, b");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        new ConnectorConfig(props);
    }

    @Test
    public void multipleTransforms() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a, b");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.b.type", SimpleTransformation.class.getName());
        props.put("transforms.b.magic.number", "84");
        final ConnectorConfig config = new ConnectorConfig(props);
        final List<Transformation<R>> transformations = config.transformations();
        assertEquals(2, transformations.size());
        assertEquals(42, ((SimpleTransformation) transformations.get(0)).magicNumber);
        assertEquals(84, ((SimpleTransformation) transformations.get(1)).magicNumber);
    }
    
}
