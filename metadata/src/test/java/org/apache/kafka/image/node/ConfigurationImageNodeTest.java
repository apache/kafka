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

package org.apache.kafka.image.node;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.image.ConfigurationImage;
import org.apache.kafka.image.node.printer.MetadataNodeRedactionCriteria;
import org.apache.kafka.image.node.printer.NodeStringifier;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


@Timeout(value = 40)
public class ConfigurationImageNodeTest {
    private static final MetadataNodeRedactionCriteria NORMAL;

    private static final ConfigurationImageNode NODE;

    static {
        KafkaConfigSchema schema = new KafkaConfigSchema(Collections.singletonMap(BROKER, new ConfigDef().
            define("non.secret", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "baz").
            define("also.non.secret", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "baz").
            define("secret.config", ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "baz")),
                Collections.emptyMap());
        NORMAL = new MetadataNodeRedactionCriteria.Normal(schema);

        Map<String, String> configs = new HashMap<>();
        configs.put("non.secret", "baaz");
        configs.put("secret.config", "123");
        ConfigurationImage image = new ConfigurationImage(new ConfigResource(BROKER, ""), configs);
        NODE = new ConfigurationImageNode(image);
    }

    @Test
    public void testNonSecretChild() {
        NodeStringifier stringifier = new NodeStringifier(NORMAL);
        NODE.child("non.secret").print(stringifier);
        assertEquals("baaz", stringifier.toString());
    }

    @Test
    public void testSecretChild() {
        NodeStringifier stringifier = new NodeStringifier(NORMAL);
        NODE.child("secret.config").print(stringifier);
        assertEquals("[redacted]", stringifier.toString());
    }

    @Test
    public void testUnknownChild() {
        assertNull(NODE.child("also.non.secret"));
    }
}
