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

package org.apache.kafka.controller;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.APPEND;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.DELETE;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER_LOGGER;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.apache.kafka.common.config.ConfigResource.Type.UNKNOWN;
import static org.junit.Assert.assertEquals;

public class ConfigurationControlManagerTest {
    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    private static final Map<ConfigResource.Type, ConfigDef> CONFIGS = new HashMap<>();

    static {
        CONFIGS.put(BROKER, new ConfigDef().
            define("foo.bar", ConfigDef.Type.LIST, "1", ConfigDef.Importance.HIGH, "foo bar").
            define("baz", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "baz").
            define("quux", ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "quux"));
        CONFIGS.put(TOPIC, new ConfigDef().
            define("abc", ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "abc").
            define("def", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "def"));
    }

    @SuppressWarnings("unchecked")
    private static <A, B> Map<A, B> toMap(Entry... entries) {
        Map<A, B> map = new HashMap<>();
        for (Entry<A, B> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    private static <A, B> Entry<A, B> entry(A a, B b) {
        return new SimpleImmutableEntry<>(a, b);
    }

    @Test
    public void testReplay() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(0);
        ConfigurationControlManager manager =
            new ConfigurationControlManager(snapshotRegistry, CONFIGS);
        assertEquals(Collections.emptyMap(),
            manager.getConfigs(new ConfigResource(BROKER, "0")));
        manager.replay(new ConfigRecord().
            setResourceType(BROKER.id()).setResourceName("0").
            setName("foo.bar").setValue("1,2"));
        assertEquals(Collections.singletonMap("foo.bar", "1,2"),
            manager.getConfigs(new ConfigResource(BROKER, "0")));
        manager.replay(new ConfigRecord().
            setResourceType(BROKER.id()).setResourceName("0").
            setName("foo.bar").setValue(null));
        assertEquals(Collections.emptyMap(),
            manager.getConfigs(new ConfigResource(BROKER, "0")));
        manager.replay(new ConfigRecord().
            setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("abc").setValue("x,y,z"));
        manager.replay(new ConfigRecord().
            setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("def").setValue("blah"));
        assertEquals(toMap(entry("abc", "x,y,z"), entry("def", "blah")),
            manager.getConfigs(new ConfigResource(TOPIC, "mytopic")));
    }

    @Test
    public void testCheckConfigResource() {
        assertEquals(new ApiError(Errors.INVALID_REQUEST, "Unsupported " +
            "configuration resource type BROKER_LOGGER ").toString(),
            ConfigurationControlManager.checkConfigResource(
                new ConfigResource(BROKER_LOGGER, "kafka.server.FetchContext")).toString());
        assertEquals(new ApiError(Errors.INVALID_REQUEST, "Cannot perform " +
            "operations on a TOPIC resource with an illegal topic name.").toString(),
            ConfigurationControlManager.checkConfigResource(
                new ConfigResource(TOPIC, "* @ invalid$")).toString());
        assertEquals(new ApiError(Errors.NONE, null).toString(),
            ConfigurationControlManager.checkConfigResource(
                new ConfigResource(TOPIC, "")).toString());
        assertEquals(new ApiError(Errors.INVALID_REQUEST, "Cannot perform " +
                "operations on a BROKER resource with a non-integer name.").toString(),
            ConfigurationControlManager.checkConfigResource(
                new ConfigResource(BROKER, "bob")).toString());
        assertEquals(new ApiError(Errors.NONE, null).toString(),
            ConfigurationControlManager.checkConfigResource(
                new ConfigResource(BROKER, "")).toString());
        assertEquals(new ApiError(Errors.INVALID_REQUEST, "Cannot perform " +
                "operations on a configuration resource with type UNKNOWN.").toString(),
            ConfigurationControlManager.checkConfigResource(
                new ConfigResource(UNKNOWN, "bob")).toString());
    }

    @Test
    public void testIncrementalAlterConfigs() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(0);
        ConfigurationControlManager manager =
            new ConfigurationControlManager(snapshotRegistry, CONFIGS);
        ConfigResource broker0 = new ConfigResource(BROKER, "0");
        ConfigResource mytopic = new ConfigResource(TOPIC, "mytopic");
        assertEquals(new ControllerResult<Map<ConfigResource, ApiError>>(Collections.singletonList(
            new ApiMessageAndVersion(new ConfigRecord().
                setResourceType(TOPIC.id()).setResourceName("mytopic").
                setName("abc").setValue("123"), (short) 0)),
            toMap(entry(broker0, new ApiError(
                Errors.INVALID_REQUEST, "A DELETE op was given with a non-null value.")),
                entry(mytopic, ApiError.NONE))),
            manager.incrementalAlterConfigs(toMap(entry(broker0, toMap(
                entry("foo.bar", entry(DELETE, "abc")),
                entry("quux", entry(SET, "abc")))),
            entry(mytopic, toMap(
                entry("abc", entry(APPEND, "123")))))));
    }
}
