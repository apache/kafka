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

package org.apache.kafka.metadata.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.junit.jupiter.api.Test;

import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.junit.jupiter.api.Assertions.assertEquals;


final public class RecordRedactorTest {
    public static final Map<ConfigResource.Type, ConfigDef> CONFIGS = new HashMap<>();

    static {
        CONFIGS.put(BROKER, new ConfigDef().
                define("foobar", ConfigDef.Type.LIST, "1", ConfigDef.Importance.HIGH, "foo bar doc").
                define("quux", ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "quuux2 doc"));
    }

    private static final KafkaConfigSchema SCHEMA = new KafkaConfigSchema(CONFIGS, Collections.emptyMap());

    private static final RecordRedactor REDACTOR = new RecordRedactor(SCHEMA);

    @Test
    public void testTopicRecordToString() {
        assertEquals("TopicRecord(name='foo', topicId=UOovKkohSU6AGdYW33ZUNg)",
                REDACTOR.toLoggableString(new TopicRecord().
                    setTopicId(Uuid.fromString("UOovKkohSU6AGdYW33ZUNg")).
                    setName("foo")));
    }

    @Test
    public void testUserScramCredentialRecordToString() {
        assertEquals("UserScramCredentialRecord(name='bob', mechanism=0, " +
            "salt=(redacted), storedKey=(redacted), serverKey=(redacted), iterations=128)",
                REDACTOR.toLoggableString(new UserScramCredentialRecord().
                    setName("bob").
                    setMechanism((byte) 0).
                    setSalt(new byte[512]).
                    setServerKey(new byte[128]).
                    setStoredKey(new byte[128]).
                    setIterations(128)));
    }

    @Test
    public void testUserScramCredentialRecordToStringWithNullName() {
        assertEquals("UserScramCredentialRecord(name=null, mechanism=1, " +
                        "salt=(redacted), storedKey=(redacted), serverKey=(redacted), iterations=256)",
                REDACTOR.toLoggableString(new UserScramCredentialRecord().
                        setName(null).
                        setMechanism((byte) 1).
                        setSalt(new byte[512]).
                        setServerKey(new byte[128]).
                        setStoredKey(new byte[128]).
                        setIterations(256)));
    }

    @Test
    public void testSensitiveConfigRecordToString() {
        assertEquals("ConfigRecord(resourceType=4, resourceName='0', name='quux', " +
            "value='(redacted)')",
                REDACTOR.toLoggableString(new ConfigRecord().
                    setResourceType(BROKER.id()).
                    setResourceName("0").
                    setName("quux").
                    setValue("mysecret")));
    }

    @Test
    public void testNonSensitiveConfigRecordToString() {
        assertEquals("ConfigRecord(resourceType=4, resourceName='0', name='foobar', " +
            "value='item1,item2')",
                REDACTOR.toLoggableString(new ConfigRecord().
                    setResourceType(BROKER.id()).
                    setResourceName("0").
                    setName("foobar").
                    setValue("item1,item2")));
    }
}
