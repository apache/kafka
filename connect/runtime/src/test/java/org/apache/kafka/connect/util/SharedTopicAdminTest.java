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
package org.apache.kafka.connect.util;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

@RunWith(PowerMockRunner.class)
@PrepareForTest(KafkaBasedLog.class)
@PowerMockIgnore("javax.management.*")
public class SharedTopicAdminTest {

    private static final Map<String, Object> CONFIG = Collections.emptyMap();

    @Mock private TopicAdmin mockTopicAdmin;
    private SharedTopicAdmin sharedAdmin;
    private int created = 0;

    @Before
    public void beforeEach() {
        sharedAdmin = new SharedTopicAdmin(CONFIG, config -> {
            ++created;
            return mockTopicAdmin;
        });
    }

    @Test
    public void shouldCloseWithoutBeingUsed() {
        PowerMock.replayAll();
        sharedAdmin.close();
        assertEquals(0, created);
        PowerMock.verifyAll();
    }

    @Test
    public void shouldCloseAfterTopicAdminUsed() {
        mockTopicAdmin.close(SharedTopicAdmin.DEFAULT_CLOSE_DURATION);
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        assertSame(mockTopicAdmin, sharedAdmin.topicAdmin());
        sharedAdmin.close();
        assertEquals(1, created);

        PowerMock.verifyAll();
    }

    @Test
    public void shouldCloseAfterTopicAdminUsedMultipleTimes() {
        mockTopicAdmin.close(SharedTopicAdmin.DEFAULT_CLOSE_DURATION);
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        for (int i = 0; i != 10; ++i) {
            assertSame(mockTopicAdmin, sharedAdmin.topicAdmin());
        }
        sharedAdmin.close();
        assertEquals(1, created);

        PowerMock.verifyAll();
    }

    @Test
    public void shouldCloseWithDurationAfterTopicAdminUsed() {
        Duration timeout = Duration.ofSeconds(1);
        mockTopicAdmin.close(timeout);
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        assertSame(mockTopicAdmin, sharedAdmin.topicAdmin());
        sharedAdmin.close(timeout);
        assertEquals(1, created);

        PowerMock.verifyAll();
    }

    @Test
    public void shouldFailToGetTopicAdminAfterClose() {
        PowerMock.replayAll();
        sharedAdmin.close();
        assertEquals(0, created);
        assertThrows(ConnectException.class, () -> sharedAdmin.topicAdmin());
        PowerMock.verifyAll();
    }
}