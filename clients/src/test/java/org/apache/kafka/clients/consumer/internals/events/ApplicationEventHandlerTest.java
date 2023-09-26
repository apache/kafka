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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ApplicationEventHandlerTest {

    private ConsumerTestBuilder.ApplicationEventHandlerTestBuilder testBuilder;
    private ApplicationEventHandler applicationEventHandler;
    private BlockingQueue<ApplicationEvent> applicationEventQueue;

    @BeforeEach
    public void setup() {
        testBuilder = new ConsumerTestBuilder.ApplicationEventHandlerTestBuilder();
        applicationEventHandler = testBuilder.applicationEventHandler;
        applicationEventQueue = testBuilder.applicationEventQueue;
    }

    @AfterEach
    public void tearDown() {
        if (testBuilder != null)
            testBuilder.close();
    }

    @Test
    public void testBasicHandlerOps() {
        applicationEventHandler.add(new FetchEvent());
        assertEquals(1, applicationEventQueue.size());
    }
}
