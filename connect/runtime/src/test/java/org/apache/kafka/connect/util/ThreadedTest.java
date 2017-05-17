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

import org.junit.After;
import org.junit.Before;

/**
 * Base class for tests that use threads. It sets up uncaught exception handlers for all known
 * thread classes and checks for errors at the end of the test so that failures in background
 * threads will cause the test to fail.
 */
public class ThreadedTest {

    protected TestBackgroundThreadExceptionHandler backgroundThreadExceptionHandler;

    @Before
    public void setup() {
        backgroundThreadExceptionHandler = new TestBackgroundThreadExceptionHandler();
        ShutdownableThread.funcaughtExceptionHandler = backgroundThreadExceptionHandler;
    }

    @After
    public void teardown() {
        backgroundThreadExceptionHandler.verifyNoExceptions();
        ShutdownableThread.funcaughtExceptionHandler = null;
    }
}
